"""
Smart City - Traffic Flow Processor
====================================
FastAPI server que:
 - Recibe frames JPEG (base64) simulando cámaras de tráfico
 - Detecta vehículos y calcula nivel de congestión
 - Exporta métricas QoS via OpenTelemetry
 - Sirve un dashboard en vivo en /dashboard (SSE)
 - Expone /qos y /health para el migration controller

Nodo inicial: r3-node (RPi3) → migra a vm1node cuando QoS cae.
"""

import os
import time
import io
import base64
import threading
import collections
import json
import asyncio
import logging
from contextlib import asynccontextmanager

import psutil
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel
from PIL import Image, ImageFilter, ImageEnhance

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("traffic-processor")

# ─── Configuración vía env vars ───────────────────────────────────────────────
SERVICE_NAME            = os.getenv("SERVICE_NAME",              "traffic-processor-r3")
OTEL_ENDPOINT           = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability:4318")
PORT                    = int(os.getenv("PORT",                  "8080"))
QOS_LATENCY_BASELINE    = float(os.getenv("QOS_LATENCY_BASELINE",  "0.3"))   # segundos
QOS_THROUGHPUT_BASELINE = float(os.getenv("QOS_THROUGHPUT_BASELINE","2.0"))  # req/s
QOS_WINDOW_SECONDS      = int(os.getenv("QOS_WINDOW_SECONDS",    "30"))
SERVER_PIPELINE_REPEATS = int(os.getenv("SERVER_PIPELINE_REPEATS","8"))      # amplificación CPU
MAX_IMAGE_MB            = float(os.getenv("MAX_IMAGE_MB",        "10.0"))
OTEL_EXPORT_INTERVAL_MS = int(os.getenv("OTEL_EXPORT_INTERVAL_MS","5000"))

# ─── Estado QoS (thread-safe) ─────────────────────────────────────────────────
qos_lock = threading.Lock()
# Cada evento: (timestamp, proc_time_s, success, vehicle_count, congestion_level)
processing_events: collections.deque = collections.deque()

qos_state: dict = {
    "latency_score":     1.0,
    "cpu_score":         1.0,
    "throughput_score":  1.0,
    "rejection_rate":    1.0,
    "composite":         1.0,
    "avg_proc_time":     0.0,
    "cpu_percent":       0.0,
    "total_requests":    0,
    "error_count":       0,
    "vehicle_count_avg": 0.0,
    "congestion_avg":    0.0,
    "node":              SERVICE_NAME,
    "uptime_s":          0,
}

_start_time = time.time()

# ─── Historial para el dashboard en vivo ──────────────────────────────────────
HISTORY_LEN = 60  # puntos de historia
history_lock = threading.Lock()
history: dict = {
    "composite":    collections.deque(maxlen=HISTORY_LEN),
    "cpu":          collections.deque(maxlen=HISTORY_LEN),
    "latency":      collections.deque(maxlen=HISTORY_LEN),
    "vehicles":     collections.deque(maxlen=HISTORY_LEN),
    "congestion":   collections.deque(maxlen=HISTORY_LEN),
    "throughput":   collections.deque(maxlen=HISTORY_LEN),
    "timestamps":   collections.deque(maxlen=HISTORY_LEN),
}

# SSE subscribers
sse_subscribers: list = []
sse_lock = threading.Lock()


# ─── Hilo de cómputo QoS ──────────────────────────────────────────────────────
def compute_qos_loop():
    """Recalcula QoS cada 5 segundos y notifica SSE subscribers."""
    while True:
        time.sleep(5)
        now = time.time()
        cutoff = now - QOS_WINDOW_SECONDS

        with qos_lock:
            while processing_events and processing_events[0][0] < cutoff:
                processing_events.popleft()
            events       = list(processing_events)
            total_req    = qos_state["total_requests"]
            error_count  = qos_state["error_count"]

        if not events:
            continue

        proc_times  = [e[1] for e in events]
        successes   = [e[2] for e in events]
        vehicles    = [e[3] for e in events]
        congestions = [e[4] for e in events]

        avg_proc    = sum(proc_times) / len(proc_times)
        cpu         = psutil.cpu_percent(interval=None)

        # Dimensiones QoS
        latency_score    = min(1.0, QOS_LATENCY_BASELINE / avg_proc) if avg_proc > 0 else 1.0
        cpu_score        = max(0.0, 1.0 - cpu / 100.0)

        window_total     = len(events)
        window_errors    = window_total - sum(successes)
        rejection_rate   = max(0.0, 1.0 - window_errors / window_total) if window_total > 0 else 1.0

        elapsed_window   = now - events[0][0] if events else 1
        throughput       = window_total / max(elapsed_window, 1)
        throughput_score = min(1.0, throughput / QOS_THROUGHPUT_BASELINE)

        composite = (
            latency_score    * 0.40 +
            cpu_score        * 0.30 +
            throughput_score * 0.15 +
            rejection_rate   * 0.15
        )

        vehicle_avg    = sum(vehicles) / len(vehicles) if vehicles else 0
        congestion_avg = sum(congestions) / len(congestions) if congestions else 0

        with qos_lock:
            qos_state.update({
                "latency_score":     round(latency_score,    4),
                "cpu_score":         round(cpu_score,        4),
                "throughput_score":  round(throughput_score, 4),
                "rejection_rate":    round(rejection_rate,   4),
                "composite":         round(composite,        4),
                "avg_proc_time":     round(avg_proc,         4),
                "cpu_percent":       round(cpu,              2),
                "vehicle_count_avg": round(vehicle_avg,      2),
                "congestion_avg":    round(congestion_avg,   4),
                "uptime_s":          int(now - _start_time),
            })

        # Guardar historia
        ts = time.strftime("%H:%M:%S")
        with history_lock:
            history["composite"].append(round(composite, 3))
            history["cpu"].append(round(cpu, 1))
            history["latency"].append(round(avg_proc * 1000, 1))   # ms
            history["vehicles"].append(round(vehicle_avg, 1))
            history["congestion"].append(round(congestion_avg * 100, 1))
            history["throughput"].append(round(throughput, 2))
            history["timestamps"].append(ts)

        # Notificar SSE subscribers
        with sse_lock:
            snapshot = {**qos_state}
            snapshot["history"] = {k: list(v) for k, v in history.items()}
        _broadcast_sse(snapshot)

        logger.info(
            f"QoS composite={composite:.3f} cpu={cpu:.1f}% "
            f"latency={avg_proc*1000:.0f}ms vehicles={vehicle_avg:.1f} "
            f"congestion={congestion_avg*100:.1f}%"
        )


def _broadcast_sse(data: dict):
    payload = f"data: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_subscribers:
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        for q in dead:
            sse_subscribers.remove(q)


# ─── OpenTelemetry ────────────────────────────────────────────────────────────
_frame_hist = None

def setup_otel():
    global _frame_hist
    resource = Resource.create({"service.name": SERVICE_NAME})
    exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
    reader   = PeriodicExportingMetricReader(exporter, export_interval_millis=OTEL_EXPORT_INTERVAL_MS)
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter(SERVICE_NAME)

    _frame_hist = meter.create_histogram(
        "server_frame_processing_seconds",
        description="Frame processing time (seconds)",
        unit="s",
    )

    def obs(name):
        def cb(_options):
            with qos_lock:
                yield metrics.Observation(qos_state[name], {"service": SERVICE_NAME})
        return cb

    meter.create_observable_gauge("qos_composite",       callbacks=[obs("composite")])
    meter.create_observable_gauge("qos_latency_score",   callbacks=[obs("latency_score")])
    meter.create_observable_gauge("qos_cpu_score",       callbacks=[obs("cpu_score")])
    meter.create_observable_gauge("qos_throughput_score",callbacks=[obs("throughput_score")])
    meter.create_observable_gauge("qos_rejection_rate",  callbacks=[obs("rejection_rate")])
    meter.create_observable_gauge("qos_cpu_percent",     callbacks=[obs("cpu_percent")])
    meter.create_observable_gauge("qos_avg_proc_time",   callbacks=[obs("avg_proc_time")])
    meter.create_observable_gauge("traffic_vehicle_count_avg", callbacks=[obs("vehicle_count_avg")])
    meter.create_observable_gauge("traffic_congestion_level",  callbacks=[obs("congestion_avg")])

    logger.info(f"OTEL configurado → {OTEL_ENDPOINT}  export_interval={OTEL_EXPORT_INTERVAL_MS}ms")


# ─── FastAPI ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_otel()
    threading.Thread(target=compute_qos_loop, daemon=True).start()
    logger.info(f"Traffic Processor listo  service={SERVICE_NAME}  port={PORT}")
    yield

app = FastAPI(title="Smart City — Traffic Processor", version="1.0.0", lifespan=lifespan)


# ─── Modelos ──────────────────────────────────────────────────────────────────
class FrameRequest(BaseModel):
    frame:      str            # JPEG base64
    camera_id:  str = "cam-01"
    operations: list = []


# ─── Lógica de procesamiento ──────────────────────────────────────────────────
def detect_vehicles(img: Image.Image) -> tuple[int, float]:
    """
    Detección simplificada de vehículos por segmentación de color.
    Retorna (conteo, nivel_congestión 0-1).
    """
    import numpy as np
    # Trabajar a tamaño reducido para no saturar el RPi3 en esta operación
    small = img.convert("RGB").resize((320, 180), Image.BILINEAR)
    arr = np.array(small)

    # Saturación como proxy de "vehículo" (píxeles coloridos ≠ asfalto gris)
    r, g, b = arr[:, :, 0].astype(int), arr[:, :, 1].astype(int), arr[:, :, 2].astype(int)
    diff = arr.max(axis=2).astype(int) - arr.min(axis=2).astype(int)
    vehicle_mask = diff > 45

    h, w = arr.shape[:2]
    congestion = float(vehicle_mask.sum()) / (h * w)

    # Contar "blobs" por escaneo de columnas
    cols_with_vehicle = vehicle_mask.any(axis=0)
    count, in_blob = 0, False
    for v in cols_with_vehicle:
        if v and not in_blob:
            count += 1
            in_blob = True
        elif not v:
            in_blob = False

    return count, congestion


def apply_operations(img: Image.Image, operations: list) -> Image.Image:
    for op in operations:
        t = op.get("type", "")
        if t == "blur":
            img = img.filter(ImageFilter.GaussianBlur(radius=op.get("radius", 2)))
        elif t == "sharpen":
            img = img.filter(ImageFilter.SHARPEN)
        elif t == "grayscale":
            img = img.convert("L").convert("RGB")
        elif t == "resize":
            img = img.resize((op.get("width", 640), op.get("height", 360)), Image.LANCZOS)
        elif t == "enhance_contrast":
            img = ImageEnhance.Contrast(img).enhance(op.get("factor", 1.5))
        elif t == "edge_detect":
            img = img.filter(ImageFilter.FIND_EDGES)
    return img


def amplify_cpu(img: Image.Image) -> Image.Image:
    """Amplificación de carga CPU para simular procesamiento intensivo en RPi3."""
    for _ in range(SERVER_PIPELINE_REPEATS):
        img = img.filter(ImageFilter.GaussianBlur(radius=1))
        img = img.filter(ImageFilter.SHARPEN)
        img = ImageEnhance.Contrast(img).enhance(1.05)
    return img


def congestion_label(level: float) -> str:
    if level < 0.08:  return "libre"
    if level < 0.20:  return "fluido"
    if level < 0.40:  return "moderado"
    if level < 0.65:  return "congestionado"
    return "colapso"


# ─── Endpoints ────────────────────────────────────────────────────────────────
@app.post("/process")
async def process_frame(req: FrameRequest, request: Request):
    start   = time.time()
    attempt = int(request.headers.get("X-Attempt", "1"))

    try:
        raw = base64.b64decode(req.frame)
        if len(raw) > MAX_IMAGE_MB * 1024 * 1024:
            raise HTTPException(status_code=413, detail="Frame demasiado grande")

        img = Image.open(io.BytesIO(raw))
        img = apply_operations(img, req.operations)

        vehicle_count, congestion_level = detect_vehicles(img)

        # Amplificación CPU (el coste real que satura el RPi3)
        img = amplify_cpu(img)

        elapsed = time.time() - start

        with qos_lock:
            processing_events.append((time.time(), elapsed, True, vehicle_count, congestion_level))
            qos_state["total_requests"] += 1

        if _frame_hist:
            _frame_hist.record(elapsed, {"service": SERVICE_NAME, "camera": req.camera_id})

        with qos_lock:
            snap = dict(qos_state)

        logger.info(
            f"[{req.camera_id}] attempt={attempt} vehicles={vehicle_count} "
            f"congestion={congestion_level:.1%} proc={elapsed:.3f}s "
            f"qos={snap['composite']:.3f}"
        )

        return JSONResponse({
            "camera_id":         req.camera_id,
            "vehicle_count":     vehicle_count,
            "congestion_level":  round(congestion_level, 4),
            "congestion_label":  congestion_label(congestion_level),
            "processing_time_s": round(elapsed, 4),
            "node":              SERVICE_NAME,
            "qos": {
                "composite":     snap["composite"],
                "latency_score": snap["latency_score"],
                "cpu_score":     snap["cpu_score"],
                "cpu_percent":   snap["cpu_percent"],
            },
        })

    except HTTPException:
        raise
    except Exception as exc:
        elapsed = time.time() - start
        with qos_lock:
            processing_events.append((time.time(), elapsed, False, 0, 0.0))
            qos_state["error_count"]    += 1
            qos_state["total_requests"] += 1
        logger.error(f"Error procesando frame: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME, "uptime_s": int(time.time() - _start_time)}


@app.get("/qos")
def get_qos():
    with qos_lock:
        return dict(qos_state)


# ─── Server-Sent Events (stream en tiempo real) ───────────────────────────────
@app.get("/stream")
async def sse_stream(request: Request):
    """SSE endpoint — el dashboard se suscribe aquí para recibir métricas en vivo."""
    import asyncio
    queue: asyncio.Queue = asyncio.Queue(maxsize=20)

    # Enviar snapshot inicial
    with qos_lock:
        snap = dict(qos_state)
    with history_lock:
        snap["history"] = {k: list(v) for k, v in history.items()}

    with sse_lock:
        sse_subscribers.append(queue)

    async def generator():
        # Primer evento inmediato
        yield f"data: {json.dumps(snap)}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            with sse_lock:
                if queue in sse_subscribers:
                    sse_subscribers.remove(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ─── Dashboard en vivo (HTML embebido) ────────────────────────────────────────
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Smart City — Traffic Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0f172a; --card: #1e293b; --border: #334155;
    --text: #e2e8f0; --muted: #94a3b8;
    --green: #22c55e; --yellow: #eab308; --red: #ef4444; --blue: #3b82f6;
    --purple: #a855f7; --cyan: #06b6d4;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: system-ui, monospace; padding: 16px; min-height: 100vh; }
  h1 { font-size: 18px; font-weight: 700; margin-bottom: 4px; }
  .subtitle { font-size: 12px; color: var(--muted); margin-bottom: 20px; }
  .status-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--green); margin-right: 6px; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
  .grid4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 16px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 16px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px; }
  .kpi-label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .kpi-value { font-size: 28px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .kpi-sub { font-size: 11px; color: var(--muted); margin-top: 3px; }
  .bar-track { height: 6px; background: var(--border); border-radius: 3px; margin-top: 8px; }
  .bar-fill { height: 100%; border-radius: 3px; transition: width 0.5s; }
  .chart-title { font-size: 12px; font-weight: 600; color: var(--muted); margin-bottom: 10px; }
  canvas { max-height: 160px; }
  .cam-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 16px; }
  .cam-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 10px; }
  .cam-name { font-size: 11px; color: var(--muted); margin-bottom: 6px; }
  .cam-stat { font-size: 14px; font-weight: 700; }
  .cam-label { font-size: 11px; margin-top: 2px; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }
  .badge-free { background: #14532d; color: #86efac; }
  .badge-fluido { background: #1e3a5f; color: #93c5fd; }
  .badge-moderado { background: #713f12; color: #fde68a; }
  .badge-congestionado { background: #7f1d1d; color: #fca5a5; }
  .badge-colapso { background: #581c87; color: #e9d5ff; }
  .node-tag { background: #1e40af22; border: 1px solid #3b82f6; color: var(--blue); padding: 2px 8px; border-radius: 4px; font-size: 11px; font-family: monospace; }
  .qos-breakdown { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
  .qos-row { display: flex; align-items: center; gap: 8px; font-size: 12px; }
  .qos-name { color: var(--muted); width: 130px; flex-shrink: 0; }
  .qos-bar { flex: 1; height: 10px; background: var(--border); border-radius: 5px; overflow: hidden; }
  .qos-bfill { height: 100%; border-radius: 5px; transition: width 0.5s; }
  .qos-num { width: 40px; text-align: right; font-size: 11px; font-weight: 600; }
</style>
</head>
<body>

<h1><span class="status-dot" id="dot"></span>Smart City — Traffic Flow Monitor</h1>
<p class="subtitle" id="node-info">Conectando...</p>

<!-- KPIs principales -->
<div class="grid4">
  <div class="card">
    <div class="kpi-label">QoS Composite</div>
    <div class="kpi-value" id="kpi-qos" style="color:var(--green)">–</div>
    <div class="bar-track"><div class="bar-fill" id="bar-qos" style="background:var(--green);width:0%"></div></div>
    <div class="kpi-sub">umbral migración: 0.50</div>
  </div>
  <div class="card">
    <div class="kpi-label">CPU Uso</div>
    <div class="kpi-value" id="kpi-cpu" style="color:var(--cyan)">–</div>
    <div class="bar-track"><div class="bar-fill" id="bar-cpu" style="background:var(--cyan);width:0%"></div></div>
    <div class="kpi-sub" id="kpi-cpu-node">–</div>
  </div>
  <div class="card">
    <div class="kpi-label">Latencia Frames</div>
    <div class="kpi-value" id="kpi-lat" style="color:var(--blue)">–</div>
    <div class="kpi-sub">baseline: <span id="lat-baseline">300</span>ms</div>
  </div>
  <div class="card">
    <div class="kpi-label">Vehículos Avg</div>
    <div class="kpi-value" id="kpi-veh" style="color:var(--purple)">–</div>
    <div class="kpi-sub" id="kpi-cong">congestión: –</div>
  </div>
</div>

<!-- QoS breakdown + Congestión -->
<div class="grid2">
  <div class="card">
    <div class="chart-title">QoS por Dimensión</div>
    <div style="display:flex;flex-direction:column;gap:10px;">
      <div class="qos-row"><span class="qos-name">Latencia (40%)</span><div class="qos-bar"><div class="qos-bfill" id="qd-lat" style="background:var(--blue)"></div></div><span class="qos-num" id="qn-lat">–</span></div>
      <div class="qos-row"><span class="qos-name">CPU (30%)</span><div class="qos-bar"><div class="qos-bfill" id="qd-cpu" style="background:var(--cyan)"></div></div><span class="qos-num" id="qn-cpu">–</span></div>
      <div class="qos-row"><span class="qos-name">Throughput (15%)</span><div class="qos-bar"><div class="qos-bfill" id="qd-thr" style="background:var(--purple)"></div></div><span class="qos-num" id="qn-thr">–</span></div>
      <div class="qos-row"><span class="qos-name">Aceptación (15%)</span><div class="qos-bar"><div class="qos-bfill" id="qd-rej" style="background:var(--green)"></div></div><span class="qos-num" id="qn-rej">–</span></div>
    </div>
    <div style="margin-top:14px;display:flex;gap:8px;flex-wrap:wrap;">
      <span class="node-tag" id="node-tag">–</span>
      <span id="uptime-tag" style="font-size:11px;color:var(--muted);margin-top:2px;">uptime: –</span>
    </div>
  </div>
  <div class="card">
    <div class="chart-title">Nivel de Congestión (% frame)</div>
    <canvas id="chart-cong"></canvas>
  </div>
</div>

<!-- Series temporales -->
<div class="grid2">
  <div class="card">
    <div class="chart-title">QoS Composite · últimos 60s</div>
    <canvas id="chart-qos"></canvas>
  </div>
  <div class="card">
    <div class="chart-title">Latencia de Frames (ms)</div>
    <canvas id="chart-lat"></canvas>
  </div>
</div>

<div class="grid2">
  <div class="card">
    <div class="chart-title">CPU % en nodo</div>
    <canvas id="chart-cpu"></canvas>
  </div>
  <div class="card">
    <div class="chart-title">Throughput (frames/s)</div>
    <canvas id="chart-thr"></canvas>
  </div>
</div>

<script>
const MIGRATION_THRESHOLD = 0.50;
let charts = {};
const commonOpts = (yMax=1, color='#3b82f6') => ({
  responsive: true, maintainAspectRatio: false,
  animation: { duration: 300 },
  plugins: { legend: { display: false } },
  scales: {
    x: { ticks: { color:'#64748b', font:{size:9}, maxTicksLimit:6 }, grid:{color:'#1e293b'} },
    y: { min:0, max:yMax, ticks:{color:'#64748b',font:{size:9}}, grid:{color:'#334155'} }
  }
});

function makeChart(id, color, yMax=1, label='') {
  const ctx = document.getElementById(id).getContext('2d');
  return new Chart(ctx, {
    type: 'line',
    data: { labels:[], datasets:[{ data:[], borderColor:color, backgroundColor:color+'22',
      fill:true, tension:0.3, pointRadius:0, borderWidth:2 }] },
    options: commonOpts(yMax, color)
  });
}

charts.qos = makeChart('chart-qos', '#22c55e', 1.0);
charts.lat = makeChart('chart-lat', '#3b82f6', 2000, 'ms');
charts.cpu = makeChart('chart-cpu', '#06b6d4', 100);
charts.thr = makeChart('chart-thr', '#a855f7', 10);
charts.cong = makeChart('chart-cong', '#eab308', 100);

function updateChart(chart, labels, data) {
  chart.data.labels = labels;
  chart.data.datasets[0].data = data;
  chart.update('none');
}

function qosColor(v) {
  if (v >= 0.70) return 'var(--green)';
  if (v >= 0.50) return 'var(--yellow)';
  return 'var(--red)';
}
function cpuColor(v) {
  if (v < 60) return 'var(--cyan)';
  if (v < 80) return 'var(--yellow)';
  return 'var(--red)';
}
function pct(v) { return (v * 100).toFixed(0) + '%'; }

function update(d) {
  const qos = d.composite;
  const cpu = d.cpu_percent;
  const lat = (d.avg_proc_time * 1000).toFixed(0);

  // KPIs
  const qosEl = document.getElementById('kpi-qos');
  qosEl.textContent = qos.toFixed(3);
  qosEl.style.color = qosColor(qos);
  document.getElementById('bar-qos').style.width = (qos*100)+'%';
  document.getElementById('bar-qos').style.background = qosColor(qos);

  document.getElementById('kpi-cpu').textContent = cpu.toFixed(1) + '%';
  document.getElementById('kpi-cpu').style.color = cpuColor(cpu);
  document.getElementById('bar-cpu').style.width = cpu + '%';
  document.getElementById('bar-cpu').style.background = cpuColor(cpu);
  document.getElementById('kpi-cpu-node').textContent = d.node || '';

  document.getElementById('kpi-lat').textContent = lat + 'ms';
  document.getElementById('kpi-veh').textContent = (d.vehicle_count_avg||0).toFixed(1);

  const congPct = ((d.congestion_avg||0)*100).toFixed(1);
  document.getElementById('kpi-cong').textContent = 'congestión: ' + congPct + '%';

  // QoS breakdown
  ['lat','cpu','thr','rej'].forEach((k,i) => {
    const vals = [d.latency_score, d.cpu_score, d.throughput_score, d.rejection_rate];
    const v = vals[i] || 0;
    document.getElementById('qd-'+k).style.width = (v*100)+'%';
    document.getElementById('qn-'+k).textContent = v.toFixed(2);
  });

  // Node tag
  document.getElementById('node-tag').textContent = d.node || '–';
  const uptimeS = d.uptime_s || 0;
  document.getElementById('uptime-tag').textContent = 'uptime: ' + new Date(uptimeS*1000).toISOString().substr(11,8);

  // Estado migración en dot
  const dot = document.getElementById('dot');
  if (qos < MIGRATION_THRESHOLD) {
    dot.style.background = 'var(--red)';
    document.getElementById('node-info').textContent = '⚠️  QoS CRÍTICO — candidato para migración → vm1node';
  } else {
    dot.style.background = 'var(--green)';
    document.getElementById('node-info').textContent = `Nodo activo: ${d.node}  ·  total_frames: ${d.total_requests}  ·  errores: ${d.error_count}`;
  }

  // Historial
  if (d.history) {
    const h = d.history;
    const ts = h.timestamps || [];
    updateChart(charts.qos,  ts, h.composite  || []);
    updateChart(charts.lat,  ts, h.latency    || []);
    updateChart(charts.cpu,  ts, h.cpu        || []);
    updateChart(charts.thr,  ts, h.throughput || []);
    updateChart(charts.cong, ts, h.congestion || []);
    // Ajustar eje Y latencia dinámicamente
    const maxLat = Math.max(...(h.latency||[0]), 200);
    charts.lat.options.scales.y.max = Math.ceil(maxLat * 1.3 / 100) * 100;
    charts.lat.update('none');
  }
}

// Conectar SSE
function connect() {
  const es = new EventSource('/stream');
  es.onmessage = e => {
    try { update(JSON.parse(e.data)); } catch(err) {}
  };
  es.onerror = () => {
    document.getElementById('dot').style.background = 'var(--red)';
    setTimeout(connect, 3000);
  };
}
connect();
</script>
</body>
</html>
"""


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
