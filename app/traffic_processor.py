"""
Smart City - Traffic Flow Processor  (RPi3-optimized)
======================================================
- SIN numpy  -> ahorra ~150MB RSS en ARM64
- OTEL lazy  -> servidor responde /health antes de que OTEL inicie
- detect_vehicles con PIL puro (getdata)
- ThreadPoolExecutor para no bloquear el event loop
- Memory score: consulta Prometheus (mismo valor que Grafana) con fallback a psutil
"""

import os, time, io, base64, threading, collections, json, asyncio, logging, math
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import psutil
import urllib.request
import urllib.parse
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel
from PIL import Image, ImageFilter, ImageEnhance

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("traffic-processor")

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_NAME            = os.getenv("SERVICE_NAME",                "traffic-processor-r3")
OTEL_ENDPOINT           = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector.observability:4318")
PORT                    = int(os.getenv("PORT",                    "8080"))
QOS_LATENCY_BASELINE    = float(os.getenv("QOS_LATENCY_BASELINE",    "0.3"))
QOS_THROUGHPUT_BASELINE = float(os.getenv("QOS_THROUGHPUT_BASELINE",  "2.0"))
QOS_WINDOW_SECONDS      = int(os.getenv("QOS_WINDOW_SECONDS",         "30"))
SERVER_PIPELINE_REPEATS = int(os.getenv("SERVER_PIPELINE_REPEATS",    "5"))
MAX_IMAGE_MB            = float(os.getenv("MAX_IMAGE_MB",             "10.0"))
OTEL_EXPORT_INTERVAL_MS = int(os.getenv("OTEL_EXPORT_INTERVAL_MS",   "5000"))
QOS_CPU_NORMAL          = float(os.getenv("QOS_CPU_NORMAL",   "50"))
QOS_CPU_CRITICAL        = float(os.getenv("QOS_CPU_CRITICAL", "80"))
# Umbrales de memoria en % — misma escala que Grafana (via Prometheus)
QOS_MEM_NORMAL_PCT      = float(os.getenv("QOS_MEM_NORMAL_PCT",   "70"))
QOS_MEM_CRITICAL_PCT    = float(os.getenv("QOS_MEM_CRITICAL_PCT", "80"))
# Prometheus para leer memoria igual que Grafana
PROMETHEUS_URL          = os.getenv("PROMETHEUS_URL", "http://192.168.0.42:9090")
# Nombre del nodo en las métricas k8s (label k8s_node_name)
K8S_NODE_NAME           = os.getenv("K8S_NODE_NAME", "r3-node")

# ── QoS state ─────────────────────────────────────────────────────────────────
qos_lock = threading.Lock()
processing_events: collections.deque = collections.deque()
qos_state = dict(
    latency_score=1.0, cpu_score=1.0, memory_score=1.0,
    throughput_score=1.0, rejection_rate=1.0, composite=1.0,
    avg_proc_time=0.0, cpu_percent=0.0, mem_mb=0.0, mem_pct=0.0,
    mem_source="psutil",
    total_requests=0, error_count=0,
    vehicle_count_avg=0.0, congestion_avg=0.0,
    node=SERVICE_NAME, uptime_s=0,
)
_start_time = time.time()

HISTORY_LEN = 60
history_lock = threading.Lock()
history = {k: collections.deque(maxlen=HISTORY_LEN)
           for k in ("composite", "cpu", "latency", "vehicles",
                     "congestion", "throughput", "timestamps")}

sse_subscribers: list = []
sse_lock = threading.Lock()
_frame_hist = None
_otel_ready = False


# ── Memoria desde Prometheus (= mismo valor que Grafana) ─────────────────────
def _query_prometheus_mem() -> tuple:
    """
    Retorna (mem_mb, mem_pct) usando la misma formula que el panel de Grafana:
      usage / (usage + available) * 100
    Lanza excepcion si Prometheus no responde -> caller usa psutil como fallback.
    """
    def prom_query(expr: str) -> float:
        url = PROMETHEUS_URL + "/api/v1/query?query=" + urllib.parse.quote(expr)
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as r:
            data = json.loads(r.read())
        result = data["data"]["result"]
        if not result:
            raise ValueError("no data")
        return float(result[0]["value"][1])

    node   = K8S_NODE_NAME
    usage  = prom_query(f'k8s_node_memory_usage_bytes{{k8s_node_name="{node}"}}')
    avail  = prom_query(f'k8s_node_memory_available_bytes{{k8s_node_name="{node}"}}')
    mem_mb  = usage / (1024 * 1024)
    mem_pct = usage / (usage + avail) * 100
    return mem_mb, mem_pct


def get_mem_metrics() -> tuple:
    """
    Intenta Prometheus primero. Si falla usa psutil como fallback.
    Retorna (mem_mb, mem_pct, source)
    """
    try:
        mem_mb, mem_pct = _query_prometheus_mem()
        return mem_mb, mem_pct, "prometheus"
    except Exception as e:
        logger.warning("Prometheus mem fallback a psutil: %s", e)
        vm = psutil.virtual_memory()
        mem_mb  = (vm.total - vm.available) / (1024 * 1024)
        mem_pct = (vm.total - vm.available) / vm.total * 100
        return mem_mb, mem_pct, "psutil"


# ── QoS compute loop ──────────────────────────────────────────────────────────
def compute_qos_loop():
    while True:
        time.sleep(5)
        now    = time.time()
        cutoff = now - QOS_WINDOW_SECONDS
        with qos_lock:
            while processing_events and processing_events[0][0] < cutoff:
                processing_events.popleft()
            events = list(processing_events)

        if not events:
            continue

        proc_times  = [e[1] for e in events]
        successes   = [e[2] for e in events]
        vehicles    = [e[3] for e in events]
        congestions = [e[4] for e in events]

        avg_proc              = sum(proc_times) / len(proc_times)
        cpu                   = psutil.cpu_percent(interval=None)
        mem_mb, mem_pct, mem_source = get_mem_metrics()

        # Latencia
        latency_score = min(1.0, QOS_LATENCY_BASELINE / avg_proc) if avg_proc > 0 else 1.0

        # CPU lineal
        if cpu <= QOS_CPU_NORMAL:
            cpu_score = 1.0
        elif cpu >= QOS_CPU_CRITICAL:
            cpu_score = 0.0
        else:
            cpu_score = 1.0 - (cpu - QOS_CPU_NORMAL) / (QOS_CPU_CRITICAL - QOS_CPU_NORMAL)

        # Memoria piecewise (% de Prometheus = Grafana):
        #   <= NORMAL_PCT (64%) : score = 1.0
        #   68%                 : score = 0.70  (pivot)
        #   >= CRITICAL_PCT (72%): score = 0.0
        _MEM_PIVOT = 68.0
        if mem_pct <= QOS_MEM_NORMAL_PCT:
            memory_score = 1.0
        elif mem_pct >= QOS_MEM_CRITICAL_PCT:
            memory_score = 0.0
        elif mem_pct <= _MEM_PIVOT:
            t = (mem_pct - QOS_MEM_NORMAL_PCT) / (_MEM_PIVOT - QOS_MEM_NORMAL_PCT)
            memory_score = 1.0 - 0.3 * t        # 1.0 → 0.70 linealmente
        else:
            t = (mem_pct - _MEM_PIVOT) / (QOS_MEM_CRITICAL_PCT - _MEM_PIVOT)
            memory_score = 0.7 * (1.0 - t)      # 0.70 → 0.0 linealmente

        wt               = len(events)
        rejection_rate   = max(0.0, 1.0 - (wt - sum(successes)) / wt)
        elapsed_w        = now - events[0][0] if len(events) > 1 else 1.0
        throughput_score = min(1.0, (wt / max(elapsed_w, 1)) / QOS_THROUGHPUT_BASELINE)

        composite = (latency_score    * 0.25   # reducido: r3 es lento por hardware
                   + cpu_score        * 0.35   # aumentado: CPU es mejor indicador
                   + memory_score     * 0.25
                   + throughput_score * 0.10
                   + rejection_rate   * 0.05)

        with qos_lock:
            qos_state.update(
                latency_score    = round(latency_score,    4),
                cpu_score        = round(cpu_score,        4),
                memory_score     = round(memory_score,     4),
                throughput_score = round(throughput_score, 4),
                rejection_rate   = round(rejection_rate,   4),
                composite        = round(composite,        4),
                avg_proc_time    = round(avg_proc,         4),
                cpu_percent      = round(cpu,              2),
                mem_mb           = round(mem_mb,           1),
                mem_pct          = round(mem_pct,          1),
                mem_source       = mem_source,
                vehicle_count_avg= round(sum(vehicles)/len(vehicles), 2) if vehicles else 0,
                congestion_avg   = round(sum(congestions)/len(congestions), 4) if congestions else 0,
                uptime_s         = int(now - _start_time),
            )

        ts = time.strftime("%H:%M:%S")
        with history_lock:
            history["composite"].append(round(composite, 3))
            history["cpu"].append(round(cpu, 1))
            history["latency"].append(round(avg_proc * 1000, 1))
            history["vehicles"].append(round(sum(vehicles)/len(vehicles), 1) if vehicles else 0)
            history["congestion"].append(round(sum(congestions)/len(congestions)*100, 1) if congestions else 0)
            history["throughput"].append(round(wt / max(elapsed_w, 1), 2))
            history["timestamps"].append(ts)

        with qos_lock:
            snap = dict(qos_state)
        snap["history"]          = {k: list(v) for k, v in history.items()}
        snap["cpu_normal"]       = QOS_CPU_NORMAL
        snap["cpu_critical"]     = QOS_CPU_CRITICAL
        snap["mem_normal_pct"]   = QOS_MEM_NORMAL_PCT
        snap["mem_critical_pct"] = QOS_MEM_CRITICAL_PCT
        _broadcast_sse(snap)

        logger.info("QoS composite=%.3f cpu=%.1f%% mem=%.0fMB(%.1f%% %s) lat=%.0fms mem_score=%.3f",
                    composite, cpu, mem_mb, mem_pct, mem_source, avg_proc * 1000, memory_score)


def _broadcast_sse(data: dict):
    payload = "data: " + json.dumps(data) + "\n\n"
    with sse_lock:
        dead = []
        for q in sse_subscribers:
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        for q in dead:
            sse_subscribers.remove(q)


# ── OTEL lazy init ────────────────────────────────────────────────────────────
def _init_otel_background():
    global _frame_hist, _otel_ready
    try:
        from opentelemetry import metrics as otel_metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({"service.name": SERVICE_NAME})
        exporter = OTLPMetricExporter(endpoint=OTEL_ENDPOINT + "/v1/metrics")
        reader   = PeriodicExportingMetricReader(exporter,
                       export_interval_millis=OTEL_EXPORT_INTERVAL_MS)
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        otel_metrics.set_meter_provider(provider)
        meter = otel_metrics.get_meter(SERVICE_NAME)

        _frame_hist = meter.create_histogram("server_frame_processing_seconds", unit="s")

        def obs(name):
            def cb(_):
                with qos_lock:
                    yield otel_metrics.Observation(qos_state[name], {"service": SERVICE_NAME})
            return cb

        for name in ("composite", "latency_score", "cpu_score", "memory_score",
                     "throughput_score", "rejection_rate", "cpu_percent",
                     "avg_proc_time", "mem_mb", "mem_pct"):
            meter.create_observable_gauge("qos_" + name, callbacks=[obs(name)])
        meter.create_observable_gauge("traffic_vehicle_count_avg", callbacks=[obs("vehicle_count_avg")])
        meter.create_observable_gauge("traffic_congestion_level",  callbacks=[obs("congestion_avg")])

        _otel_ready = True
        logger.info("OTEL listo -> %s", OTEL_ENDPOINT)
    except Exception as exc:
        logger.warning("OTEL init fallido: %s", exc)


# ── FastAPI ───────────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=compute_qos_loop,      daemon=True).start()
    threading.Thread(target=_init_otel_background, daemon=True).start()
    logger.info("Traffic Processor listo  node=%s  port=%s", SERVICE_NAME, PORT)
    yield
    _executor.shutdown(wait=False)


app = FastAPI(title="Smart City Traffic Processor", version="1.5.0", lifespan=lifespan)


class FrameRequest(BaseModel):
    frame:      str
    camera_id:  str  = "cam-01"
    operations: list = []


# ── Procesamiento PIL puro ────────────────────────────────────────────────────
def detect_vehicles(img: Image.Image):
    small   = img.convert("RGB").resize((160, 90), Image.BILINEAR)
    pixels  = list(small.getdata())
    total   = len(pixels)
    WIDTH   = 160
    vehicle_px = 0
    col_hit    = [False] * WIDTH
    for i, (r, g, b) in enumerate(pixels):
        sat = max(r, g, b) - min(r, g, b)
        if sat > 45:
            vehicle_px += 1
            col_hit[i % WIDTH] = True
    congestion = vehicle_px / total
    count, in_blob = 0, False
    for v in col_hit:
        if v and not in_blob:
            count += 1
            in_blob = True
        elif not v:
            in_blob = False
    return count, congestion


def apply_operations(img: Image.Image, operations: list) -> Image.Image:
    for op in operations:
        t = op.get("type", "")
        if   t == "blur":             img = img.filter(ImageFilter.GaussianBlur(op.get("radius", 2)))
        elif t == "sharpen":          img = img.filter(ImageFilter.SHARPEN)
        elif t == "grayscale":        img = img.convert("L").convert("RGB")
        elif t == "resize":           img = img.resize((op.get("width", 640), op.get("height", 360)), Image.LANCZOS)
        elif t == "enhance_contrast": img = ImageEnhance.Contrast(img).enhance(op.get("factor", 1.5))
        elif t == "edge_detect":      img = img.filter(ImageFilter.FIND_EDGES)
    return img


def amplify_cpu(img: Image.Image) -> Image.Image:
    for _ in range(SERVER_PIPELINE_REPEATS):
        img = img.filter(ImageFilter.GaussianBlur(radius=1))
        img = img.filter(ImageFilter.SHARPEN)
        img = ImageEnhance.Contrast(img).enhance(1.05)
    return img


def congestion_label(level: float) -> str:
    if level < 0.08: return "libre"
    if level < 0.20: return "fluido"
    if level < 0.40: return "moderado"
    if level < 0.65: return "congestionado"
    return "colapso"


def _process_sync(raw: bytes, camera_id: str, operations: list) -> dict:
    start = time.time()
    img   = Image.open(io.BytesIO(raw))
    img   = apply_operations(img, operations)
    count, cong = detect_vehicles(img)
    amplify_cpu(img)
    elapsed = time.time() - start

    with qos_lock:
        processing_events.append((time.time(), elapsed, True, count, cong))
        qos_state["total_requests"] += 1

    if _frame_hist:
        _frame_hist.record(elapsed, {"service": SERVICE_NAME, "camera": camera_id})

    with qos_lock:
        snap = dict(qos_state)

    return dict(
        camera_id        = camera_id,
        vehicle_count    = count,
        congestion_level = round(cong, 4),
        congestion_label = congestion_label(cong),
        processing_time_s= round(elapsed, 4),
        node             = SERVICE_NAME,
        qos=dict(composite=snap["composite"], latency_score=snap["latency_score"],
                 cpu_score=snap["cpu_score"], cpu_percent=snap["cpu_percent"]),
        _meta=dict(elapsed=elapsed, count=count, cong=cong),
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/process")
async def process_frame(req: FrameRequest, request: Request):
    attempt = int(request.headers.get("X-Attempt", "1"))
    try:
        raw = base64.b64decode(req.frame)
        if len(raw) > MAX_IMAGE_MB * 1024 * 1024:
            raise HTTPException(413, "Frame demasiado grande")
        loop   = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, _process_sync, raw, req.camera_id, req.operations)
        m = result.pop("_meta")
        logger.info("[%s] attempt=%s vehicles=%s congestion=%.1f%% proc=%.3fs qos=%.3f",
                    req.camera_id, attempt, m["count"], m["cong"]*100,
                    m["elapsed"], result["qos"]["composite"])
        return JSONResponse(result)
    except HTTPException:
        raise
    except Exception as exc:
        with qos_lock:
            processing_events.append((time.time(), 0.0, False, 0, 0.0))
            qos_state["error_count"]    += 1
            qos_state["total_requests"] += 1
        logger.error("Error procesando frame: %s", exc)
        raise HTTPException(500, str(exc))


@app.get("/health")
def health():
    return {"status": "ok", "service": SERVICE_NAME,
            "uptime_s": int(time.time() - _start_time),
            "otel": _otel_ready}


@app.get("/qos")
def get_qos():
    with qos_lock:
        return dict(qos_state)


@app.get("/stream")
async def sse_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=20)
    with qos_lock:
        snap = dict(qos_state)
    with history_lock:
        snap["history"] = {k: list(v) for k, v in history.items()}
    snap["cpu_normal"]       = QOS_CPU_NORMAL
    snap["cpu_critical"]     = QOS_CPU_CRITICAL
    snap["mem_normal_pct"]   = QOS_MEM_NORMAL_PCT
    snap["mem_critical_pct"] = QOS_MEM_CRITICAL_PCT
    with sse_lock:
        sse_subscribers.append(queue)

    async def generator():
        yield "data: " + json.dumps(snap) + "\n\n"
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
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ── Dashboard ─────────────────────────────────────────────────────────────────
import pathlib as _pathlib
_DASHBOARD_PATH = _pathlib.Path(__file__).parent / "dashboard.html"


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=_DASHBOARD_PATH.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
