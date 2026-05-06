"""
Smart City - Traffic Flow Processor  (RPi3-optimized)
======================================================
Cambios vs versión anterior:
 - SIN numpy  → ahorra ~150MB RSS en ARM64 (principal causa del OOM)
 - OTEL lazy  → el servidor responde /health antes de que OTEL termine de iniciar
 - startupProbe friendly: uvicorn arranca en <5s, OTEL en background
 - detect_vehicles con PIL puro (getdata)
 - ThreadPoolExecutor para no bloquear el event loop durante PIL ops
"""

import os, time, io, base64, threading, collections, json, asyncio, logging
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import psutil
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel
from PIL import Image, ImageFilter, ImageEnhance

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("traffic-processor")

# ── Config ────────────────────────────────────────────────────────────────────
SERVICE_NAME            = os.getenv("SERVICE_NAME",               "traffic-processor-r3")
OTEL_ENDPOINT           = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT","http://otel-collector.observability:4318")
PORT                    = int(os.getenv("PORT",                   "8080"))
QOS_LATENCY_BASELINE    = float(os.getenv("QOS_LATENCY_BASELINE", "0.3"))
QOS_THROUGHPUT_BASELINE = float(os.getenv("QOS_THROUGHPUT_BASELINE","2.0"))
QOS_WINDOW_SECONDS      = int(os.getenv("QOS_WINDOW_SECONDS",    "30"))
SERVER_PIPELINE_REPEATS = int(os.getenv("SERVER_PIPELINE_REPEATS","5"))
MAX_IMAGE_MB            = float(os.getenv("MAX_IMAGE_MB",        "10.0"))
OTEL_EXPORT_INTERVAL_MS = int(os.getenv("OTEL_EXPORT_INTERVAL_MS","5000"))

# ── QoS state ─────────────────────────────────────────────────────────────────
qos_lock = threading.Lock()
processing_events: collections.deque = collections.deque()
qos_state = dict(
    latency_score=1.0, cpu_score=1.0, throughput_score=1.0,
    rejection_rate=1.0, composite=1.0, avg_proc_time=0.0,
    cpu_percent=0.0, total_requests=0, error_count=0,
    vehicle_count_avg=0.0, congestion_avg=0.0,
    node=SERVICE_NAME, uptime_s=0,
)
_start_time = time.time()

HISTORY_LEN = 60
history_lock = threading.Lock()
history = {k: collections.deque(maxlen=HISTORY_LEN)
           for k in ("composite","cpu","latency","vehicles","congestion","throughput","timestamps")}

sse_subscribers: list = []
sse_lock = threading.Lock()

# OTEL meter refs (None until lazy init completes)
_frame_hist = None
_otel_ready  = False


# ── QoS compute loop ─────────────────────────────────────────────────────────
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

        avg_proc = sum(proc_times) / len(proc_times)
        cpu      = psutil.cpu_percent(interval=None)

        latency_score    = min(1.0, QOS_LATENCY_BASELINE / avg_proc) if avg_proc > 0 else 1.0
        cpu_score        = max(0.0, 1.0 - cpu / 100.0)
        wt               = len(events)
        rejection_rate   = max(0.0, 1.0 - (wt - sum(successes)) / wt)
        elapsed_w        = now - events[0][0] if len(events) > 1 else 1.0
        throughput_score = min(1.0, (wt / max(elapsed_w, 1)) / QOS_THROUGHPUT_BASELINE)

        composite = (latency_score * 0.40 + cpu_score * 0.30
                     + throughput_score * 0.15 + rejection_rate * 0.15)

        with qos_lock:
            qos_state.update(
                latency_score=round(latency_score, 4),
                cpu_score=round(cpu_score, 4),
                throughput_score=round(throughput_score, 4),
                rejection_rate=round(rejection_rate, 4),
                composite=round(composite, 4),
                avg_proc_time=round(avg_proc, 4),
                cpu_percent=round(cpu, 2),
                vehicle_count_avg=round(sum(vehicles)/len(vehicles), 2) if vehicles else 0,
                congestion_avg=round(sum(congestions)/len(congestions), 4) if congestions else 0,
                uptime_s=int(now - _start_time),
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
        snap["history"] = {k: list(v) for k, v in history.items()}
        _broadcast_sse(snap)

        logger.info(f"QoS composite={composite:.3f} cpu={cpu:.1f}% "
                    f"lat={avg_proc*1000:.0f}ms otel={'ok' if _otel_ready else 'init'}")


def _broadcast_sse(data: dict):
    payload = f"data: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_subscribers:
            try:    q.put_nowait(payload)
            except: dead.append(q)
        for q in dead:
            sse_subscribers.remove(q)


# ── OTEL — lazy init en background ────────────────────────────────────────────
def _init_otel_background():
    """
    Arranca en un thread daemon DESPUÉS de que uvicorn ya respondió /health.
    Si el collector no está disponible, falla silenciosamente.
    """
    global _frame_hist, _otel_ready
    try:
        # Import pesado dentro del thread, no en el arranque principal
        from opentelemetry import metrics as otel_metrics
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({"service.name": SERVICE_NAME})
        exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
        reader   = PeriodicExportingMetricReader(exporter,
                       export_interval_millis=OTEL_EXPORT_INTERVAL_MS)
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        otel_metrics.set_meter_provider(provider)
        meter = otel_metrics.get_meter(SERVICE_NAME)

        _frame_hist = meter.create_histogram(
            "server_frame_processing_seconds", unit="s")

        def obs(name):
            def cb(_):
                with qos_lock:
                    yield otel_metrics.Observation(
                        qos_state[name], {"service": SERVICE_NAME})
            return cb

        for name in ("composite","latency_score","cpu_score","throughput_score",
                     "rejection_rate","cpu_percent","avg_proc_time"):
            meter.create_observable_gauge(f"qos_{name}", callbacks=[obs(name)])
        meter.create_observable_gauge("traffic_vehicle_count_avg",
                                      callbacks=[obs("vehicle_count_avg")])
        meter.create_observable_gauge("traffic_congestion_level",
                                      callbacks=[obs("congestion_avg")])
        _otel_ready = True
        logger.info(f"OTEL listo → {OTEL_ENDPOINT}")
    except Exception as e:
        logger.warning(f"OTEL init fallido (continuando sin telemetría): {e}")


# ── FastAPI ───────────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # QoS loop
    threading.Thread(target=compute_qos_loop, daemon=True).start()
    # OTEL en background — NO bloquea el arranque del servidor
    threading.Thread(target=_init_otel_background, daemon=True).start()
    logger.info(f"Traffic Processor listo  node={SERVICE_NAME}  port={PORT}")
    yield
    _executor.shutdown(wait=False)

app = FastAPI(title="Smart City — Traffic Processor", version="1.1.0", lifespan=lifespan)


# ── Modelos ───────────────────────────────────────────────────────────────────
class FrameRequest(BaseModel):
    frame:      str
    camera_id:  str  = "cam-01"
    operations: list = []


# ── Procesamiento PIL puro (sin numpy) ────────────────────────────────────────
def detect_vehicles(img: Image.Image) -> tuple[int, float]:
    """
    Detección de vehículos sin numpy.
    Usa PIL getdata() → lista de tuplas RGB.
    ~160×90 = 14 400 píxeles → rápido en RPi3, no necesita numpy.
    """
    small   = img.convert("RGB").resize((160, 90), Image.BILINEAR)
    pixels  = list(small.getdata())   # list of (R,G,B)
    total   = len(pixels)             # 14 400
    WIDTH   = 160

    vehicle_px = 0
    col_hit    = [False] * WIDTH

    for i, (r, g, b) in enumerate(pixels):
        sat = max(r, g, b) - min(r, g, b)
        if sat > 45:                  # color saturado = vehículo
            vehicle_px += 1
            col_hit[i % WIDTH] = True

    congestion = vehicle_px / total

    count, in_blob = 0, False
    for v in col_hit:
        if v and not in_blob:
            count  += 1
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
        elif t == "resize":           img = img.resize((op.get("width",640), op.get("height",360)), Image.LANCZOS)
        elif t == "enhance_contrast": img = ImageEnhance.Contrast(img).enhance(op.get("factor",1.5))
        elif t == "edge_detect":      img = img.filter(ImageFilter.FIND_EDGES)
    return img


def amplify_cpu(img: Image.Image) -> Image.Image:
    """Carga CPU artificial — reducida a 5 repeats para no saturar RAM."""
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


# ── Función sync (corre en ThreadPoolExecutor) ────────────────────────────────
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
        camera_id=camera_id,
        vehicle_count=count,
        congestion_level=round(cong, 4),
        congestion_label=congestion_label(cong),
        processing_time_s=round(elapsed, 4),
        node=SERVICE_NAME,
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
        result = await loop.run_in_executor(
            _executor, _process_sync, raw, req.camera_id, req.operations)

        m = result.pop("_meta")
        logger.info(f"[{req.camera_id}] attempt={attempt} vehicles={m['count']} "
                    f"congestion={m['cong']:.1%} proc={m['elapsed']:.3f}s "
                    f"qos={result['qos']['composite']:.3f}")
        return JSONResponse(result)

    except HTTPException:
        raise
    except Exception as exc:
        with qos_lock:
            processing_events.append((time.time(), 0.0, False, 0, 0.0))
            qos_state["error_count"]    += 1
            qos_state["total_requests"] += 1
        logger.error(f"Error: {exc}")
        raise HTTPException(500, str(exc))


@app.get("/health")
def health():
    # Responde SIEMPRE de inmediato — sin esperar OTEL ni QoS
    return {"status": "ok", "service": SERVICE_NAME,
            "uptime_s": int(time.time() - _start_time),
            "otel": _otel_ready}


@app.get("/qos")
def get_qos():
    with qos_lock:
        return dict(qos_state)


# ── SSE stream ────────────────────────────────────────────────────────────────
@app.get("/stream")
async def sse_stream(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=20)
    with qos_lock:
        snap = dict(qos_state)
    with history_lock:
        snap["history"] = {k: list(v) for k, v in history.items()}
    with sse_lock:
        sse_subscribers.append(queue)

    async def generator():
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

    return StreamingResponse(generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*"})


# ── Dashboard HTML ────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(content=DASHBOARD_HTML)


DASHBOARD_HTML = (
    "<!DOCTYPE html>"
    "<html lang='es'>"
    "<head>"
    "<meta charset='UTF-8'>"
    "<meta name='viewport' content='width=device-width, initial-scale=1'>"
    "<title>Smart City Traffic</title>"
    "<script src='https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js'></script>"
    "<style>"
    ":root{--bg:#0f172a;--card:#1e293b;--border:#334155;--text:#e2e8f0;--muted:#94a3b8;"
    "--green:#22c55e;--yellow:#eab308;--red:#ef4444;--blue:#3b82f6;--cyan:#06b6d4;--purple:#a855f7}"
    "*{box-sizing:border-box;margin:0;padding:0}"
    "body{background:var(--bg);color:var(--text);font-family:system-ui,monospace;padding:16px}"
    "h1{font-size:18px;font-weight:700;margin-bottom:4px}"
    ".sub{font-size:12px;color:var(--muted);margin-bottom:20px}"
    ".dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);"
    "margin-right:6px;animation:pulse 1.5s infinite}"
    "@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}"
    ".g4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}"
    ".g2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}"
    ".card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}"
    ".lbl{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}"
    ".val{font-size:28px;font-weight:700;font-variant-numeric:tabular-nums}"
    ".sub2{font-size:11px;color:var(--muted);margin-top:3px}"
    ".bar{height:6px;background:var(--border);border-radius:3px;margin-top:8px}"
    ".bf{height:100%;border-radius:3px;transition:width .5s}"
    ".ct{font-size:12px;font-weight:600;color:var(--muted);margin-bottom:10px}"
    "canvas{max-height:160px}"
    ".qr{display:flex;align-items:center;gap:8px;font-size:12px;margin-bottom:8px}"
    ".qn{color:var(--muted);width:140px;flex-shrink:0}"
    ".qb{flex:1;height:10px;background:var(--border);border-radius:5px;overflow:hidden}"
    ".qbf{height:100%;border-radius:5px;transition:width .5s}"
    ".qv{width:40px;text-align:right;font-size:11px;font-weight:600}"
    "</style>"
    "</head>"
    "<body>"
    "<h1><span class='dot' id='dot'></span>Smart City — Traffic Monitor</h1>"
    "<p class='sub' id='info'>Conectando...</p>"
    "<div class='g4'>"
    "<div class='card'><div class='lbl'>QoS Composite</div>"
    "<div class='val' id='vqos' style='color:var(--green)'>-</div>"
    "<div class='bar'><div class='bf' id='bqos' style='background:var(--green);width:0'></div></div>"
    "<div class='sub2'>umbral migración: 0.50</div></div>"
    "<div class='card'><div class='lbl'>CPU</div>"
    "<div class='val' id='vcpu' style='color:var(--cyan)'>-</div>"
    "<div class='bar'><div class='bf' id='bcpu' style='background:var(--cyan);width:0'></div></div>"
    "<div class='sub2' id='scpu'>-</div></div>"
    "<div class='card'><div class='lbl'>Latencia</div>"
    "<div class='val' id='vlat' style='color:var(--blue)'>-</div>"
    "<div class='sub2'>ms por frame</div></div>"
    "<div class='card'><div class='lbl'>Vehículos avg</div>"
    "<div class='val' id='vveh' style='color:var(--purple)'>-</div>"
    "<div class='sub2' id='scong'>congestión: -</div></div>"
    "</div>"
    "<div class='g2'>"
    "<div class='card'><div class='ct'>QoS por Dimensión</div>"
    "<div class='qr'><span class='qn'>Latencia (40%)</span><div class='qb'><div class='qbf' id='qlat' style='background:var(--blue)'></div></div><span class='qv' id='nlat'>-</span></div>"
    "<div class='qr'><span class='qn'>CPU (30%)</span><div class='qb'><div class='qbf' id='qcpu' style='background:var(--cyan)'></div></div><span class='qv' id='ncpu'>-</span></div>"
    "<div class='qr'><span class='qn'>Throughput (15%)</span><div class='qb'><div class='qbf' id='qthr' style='background:var(--purple)'></div></div><span class='qv' id='nthr'>-</span></div>"
    "<div class='qr'><span class='qn'>Aceptación (15%)</span><div class='qb'><div class='qbf' id='qrej' style='background:var(--green)'></div></div><span class='qv' id='nrej'>-</span></div>"
    "<div style='margin-top:12px;font-size:11px;color:var(--muted)' id='ntag'>-</div></div>"
    "<div class='card'><div class='ct'>Congestión % (historial)</div><canvas id='ccong'></canvas></div>"
    "</div>"
    "<div class='g2'>"
    "<div class='card'><div class='ct'>QoS Composite</div><canvas id='cqos'></canvas></div>"
    "<div class='card'><div class='ct'>Latencia frames (ms)</div><canvas id='clat'></canvas></div>"
    "</div>"
    "<div class='g2'>"
    "<div class='card'><div class='ct'>CPU %</div><canvas id='ccpu'></canvas></div>"
    "<div class='card'><div class='ct'>Throughput (frames/s)</div><canvas id='cthr'></canvas></div>"
    "</div>"
    "<script>"
    "const THRESH=0.50;"
    "const mkChart=(id,color,yMax=1)=>{const c=document.getElementById(id).getContext('2d');"
    "return new Chart(c,{type:'line',data:{labels:[],datasets:[{data:[],borderColor:color,"
    "backgroundColor:color+'22',fill:true,tension:.3,pointRadius:0,borderWidth:2}]},"
    "options:{responsive:true,maintainAspectRatio:false,animation:{duration:200},"
    "plugins:{legend:{display:false}},"
    "scales:{x:{ticks:{color:'#64748b',font:{size:9},maxTicksLimit:6},grid:{color:'#1e293b'}},"
    "y:{min:0,max:yMax,ticks:{color:'#64748b',font:{size:9}},grid:{color:'#334155'}}}}});};"
    "const charts={qos:mkChart('cqos','#22c55e',1),lat:mkChart('clat','#3b82f6',2000),"
    "cpu:mkChart('ccpu','#06b6d4',100),thr:mkChart('cthr','#a855f7',10),cong:mkChart('ccong','#eab308',100)};"
    "function upd(c,labels,data){c.data.labels=labels;c.data.datasets[0].data=data;c.update('none');}"
    "function qc(v){return v>=.7?'var(--green)':v>=.5?'var(--yellow)':'var(--red)';}"
    "function cc(v){return v<60?'var(--cyan)':v<80?'var(--yellow)':'var(--red)';}"
    "function render(d){"
    "const q=d.composite,c=d.cpu_percent,l=(d.avg_proc_time*1000).toFixed(0);"
    "document.getElementById('vqos').textContent=q.toFixed(3);"
    "document.getElementById('vqos').style.color=qc(q);"
    "document.getElementById('bqos').style.width=(q*100)+'%';"
    "document.getElementById('bqos').style.background=qc(q);"
    "document.getElementById('vcpu').textContent=c.toFixed(1)+'%';"
    "document.getElementById('vcpu').style.color=cc(c);"
    "document.getElementById('bcpu').style.width=c+'%';"
    "document.getElementById('bcpu').style.background=cc(c);"
    "document.getElementById('scpu').textContent=d.node||'';"
    "document.getElementById('vlat').textContent=l+'ms';"
    "document.getElementById('vveh').textContent=(d.vehicle_count_avg||0).toFixed(1);"
    "document.getElementById('scong').textContent='congestión: '+((d.congestion_avg||0)*100).toFixed(1)+'%';"
    "['lat','cpu','thr','rej'].forEach((k,i)=>{"
    "const vals=[d.latency_score,d.cpu_score,d.throughput_score,d.rejection_rate];"
    "const v=vals[i]||0;"
    "document.getElementById('q'+k).style.width=(v*100)+'%';"
    "document.getElementById('n'+k).textContent=v.toFixed(2);});"
    "document.getElementById('ntag').textContent='nodo: '+(d.node||'-')+' · requests: '+d.total_requests+' · otel: '+(d.otel!==undefined?(d.otel?'ok':'init...'):'?');"
    "const dot=document.getElementById('dot'),info=document.getElementById('info');"
    "if(q<THRESH){dot.style.background='var(--red)';info.textContent='⚠️ QoS CRÍTICO — candidato para migración → vm1node';}"
    "else{dot.style.background='var(--green)';info.textContent='Nodo: '+(d.node||'-')+' · frames: '+d.total_requests+' · errores: '+d.error_count;}"
    "if(d.history){const h=d.history,ts=h.timestamps||[];"
    "upd(charts.qos,ts,h.composite||[]);upd(charts.lat,ts,h.latency||[]);"
    "upd(charts.cpu,ts,h.cpu||[]);upd(charts.thr,ts,h.throughput||[]);upd(charts.cong,ts,h.congestion||[]);"
    "const mx=Math.max(...(h.latency||[0]),200);charts.lat.options.scales.y.max=Math.ceil(mx*1.3/100)*100;charts.lat.update('none');}}"
    "function connect(){"
    "const es=new EventSource('/stream');"
    "es.onmessage=e=>{try{render(JSON.parse(e.data));}catch(err){}};"
    "es.onerror=()=>{document.getElementById('dot').style.background='var(--red)';setTimeout(connect,3000);};}"
    "connect();"
    "</script>"
    "</body></html>"
)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
