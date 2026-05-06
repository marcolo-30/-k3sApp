"""
Smart City - Camera Simulator
==============================
Genera frames sintéticos de tráfico urbano y los envía
de forma continua al traffic-processor.

Simula N cámaras en paralelo, cada una con densidad de
tráfico variable, y exporta métricas de latencia via OTEL.
"""

import os
import io
import time
import base64
import random
import logging
import argparse
import threading

import requests
from PIL import Image, ImageDraw

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [camera-sim] %(message)s"
)
logger = logging.getLogger("camera-sim")

# ─── Configuración ────────────────────────────────────────────────────────────
SERVICE_ENDPOINT       = os.getenv("SERVICE_ENDPOINT",             "http://traffic-processor-svc:8080")
OTEL_ENDPOINT          = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT",  "http://otel-collector.observability:4318")
SERVICE_NAME           = os.getenv("SERVICE_NAME",                 "camera-simulator")
REQUEST_INTERVAL       = float(os.getenv("REQUEST_INTERVAL",       "0.4"))   # segundos entre frames
REQUEST_TIMEOUT        = float(os.getenv("REQUEST_TIMEOUT",        "30.0"))
MAX_RETRIES            = int(os.getenv("MAX_RETRIES",              "3"))
MAX_RETRY_DELAY        = float(os.getenv("MAX_RETRY_DELAY",        "15.0"))
FRAME_WIDTH            = int(os.getenv("FRAME_WIDTH",              "640"))
FRAME_HEIGHT           = int(os.getenv("FRAME_HEIGHT",             "360"))
NUM_CAMERAS            = int(os.getenv("NUM_CAMERAS",              "3"))
MAX_VEHICLES           = int(os.getenv("MAX_VEHICLES",             "15"))
TRAFFIC_WAVE           = os.getenv("TRAFFIC_WAVE",                 "true").lower() == "true"
OTEL_EXPORT_INTERVAL_MS= int(os.getenv("OTEL_EXPORT_INTERVAL_MS", "5000"))

# Paleta de colores de vehículos
VEHICLE_COLORS = [
    (210, 40,  40),   # rojo
    (40,  90,  210),  # azul
    (40,  170, 40),   # verde
    (210, 170, 40),   # amarillo
    (170, 40,  210),  # morado
    (210, 110, 40),   # naranja
    (40,  190, 190),  # cyan
    (210, 70,  150),  # rosa
    (100, 100, 100),  # gris oscuro (camión)
    (240, 240, 240),  # blanco
]

ROAD_COLOR    = (80, 80, 80)
MARKING_COLOR = (240, 240, 160)
SIDEWALK      = (150, 140, 120)
SKY_COLOR     = (100, 140, 200)


# ─── Generación de frames ─────────────────────────────────────────────────────
def generate_traffic_frame(camera_id: str, num_vehicles: int, timestamp: float) -> bytes:
    """
    Genera un frame sintético de cámara de tráfico con:
    - Cielo + acera arriba
    - Calzada con marcas de carril
    - N vehículos (rectángulos coloreados con parabrisas)
    - Overlay con ID de cámara y timestamp
    """
    w, h = FRAME_WIDTH, FRAME_HEIGHT
    road_start = int(h * 0.30)   # la carretera empieza en 30% desde arriba

    img = Image.new("RGB", (w, h), ROAD_COLOR)
    draw = ImageDraw.Draw(img)

    # Cielo
    draw.rectangle([0, 0, w, road_start], fill=SKY_COLOR)

    # Aceras (lateral)
    draw.rectangle([0, road_start, int(w * 0.06), h], fill=SIDEWALK)
    draw.rectangle([int(w * 0.94), road_start, w, h], fill=SIDEWALK)

    # Perspectiva: líneas de horizonte
    for i in range(3):
        y = road_start + i * 4
        draw.line([(0, y), (w, y)], fill=(60, 60, 60), width=1)

    # Marcas de carril (líneas discontinuas)
    num_lanes = 3
    lane_w = int((w * 0.88) / num_lanes)
    x_start = int(w * 0.06)
    for lane in range(1, num_lanes):
        lx = x_start + lane * lane_w
        for y in range(road_start + 10, h - 10, 40):
            draw.rectangle([lx - 2, y, lx + 2, y + 20], fill=MARKING_COLOR)

    # Línea central (amarilla)
    cx = w // 2
    for y in range(road_start + 10, h - 10, 30):
        draw.rectangle([cx - 2, y, cx + 2, y + 18], fill=(220, 200, 50))

    # Límite de velocidad
    draw.rectangle([8, road_start + 10, 38, road_start + 50], fill=(255, 255, 255), outline=(0,0,0), width=1)
    draw.rectangle([12, road_start + 14, 34, road_start + 46], fill=(255, 0, 0))
    draw.text((14, road_start + 20), "50", fill=(255, 255, 255))

    # Vehículos
    num_vehicles = min(num_vehicles, MAX_VEHICLES)
    placed: list = []

    for _ in range(num_vehicles):
        color = random.choice(VEHICLE_COLORS)
        vw = random.randint(34, 58)
        vh = random.randint(22, 38)

        for _attempt in range(15):
            vx = random.randint(x_start + 5, w - x_start - vw - 5)
            vy = random.randint(road_start + 8, h - vh - 8)
            # Anti-overlap
            overlap = any(
                abs(vx - px) < vw + 8 and abs(vy - py) < vh + 6
                for px, py in placed
            )
            if not overlap:
                placed.append((vx, vy))
                # Carrocería
                draw.rectangle([vx, vy, vx + vw, vy + vh], fill=color, outline=(0, 0, 0), width=1)
                # Parabrisas (más claro)
                ws_x = vx + int(vw * 0.18)
                ws_y = vy + int(vh * 0.10)
                ws_w = int(vw * 0.64)
                ws_h = int(vh * 0.38)
                lighter = tuple(min(255, c + 70) for c in color)
                draw.rectangle([ws_x, ws_y, ws_x + ws_w, ws_y + ws_h], fill=lighter)
                # Ruedas (pequeños rectángulos negros)
                wheel_y = vy + vh - 4
                draw.rectangle([vx + 4, wheel_y, vx + 10, vy + vh + 3], fill=(20, 20, 20))
                draw.rectangle([vx + vw - 10, wheel_y, vx + vw - 4, vy + vh + 3], fill=(20, 20, 20))
                break

    # Overlay: cam ID + timestamp
    ts_str = time.strftime("%H:%M:%S", time.localtime(timestamp))
    draw.rectangle([4, 2, 180, 18], fill=(0, 0, 0, 160))
    draw.text((6, 3), f"{camera_id}  {ts_str}", fill=(255, 255, 255))

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


# ─── OTEL ─────────────────────────────────────────────────────────────────────
def setup_otel():
    resource = Resource.create({"service.name": SERVICE_NAME})
    exporter = OTLPMetricExporter(endpoint=f"{OTEL_ENDPOINT}/v1/metrics")
    reader   = PeriodicExportingMetricReader(exporter, export_interval_millis=OTEL_EXPORT_INTERVAL_MS)
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter(SERVICE_NAME)

    return (
        meter.create_histogram("client_request_latency_seconds",  unit="s"),
        meter.create_counter("client_requests_total"),
        meter.create_counter("client_request_errors_total"),
        meter.create_counter("client_retries_total"),
        meter.create_histogram("client_detected_vehicles"),
    )


# ─── Envío de frames ──────────────────────────────────────────────────────────
def send_frame(
    camera_id:   str,
    frame_bytes: bytes,
    meters:      tuple,
) -> bool:
    lat_hist, req_ctr, err_ctr, retry_ctr, veh_hist = meters
    labels = {"service": SERVICE_NAME, "camera": camera_id}

    payload = {
        "frame":      base64.b64encode(frame_bytes).decode(),
        "camera_id":  camera_id,
        "operations": [
            {"type": "blur",   "radius": 1},
            {"type": "resize", "width": FRAME_WIDTH, "height": FRAME_HEIGHT},
        ],
    }

    delay = 1.0
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.time()
        try:
            resp = requests.post(
                f"{SERVICE_ENDPOINT}/process",
                json=payload,
                timeout=REQUEST_TIMEOUT,
                headers={"X-Attempt": str(attempt)},
            )
            elapsed = time.time() - t0
            lat_hist.record(elapsed, labels)
            req_ctr.add(1, {**labels, "status": "success"})

            data       = resp.json()
            vehicles   = data.get("vehicle_count", 0)
            congestion = data.get("congestion_label", "–")
            qos        = data.get("qos", {}).get("composite", 0)
            node       = data.get("node", "–")
            veh_hist.record(vehicles, labels)

            logger.info(
                f"[{camera_id}] ok  attempt={attempt}  lat={elapsed*1000:.0f}ms  "
                f"vehicles={vehicles}  congestion={congestion}  qos={qos:.3f}  node={node}"
            )
            return True

        except Exception as exc:
            elapsed = time.time() - t0
            err_ctr.add(1, {**labels, "reason": type(exc).__name__})
            logger.warning(f"[{camera_id}] error attempt={attempt}: {exc}")
            if attempt < MAX_RETRIES:
                retry_ctr.add(1, labels)
                time.sleep(min(delay, MAX_RETRY_DELAY))
                delay *= 2

    req_ctr.add(1, {**labels, "status": "error"})
    return False


# ─── Bucle por cámara ─────────────────────────────────────────────────────────
def camera_loop(camera_id: str, meters: tuple, base_vehicles: int):
    logger.info(f"Cámara {camera_id} iniciada  →  {SERVICE_ENDPOINT}  base_vehicles={base_vehicles}")
    t0 = time.time()

    while True:
        # Ola de tráfico: cada 60s aumenta la densidad para saturar el RPi3
        if TRAFFIC_WAVE:
            wave_phase = (time.time() - t0) % 120
            if wave_phase < 60:
                extra = int(wave_phase / 6)   # 0→10 vehículos adicionales
            else:
                extra = max(0, int((120 - wave_phase) / 6))
        else:
            extra = random.randint(-2, 4)

        vehicles = max(1, base_vehicles + extra)
        frame    = generate_traffic_frame(camera_id, vehicles, time.time())
        send_frame(camera_id, frame, meters)
        time.sleep(REQUEST_INTERVAL)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Smart City Camera Simulator")
    parser.add_argument("--continuous", action="store_true", help="Modo continuo (k8s)")
    args = parser.parse_args()

    meters = setup_otel()
    logger.info(
        f"Camera Simulator iniciado  cameras={NUM_CAMERAS}  "
        f"interval={REQUEST_INTERVAL}s  endpoint={SERVICE_ENDPOINT}"
    )

    threads = []
    for i in range(NUM_CAMERAS):
        cam_id  = f"cam-{i+1:02d}"
        base_v  = random.randint(4, 10)
        t = threading.Thread(
            target=camera_loop,
            args=(cam_id, meters, base_v),
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.3)   # stagger de arranque

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
