"""
run_local.py — Arranque local sin Docker ni k3s
================================================
Instala dependencias (si no están) y lanza el processor
y el simulator en dos procesos paralelos.

Uso:
    python run_local.py

Luego abre:
    http://localhost:8080/dashboard   ← Dashboard en vivo
    http://localhost:8080/qos         ← JSON con métricas QoS
    http://localhost:8080/docs        ← Swagger / OpenAPI
"""

import subprocess
import sys
import os
import time
import signal
import threading

ROOT = os.path.dirname(os.path.abspath(__file__))
APP  = os.path.join(ROOT, "app")

# ── Variables de entorno locales ──────────────────────────────────────────────
PROCESSOR_ENV = {
    **os.environ,
    "SERVICE_NAME":                   "traffic-processor-local",
    "PORT":                           "8080",
    # Sin collector local → OTEL logueará warnings, la app sigue funcionando
    "OTEL_EXPORTER_OTLP_ENDPOINT":    "http://localhost:4318",
    "QOS_LATENCY_BASELINE":           "0.05",
    "QOS_THROUGHPUT_BASELINE":        "3.0",
    "QOS_WINDOW_SECONDS":             "20",
    "SERVER_PIPELINE_REPEATS":        "3",    # reducido para no saturar tu máquina
    "MAX_IMAGE_MB":                   "10.0",
    "OTEL_EXPORT_INTERVAL_MS":        "5000",
    "PYTHONUNBUFFERED":               "1",
}

SIMULATOR_ENV = {
    **os.environ,
    "SERVICE_NAME":                   "camera-simulator-local",
    "SERVICE_ENDPOINT":               "http://localhost:8080",
    "OTEL_EXPORTER_OTLP_ENDPOINT":    "http://localhost:4318",
    "REQUEST_INTERVAL":               "0.4",
    "NUM_CAMERAS":                    "2",
    "MAX_VEHICLES":                   "12",
    "FRAME_WIDTH":                    "640",
    "FRAME_HEIGHT":                   "360",
    "TRAFFIC_WAVE":                   "true",
    "MAX_RETRIES":                    "3",
    "OTEL_EXPORT_INTERVAL_MS":        "5000",
    "PYTHONUNBUFFERED":               "1",
}


def install_deps():
    req = os.path.join(APP, "requirements.txt")
    print("📦  Instalando dependencias...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", req, "-q"],
        cwd=ROOT,
    )
    print("✅  Dependencias listas.\n")


def stream_output(proc, prefix, color_code):
    """Imprime stdout del proceso con prefijo coloreado."""
    reset = "\033[0m"
    color = f"\033[{color_code}m"
    for line in iter(proc.stdout.readline, b""):
        text = line.decode(errors="replace").rstrip()
        if text:
            print(f"{color}[{prefix}]{reset} {text}")


def main():
    print("\n🏙️  Smart City — Traffic Flow Analyzer (modo local)\n")

    # Instalar deps si faltan
    try:
        import fastapi, PIL, psutil, opentelemetry  # noqa
    except ImportError:
        install_deps()

    procs = []

    try:
        # ── 1. Arrancar traffic-processor ────────────────────────────────────
        print("🚀  Arrancando traffic-processor en :8080 ...")
        proc_server = subprocess.Popen(
            [sys.executable, "traffic_processor.py"],
            cwd=APP,
            env=PROCESSOR_ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc_server)
        t1 = threading.Thread(
            target=stream_output, args=(proc_server, "PROCESSOR", "34"), daemon=True
        )
        t1.start()

        # Esperar a que el servidor esté listo
        print("⏳  Esperando que el servidor arranque...")
        import urllib.request, urllib.error
        for attempt in range(20):
            time.sleep(1)
            try:
                urllib.request.urlopen("http://localhost:8080/health", timeout=2)
                print("✅  Servidor listo!\n")
                break
            except Exception:
                if attempt == 19:
                    print("❌  El servidor no respondió en 20s. Revisa los logs arriba.")
                    sys.exit(1)

        # ── 2. Arrancar camera-simulator ──────────────────────────────────────
        print("📷  Arrancando camera-simulator (2 cámaras) ...")
        proc_client = subprocess.Popen(
            [sys.executable, "camera_simulator.py", "--continuous"],
            cwd=APP,
            env=SIMULATOR_ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        procs.append(proc_client)
        t2 = threading.Thread(
            target=stream_output, args=(proc_client, "SIMULATOR", "32"), daemon=True
        )
        t2.start()

        print("\n" + "─" * 60)
        print("  🌐  Dashboard en vivo →  http://localhost:8080/dashboard")
        print("  📊  Métricas JSON     →  http://localhost:8080/qos")
        print("  📖  API Docs          →  http://localhost:8080/docs")
        print("─" * 60)
        print("  Ctrl+C para detener\n")

        # Esperar señal de parada
        while True:
            time.sleep(1)
            # Si algún proceso muere, salir
            for p in procs:
                if p.poll() is not None:
                    print(f"\n⚠️  Un proceso terminó inesperadamente (rc={p.poll()})")
                    raise KeyboardInterrupt

    except KeyboardInterrupt:
        print("\n\n🛑  Deteniendo...")
    finally:
        for p in procs:
            try:
                p.terminate()
                p.wait(timeout=5)
            except Exception:
                p.kill()
        print("✅  Detenido.\n")


if __name__ == "__main__":
    main()
