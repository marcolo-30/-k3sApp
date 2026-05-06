# ── Smart City Traffic App ────────────────────────────────────────────────────
# Multi-platform: linux/amd64 (vm1node) + linux/arm64 (r3-node RPi3)
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

# Dependencias del sistema (PIL necesita libjpeg, libpng)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libjpeg-dev \
        libpng-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias Python primero (caché de Docker)
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación
COPY app/ .

EXPOSE 8080

# Por defecto arranca el processor.
# El camera-simulator se lanza con command override en el manifest de k8s.
CMD ["python", "traffic_processor.py"]
