FROM python:3.11-slim

# Instalamos dependencias del sistema
RUN apt-get update && apt-get install -y \
    libmagic-dev \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalamos las librerías de Python necesarias para ambos scripts (00 y 01)
# Agregamos httpx, requests, beautifulsoup4 y lxml
RUN pip install --no-cache-dir \
    httpx \
    pandas \
    python-magic \
    beautifulsoup4 \
    requests \
    lxml \
    tqdm \
    aiofiles \
    Pillow

# Copiamos el código
COPY . .

# Comando por defecto (puedes cambiarlo a tail -f /dev/null si quieres 
# que el contenedor no se apague nunca)
CMD ["python", "00-descargaimagenes.py"]
