import sqlite3
import requests
import os
import csv
import hashlib
import shutil
import sys
import threading
import io
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURACIÓN ---
INPUT_DIR = "/lab/visualdata-ia/data_in/simplified"
DONE_DIR = "/lab/visualdata-ia/data_in/03indatabase"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
IMG_BASE_DIR = "/lab/visualdata-ia/imagenes_in"
API_URL = "http://visual_validator_api:8000/verify"
HEALTH_URL = "http://visual_validator_api:8000/health"
API_KEY = "seestocks_secret_key_wwRT"

MAX_WORKERS = 4 
db_lock = threading.Lock()
session = requests.Session()
session.headers.update({"X-API-Key": API_KEY})

def get_url_hash(url):
    return hashlib.sha256(url.encode()).hexdigest()

def count_lines(file_path):
    """Cuenta el total de líneas del CSV de forma rápida."""
    with open(file_path, 'rb') as f:
        return sum(1 for _ in f) - 1 # Restamos 1 por la cabecera

def validar_imagen(row, conn, stats, total_rows):
    url = row.get('imagenes_producto', '').split(',')[0].strip()
    if not url: return

    url_hash = get_url_hash(url)

    # AUTO-RESUME
    with db_lock:
        cursor = conn.cursor()
        cursor.execute("SELECT is_valid FROM downloads WHERE url_hash = ? AND is_valid IS NOT NULL", (url_hash,))
        if cursor.fetchone():
            stats["total"] += 1
            stats["saltadas"] += 1
            return

    rel_path = f"{url_hash[:2]}/{url_hash[2:4]}/{url_hash}.jpg"
    img_path = os.path.join(IMG_BASE_DIR, rel_path)

    if not os.path.exists(img_path):
        with db_lock: stats["total"] += 1
        return

    try:
        with Image.open(img_path) as img:
            img = img.convert("RGB")
            img = img.resize((224, 224), Image.Resampling.LANCZOS)
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=85)
            img_bytes = img_byte_arr.getvalue()

        data = {"title": row.get('titulo', 'producto'), "category": row.get('categoria', 'general')}
        r = session.post(API_URL, data=data, files={"file": ("img.jpg", img_bytes, "image/jpeg")}, timeout=20)
        
        if r.status_code == 200:
            res = r.json()
            det = res['detections']
            v = 1 if res['is_valid'] else 0
            
            with db_lock:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE downloads SET 
                        is_valid = ?, confidence = ?, score_category = ?, 
                        score_product = ?, score_watermark = ?, 
                        score_placeholder = ?, score_quality = ?
                    WHERE url_hash = ?
                """, (v, res['confidence'], det['category_match'], det['product_match'],
                      det['watermark_text'], det['placeholder_or_error'], 
                      det['low_quality'], url_hash))
                
                stats["total"] += 1
                if v: stats["validas"] += 1 
                else: stats["rechazadas"] += 1
                
                if stats["total"] % 100 == 0:
                    conn.commit()
                    porcentaje = (stats["total"] / total_rows) * 100
                    print(f"  💾 [{porcentaje:.1f}%] {stats['total']:,} / {total_rows:,} procesadas... (Saltadas: {stats['saltadas']:,})")
        else:
            with db_lock: stats["errores"] += 1
    except Exception as e:
        with db_lock:
            stats["errores"] += 1
            print(f"  ⚠️ Error en {url_hash[:8]}: {e}")

def procesar():
    try:
        r = session.get(HEALTH_URL, timeout=5)
        if r.status_code != 200: raise Exception()
    except:
        print("🛑 API OFF"); sys.exit(1)

    os.makedirs(DONE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        
        print(f"📊 Contando registros en {filename}...")
        total_rows = count_lines(file_path) # <-- Cálculo del total
        print(f"🚀 Iniciando: {total_rows:,} productos encontrados.")
        
        stats = {"total": 0, "validas": 0, "rechazadas": 0, "errores": 0, "saltadas": 0}

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for row in reader:
                    executor.submit(validar_imagen, row, conn, stats, total_rows)

        conn.commit()
        shutil.move(file_path, os.path.join(DONE_DIR, filename))
        print(f"✅ Finalizado: {filename}\n")

    conn.close()

if __name__ == "__main__":
    procesar()
