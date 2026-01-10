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
#API_URL = "http://visual_validator_api:8000/verify"
#HEALTH_URL = "http://visual_validator_api:8000/health"
#API_URL = "http://192.168.1.211:8005/verify"
#HEALTH_URL = "http://192.168.1.211:8005/health"
API_KEY = "seestocks_secret_key_wwRT"
API_URL = "http://visual_validator_api:8000/verify"
HEALTH_URL = "http://visual_validator_api:8000/health"


MAX_WORKERS =32
db_lock = threading.Lock()
session = requests.Session()
session.headers.update({"X-API-Key": API_KEY})

def get_url_hash(url):
    return hashlib.sha256(url.encode()).hexdigest()

def count_lines(file_path):
    try:
        with open(file_path, 'rb') as f:
            return sum(1 for _ in f) - 1
    except: return 0

def validar_imagen(row, conn, stats, total_rows):
    url = row.get('imagenes_producto', '').split(',')[0].strip()
    if not url: return
    url_hash = get_url_hash(url)

    # Datos extraídos del CSV
    titulo = row.get('titulo', '')
    descripcion = row.get('descripcion', '')
    cuerpo_Es = row.get('cuerpo_Es', '')
    atributos = row.get('atributos', '')
    categoria = row.get('categoria', '')

    # --- PASO 3: AJUSTE FINO AUTO-RESUME ---
    # Saltamos si ya está validada Y tiene los textos.
    # Nota: image_suggest_category ahora puede estar vacío si la confianza fue baja, 
    # pero eso cuenta como procesado.
    with db_lock:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT is_valid FROM downloads 
            WHERE url_hash = ? 
              AND is_valid IS NOT NULL 
              AND titulo IS NOT NULL 
        """, (url_hash,))
        if cursor.fetchone():
            stats["total"] += 1
            stats["saltadas"] += 1
            return

    rel_path = f"{url_hash[:2]}/{url_hash[2:4]}/{url_hash}.jpg"
    img_path = os.path.join(IMG_BASE_DIR, rel_path)

    # Si no hay imagen, al menos guardamos los textos (Enriquecimiento parcial)
    if not os.path.exists(img_path):
        with db_lock:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE downloads SET 
                    titulo=?, descripcion=?, cuerpo_Es=?, atributos=?, categoria=?
                WHERE url_hash=? """, (titulo, descripcion, cuerpo_Es, atributos, categoria, url_hash))
            stats["total"] += 1
        return

    try:
        # Optimización de imagen (Pre-resize)
        with Image.open(img_path) as img:
            img = img.convert("RGB").resize((224, 224), Image.Resampling.BILINEAR) 
            #img = img.convert("RGB").resize((224, 224), Image.Resampling.LANCZOS)
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=85)
            img_bytes = img_byte_arr.getvalue()

        # Llamada a la API de validación
        r = session.post(API_URL, data={"title": titulo, "category": categoria}, 
                         files={"file": ("img.jpg", img_bytes, "image/jpeg")}, timeout=20)
        
        if r.status_code == 200:
            res = r.json()
            det = res['detections']
            v = 1 if res['is_valid'] else 0
            
            # --- CAMBIO V2: MANEJO DE CONFIANZA ---
            # Si la API devuelve null (baja confianza), guardamos cadena vacía.
            suggested_cat = res.get('image_suggest_category') 
            cat_to_save = suggested_cat if suggested_cat else ""
            
            # (Opcional) Debugging visual en logs si hay baja confianza
            # if not suggested_cat:
            #     print(f"  [DEBUG] Low Conf: {res.get('image_suggest_confidence', 0):.3f} para {url_hash[:8]}")

            with db_lock:
                cursor = conn.cursor()
                # Actualización masiva
                cursor.execute("""
                    UPDATE downloads SET 
                        is_valid = ?, confidence = ?, score_category = ?, 
                        score_product = ?, score_watermark = ?, 
                        score_placeholder = ?, score_quality = ?,
                        titulo = ?, descripcion = ?, cuerpo_Es = ?, 
                        atributos = ?, categoria = ?, 
                        image_suggest_category = ?
                    WHERE url_hash = ?
                """, (v, res['confidence'], det['category_match'], det['product_match'],
                      det['watermark_text'], det['placeholder_or_error'], det['low_quality'],
                      titulo, descripcion, cuerpo_Es, atributos, categoria, 
                      cat_to_save, # <--- Guardamos "" si la IA duda
                      url_hash))
                
                stats["total"] += 1
                if v: stats["validas"] += 1 
                else: stats["rechazadas"] += 1
                
                # Checkpoint cada 100 imágenes
                if stats["total"] % 100 == 0:
                    conn.commit()
                    p = (stats["total"] / total_rows) * 100 if total_rows > 0 else 0
                    print(f"  💾 [{p:.1f}%] {stats['total']:,} / {total_rows:,} (Procesadas V2)")
        else:
            with db_lock: stats["errores"] += 1
    except Exception as e:
        with db_lock:
            stats["errores"] += 1
            print(f"  ⚠️ Error en {url_hash[:8]}: {e}")

def procesar():
    # Verificar salud de la API
    try:
        r = session.get(HEALTH_URL, timeout=5)
        if r.status_code != 200: raise Exception()
        print(f"✅ API Online: {r.json().get('engine', 'Desconocido')}")
    except:
        print("🛑 ERROR: La API de validación está caída."); sys.exit(1)

    os.makedirs(DONE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    
    # Buscar archivos CSV
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    if not files:
        print("📭 No hay archivos CSV en la carpeta simplified. ¿Olvidaste moverlos de vuelta?")
        return

    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        total_rows = count_lines(file_path)
        print(f"\n🚀 Procesando: {filename} ({total_rows:,} registros)")
        
        stats = {"total": 0, "validas": 0, "rechazadas": 0, "errores": 0, "saltadas": 0}

        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for row in reader:
                    executor.submit(validar_imagen, row, conn, stats, total_rows)

        conn.commit()
        # Mover archivo a procesados
        shutil.move(file_path, os.path.join(DONE_DIR, filename))
        print(f"✅ Finalizado: {filename}")
        print(f"📊 Resumen: {stats['validas']} OK | {stats['rechazadas']} KO | {stats['saltadas']} Saltadas\n")

    conn.close()
    print("🏁 Fin del proceso de enriquecimiento.")

if __name__ == "__main__":
    procesar()
