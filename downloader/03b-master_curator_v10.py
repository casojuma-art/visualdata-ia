import sqlite3
import requests
import time
import re
import json
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from difflib import get_close_matches
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================
# CONFIGURACIÓN (V10 FINAL + LECTURA SEGURA)
# =================================================================
DEBUG_MODE = False          
BATCH_SIZE = 256
MAX_WORKERS =   64       # <--- TUS 38 HILOS

API_URL = "http://192.168.1.211:8000/v1/chat/completions"
#API_URL = "http://192.168.1.211:8001/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# === OPTIMIZACIÓN TÉCNICA: SESSION ===
session = requests.Session()
adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
session.mount('http://', adapter)
session.mount('https://', adapter)
# =====================================

print("Cargando taxonomía...")
try:
    tax_df = pd.read_csv(TAXONOMY_PATH)
    VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}
    LEAF_TO_PATH = {path.split('>')[-1].strip().lower(): path for path in tax_df['path'].unique()}
    VALID_KEYS_LIST = list(VALID_PATHS.keys())
    print(f"Taxonomía cargada: {len(VALID_PATHS)} rutas.")
except Exception as e:
    print(f"❌ ERROR CRÍTICO cargando CSV: {e}")
    exit()

# =================================================================
# TU PROMPT V10 ORIGINAL
# =================================================================
SYSTEM_PROMPT = """
Eres un clasificador estrictamente exacto de productos para taxonomía GPC.

REGLAS IMPRESCINDIBLES:
- Elige ÚNICAMENTE una categoría que exista EXACTAMENTE en la taxonomía GPC oficial.
- NO inventes, no modifiques, no abbreviates ni traduzcas nombres de categorías.
- Usa solo rutas completas tal como aparecen en la taxonomía.

Tarea:
Con el título, descripción, atributos, sugerencia BERT y CLIP, elige la categoría GPC más precisa y específica posible.
Extrae y normaliza atributos a estándares de Google Merchant. Usa IA para inferir/normalizar valores si no son explícitos.
- Para "Sexo": Solo "Hombre", "Mujer" o "Unisex". Mapea variaciones (ej: "Niño" -> "Hombre", "Niña" -> "Mujer", "Infantil" -> "Unisex").
- Para "Grupo de edad": Solo "recién nacido", "bebé", "infante", "niños" o "adulto". Inferir de edades (ej: "+3 años" -> "niños", "Adulto" -> "adulto", "Bebé" -> "bebé").
Rellena atributos_estandar SOLO con evidencia (del texto o inferencia lógica), si no, usa null.
Para atributos no estándar, usa detalles_producto como diccionario clave-valor.
aspectos_destacados: 2-5 mini-frases SEO/marketing con beneficios clave (máx 10-15 palabras cada una).

Responde EXCLUSIVAMENTE con JSON válido y nada más (sin texto adicional, sin markdown):

{
  "categoria_final": "Categoría > Exacta > De > La > Taxonomía",
  "atributos_estandar": {
      "Color": null,
      "Sexo": null,
      "Grupo de edad": null,
      "Material": null,
      "Talla": null,
      "Diseño": null,
      "Longitud del producto": null,
      "Anchura del producto": null,
      "Altura del producto": null,
      "Peso del producto": null,
      "Nivel de eficiencia energética": null
  },
  "detalles_producto": {
      "Característica_Tecnica": "Valor"
  },
  "aspectos_destacados": [
      "Beneficio 1",
      "Beneficio 2"
  ],
  "texto_limpio": "Descripción breve sin precios ni marcas específicas",
  "confianza_ia": 0.92,
  "razonamiento": "Máximo 15 palabras explicando la elección"
}
"""

def extract_json_content(text):
    try:
        start = text.find('{')
        if start == -1: return None
        end = text.rfind('}', start)
        if end == -1: return None
        json_str = text[start:end+1].strip()
        if json_str.startswith("```json"): json_str = json_str[7:].strip()
        if json_str.startswith("```"): json_str = json_str[3:].strip()
        if json_str.endswith("```"): json_str = json_str[:-3:].strip()
        return json.loads(json_str)
    except:
        return None

def merge_attributes(old_attrs_str, new_attrs_dict):
    try:
        old_dict = {} if not old_attrs_str or old_attrs_str == 'null' else json.loads(old_attrs_str)
    except:
        old_dict = {}
    if not isinstance(old_dict, dict): old_dict = {}
    if not isinstance(new_attrs_dict, dict): new_attrs_dict = {}
    
    final_dict = {
        "atributos_estandar": new_attrs_dict.get("atributos_estandar", {}),
        "detalles_producto": new_attrs_dict.get("detalles_producto", {}),
        "aspectos_destacados": new_attrs_dict.get("aspectos_destacados", []),
        "raw_feed": old_dict
    }
    
    for key in ["atributos_estandar", "detalles_producto"]:
        if key in old_dict:
            final_dict[key].update(old_dict[key])
            
    if "aspectos_destacados" in old_dict:
        final_dict["aspectos_destacados"].extend(old_dict["aspectos_destacados"])
            
    return json.dumps(final_dict, ensure_ascii=False)

def find_best_match(ia_category):
    if not ia_category: return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches: return VALID_PATHS[matches[0]]
    for key in VALID_KEYS_LIST:
        if cat_norm in key: return VALID_PATHS[key]
    return None

def process_single_row(row):
    u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo, val_titulo = row
    
    clip_conf_val = float(clip_conf or 0)
    val_titulo_val = float(val_titulo or 0)
    temperature = 0.1 if val_titulo_val > 0.75 and clip_conf_val > 80 else 0.7

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"PRODUCTO: {tit}\nDESC: {desc[:400] if desc else ''}\nATTRS: {attrs}\nBERT: {cat_bert}\nCLIP: {cat_clip}"}
        ],
        "temperature": temperature,
        "max_tokens": 1300
    }

    try:
        r = session.post(API_URL, json=payload, timeout=90)
        
        if r.status_code != 200: return None

        content = r.json()['choices'][0]['message']['content']
        js = extract_json_content(content)
        if not js: return None

        cat_ia = js.get("categoria_final", "").strip()
        final_cat = find_best_match(cat_ia)

        if not final_cat and cat_ia:
            leaves = [p.strip().lower() for p in cat_ia.split('>') if p.strip()]
            for leaf in reversed(leaves):
                if leaf in LEAF_TO_PATH:
                    final_cat = LEAF_TO_PATH[leaf]
                    break
            if not final_cat:
                matches = get_close_matches(re.sub(r'\s+', ' ', cat_ia).strip().lower(), VALID_KEYS_LIST, n=1, cutoff=0.7)
                if matches: final_cat = VALID_PATHS[matches[0]]
        
        if not final_cat and val_titulo_val > 0.65 and cat_clip:
            final_cat = find_best_match(cat_clip) or VALID_PATHS.get(re.sub(r'\s+', ' ', cat_clip).strip().lower(), cat_clip)
        if not final_cat and cat_bert:
            final_cat = VALID_PATHS.get(re.sub(r'\s+', ' ', cat_bert).strip().lower(), cat_bert)
        if not final_cat:
            final_cat = "Consumibles > Otros Consumibles"

        attrs_final = merge_attributes(attrs, js)

        return (
            final_cat,
            attrs_final,
            js.get("texto_limpio", ""),
            js.get("confianza_ia", 0.0),
            js.get("razonamiento", ""),
            u_hash
        )

    except Exception:
        return None

def process_batch_parallel(rows):
    output = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_row, row): row for row in rows}
        for future in as_completed(futures):
            res = future.result()
            if res: output.append(res)
    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total_processed = 0
    start_global = time.time()

    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND (categoria_final IS NULL OR score_ia IS NULL)
          AND (curator_attempts < 2 OR curator_attempts IS NULL)
    """)
    initial_pending = cursor.fetchone()[0]
    print(f"🚀 Iniciando Curador V10 FINAL (Reading from ORIGINALS) - Workers: {MAX_WORKERS}")
    print(f"Registros pendientes: {initial_pending:,}")

    while True:
        batch_start = time.time()
        
        # === CORRECCIÓN AQUÍ: Leemos COALESCE(atributos_originales, atributos) ===
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, 
                   COALESCE(atributos_originales, atributos) as atributos, 
                   categoria, image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads
            WHERE is_valid = 1
              AND (categoria_final IS NULL OR score_ia IS NULL)
              AND (curator_attempts < 2 OR curator_attempts IS NULL)
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Todo limpio!")
            break

        results = process_batch_parallel(rows)

        ok_count = len(results)
        total_processed += ok_count
        
        batch_elapsed = time.time() - batch_start
        rate = ok_count / batch_elapsed if batch_elapsed > 0 else 0
        elapsed_global = time.time() - start_global
        avg_rate_global = total_processed / elapsed_global if elapsed_global > 0 else rate
        remaining = initial_pending - total_processed
        eta_minutes = remaining / avg_rate_global / 60 if avg_rate_global > 0 else 0

        print(f"⚡ {rate:5.1f} items/s | Total: {total_processed:,} | ETA: {eta_minutes:.0f} mins")

        if results:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?,
                    curator_attempts = COALESCE(curator_attempts, 0) + 1
                WHERE url_hash=?
            """, results)
            
            processed_hashes = set(r[5] for r in results)
            failed_hashes = [r[0] for r in rows if r[0] not in processed_hashes]
            if failed_hashes:
                 cursor.executemany("UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?", [(h,) for h in failed_hashes])

            conn.commit()

    conn.close()

if __name__ == "__main__":
    run_curator()
