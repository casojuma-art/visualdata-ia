import sqlite3
import requests
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================
# CONFIGURACIÓN
# =================================================================
#DEBUG_MODE = True          # <<< CAMBIA A False CUANDO ESTÉ SATISFECHO
DEBUG_MODE = False          # <<< CAMBIA A False CUANDO ESTÉ SATISFECHO

BATCH_SIZE = 64 if DEBUG_MODE else 512
MAX_WORKERS = 32
API_URL = "http://192.168.1.211:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# CARGA DE TAXONOMÍA
# =================================================================
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
# PROMPT NUEVO: CORTO, CLARO Y BLINDADO (no inventa categorías)
# =================================================================
SYSTEM_PROMPT = """
Eres un clasificador estrictamente exacto de productos para taxonomía GPC.

REGLAS IMPRESCINDIBLES:
- Elige ÚNICAMENTE una categoría que exista EXACTAMENTE en la taxonomía GPC oficial.
- NO inventes, no modifiques, no abbreviates ni traduzcas nombres de categorías.
- Usa solo rutas completas tal como aparecen en la taxonomía.

Tarea:
Con el título, descripción, atributos, sugerencia BERT y CLIP, elige la categoría GPC más precisa y específica posible.

Responde EXCLUSIVAMENTE con JSON válido y nada más (sin texto adicional, sin markdown):

{
  "categoria_final": "Categoría > Exacta > De > La > Taxonomía",
  "json_atributos": {"clave": "valor"},
  "texto_limpio": "Descripción breve sin precios ni marcas específicas",
  "confianza_ia": 0.92,
  "razonamiento": "Máximo 15 palabras explicando la elección"
}
"""

# =================================================================
# FUNCIONES AUXILIARES
# =================================================================
def extract_json_content(text):
    try:
        start = text.find('{')
        if start == -1:
            return None
        end = text.rfind('}', start)
        if end == -1:
            return None
        json_str = text[start:end+1].strip()
        if json_str.startswith("```json"):
            json_str = json_str[7:].strip()
        if json_str.startswith("```"):
            json_str = json_str[3:].strip()
        if json_str.endswith("```"):
            json_str = json_str[:-3:].strip()
        return json.loads(json_str)
    except Exception as e:
        return None

def merge_attributes(old_attrs_str, new_attrs_dict):
    # (igual que antes)
    try:
        old_dict = {} if not old_attrs_str or old_attrs_str == 'null' else json.loads(old_attrs_str)
    except:
        old_dict = {}
    if not isinstance(old_dict, dict):
        old_dict = {}
    if not isinstance(new_attrs_dict, dict):
        new_attrs_dict = {}
    final_dict = new_attrs_dict.copy()
    final_dict.update(old_dict)
    return json.dumps(final_dict, ensure_ascii=False)

def find_best_match(ia_category):
    if not ia_category:
        return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()
    if cat_norm in VALID_PATHS:
        return VALID_PATHS[cat_norm]
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH:
        return LEAF_TO_PATH[leaf]
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches:
        return VALID_PATHS[matches[0]]
    for key in VALID_KEYS_LIST:
        if cat_norm in key:
            return VALID_PATHS[key]
    return None

def process_single_row(row):
    u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo, val_titulo = row
    short_tit = tit[:30] + "..." if tit and len(tit) > 30 else tit

    clip_conf_val = float(clip_conf or 0)
    val_titulo_val = float(val_titulo or 0)

    # === ENTRADA COMPLETA (solo en debug) ===
    if DEBUG_MODE:
        print("\n" + "="*80)
        print(f"HASH: {u_hash}")
        print(f"TÍTULO: {tit}")
        print(f"DESCRIPCIÓN (primeros 300): {desc[:300] if desc else 'None'}")
        print(f"ATRIBUTOS: {attrs}")
        print(f"BERT: {cat_bert}")
        print(f"CLIP: {cat_clip} (conf: {clip_conf_val:.0f}%, val_titulo: {val_titulo_val:.3f})")
        print("-"*80)

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
        r = requests.post(API_URL, json=payload, timeout=90)
        if r.status_code != 200:
            if DEBUG_MODE:
                print(f"⚠️ API ERROR {r.status_code}")
            return None

        content = r.json()['choices'][0]['message']['content']

        if DEBUG_MODE:
            print(f"RESPUESTA COMPLETA DEL MODELO:\n{content.strip()}\n")

        js = extract_json_content(content)
        if not js:
            if DEBUG_MODE:
                print("❌ JSON PARSE FALLÓ")
            return None

        if DEBUG_MODE:
            print(f"JSON PARSEADO CORRECTAMENTE:")
            print(json.dumps(js, indent=2, ensure_ascii=False))

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
                if matches:
                    final_cat = VALID_PATHS[matches[0]]

        if not final_cat and val_titulo_val > 0.65 and cat_clip:
            final_cat = find_best_match(cat_clip) or VALID_PATHS.get(re.sub(r'\s+', ' ', cat_clip).strip().lower(), cat_clip)

        if not final_cat and cat_bert:
            final_cat = VALID_PATHS.get(re.sub(r'\s+', ' ', cat_bert).strip().lower(), cat_bert)

        if not final_cat:
            final_cat = "Consumibles > Otros Consumibles"

        if DEBUG_MODE:
            print(f"✅ CATEGORÍA FINAL ELEGIDA: {final_cat}")
            print(f"Confianza IA: {js.get('confianza_ia', '?')}")
            print(f"Razonamiento: {js.get('razonamiento', '?')}")
            print(f"Texto limpio: {js.get('texto_limpio', '?')[:100]}...")
            print("="*80 + "\n")

        return (
            final_cat,
            merge_attributes(attrs, js.get("json_atributos", {})),
            js.get("texto_limpio", ""),
            js.get("confianza_ia", 0.0),
            js.get("razonamiento", ""),
            u_hash
        )

    except Exception as e:
        if DEBUG_MODE:
            print(f"❌ EXCEPCIÓN: {str(e)}")
        return None

def process_batch_parallel(rows):
    output = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_row, row): row for row in rows}
        for future in as_completed(futures):
            res = future.result()
            if res:
                output.append(res)
    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total_processed = 0
    failed_this_run = 0
    start_global = time.time()

    # Contar pendientes al inicio para previsión
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND (categoria_final IS NULL OR score_ia IS NULL)
          AND (curator_attempts < 2 OR curator_attempts IS NULL)
    """)
    initial_pending = cursor.fetchone()[0]
    print(f"🚀 Iniciando Curador en MODO PRODUCCIÓN")
    print(f"Registros pendientes al inicio: {initial_pending:,}")

    batch_num = 0

    while True:
        batch_start = time.time()
        
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria,
                   image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads
            WHERE is_valid = 1
              AND (categoria_final IS NULL OR score_ia IS NULL)
              AND (curator_attempts < 2 OR curator_attempts IS NULL)
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Todo limpio! No hay más registros pendientes.")
            break

        batch_num += 1
        results = process_batch_parallel(rows)

        ok_count = len(results)
        failed_count = len(rows) - ok_count
        total_processed += ok_count
        failed_this_run += failed_count

        batch_elapsed = time.time() - batch_start
        rate = ok_count / batch_elapsed if batch_elapsed > 0 else 0

        # Estimación de tiempo restante
        elapsed_global = time.time() - start_global
        avg_rate_global = total_processed / elapsed_global if elapsed_global > 0 else rate
        remaining = initial_pending - total_processed
        eta_minutes = remaining / avg_rate_global / 60 if avg_rate_global > 0 else 0

        print(f"Batch {batch_num:3d} | {ok_count:3d}/{len(rows):3d} OK "
              f"| ❌ {failed_count:3d} fallidos "
              f"| ⚡ {rate:5.1f} items/seg "
              f"| Total: {total_processed:7,} "
              f"| ETA: {eta_minutes:5.1f} mins ({remaining:,} restantes)")

        if results:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?,
                    curator_attempts = COALESCE(curator_attempts, 0) + 1
                WHERE url_hash=?
            """, results)

            # Incrementar attempts en fallidos
            failed_hashes = [row[0] for row in rows if row[0] not in [r[5] for r in results]]
            if failed_hashes:
                cursor.executemany(
                    "UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?",
                    [(h,) for h in failed_hashes]
                )

            # Marcar fallidos definitivos
            cursor.execute("""
                UPDATE downloads 
                SET categoria_final = 'CURATION_FAILED_AFTER_2_ATTEMPTS',
                    score_ia = 0.0,
                    cuerpo_limpio = 'Falló curación tras 2 intentos'
                WHERE is_valid = 1 
                  AND COALESCE(curator_attempts, 0) >= 2 
                  AND (categoria_final IS NULL OR categoria_final = '')
            """)
            conn.commit()

    total_time_mins = (time.time() - start_global) / 60
    print(f"\n=== FIN DEL CURADOR ===")
    print(f"Tiempo total: {total_time_mins:.1f} minutos")
    print(f"Procesados con éxito: {total_processed:,}")
    print(f"Fallidos en esta ejecución: {failed_this_run:,}")
    print(f"¡Listo! Revisa los pocos fallidos si los hay.")

    conn.close()

if __name__ == "__main__":
    run_curator()
