import sqlite3
import requests
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================
# CONFIGURACIÓN DE RENDIMIENTO
# =================================================================
BATCH_SIZE = 512          # Leemos más filas de golpe de la DB
MAX_WORKERS = 20          # Peticiones simultáneas a la API (Ajusta según tu VRAM)
API_URL = "http://localhost:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# CARGA DE TAXONOMÍA (Optimizado)
# =================================================================
print("Cargando taxonomía...")
tax_df = pd.read_csv(TAXONOMY_PATH)
# Mapa: texto normalizado -> Ruta Oficial
VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}
# Mapa: Hoja -> Ruta Oficial (para búsquedas rápidas)
LEAF_TO_PATH = {path.split('>')[-1].strip().lower(): path for path in tax_df['path'].unique()}
# Lista de claves para fuzzy matching (solo keys para ahorrar memoria en diff)
VALID_KEYS_LIST = list(VALID_PATHS.keys())

# =================================================================
# PROMPT
# =================================================================
SYSTEM_PROMPT = """
Eres un asistente JSON estricto para clasificación GPC.
TU OBJETIVO: Decidir la categoría oficial basándote en Título, Visión (CLIP) y Texto (BERT).

REGLAS DE DECISIÓN LÓGICA:
1. Si 'Validación CLIP' > 0.6 y tiene sentido con el Título -> USA CLIP.
2. Si CLIP falla, evalúa BERT. Si BERT tiene sentido semántico -> USA BERT.
3. Si AMBOS fallan (son incoherentes con el Título) -> GENERA TÚ la ruta oficial correcta basándote en el Título.

IMPORTANTE:
- NO expliques nada fuera del JSON.
- NO uses Markdown (```json).
- Devuelve SOLO el objeto JSON crudo.

FORMATO DE RESPUESTA:
{
  "categoria_final": "Ruta > Oficial > Completa",
  "json_atributos": {"clave": "valor"},
  "texto_limpio": "Descripción breve",
  "confianza_ia": 0.95,
  "razonamiento": "Breve explicación de por qué elegiste esta fuente."
}
"""



def find_best_match(ia_category):
    if not ia_category: return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()

    # 1. Exacto
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    
    # 2. Por hoja ("Grifos")
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]

    # 3. Contenido (Si "A > B" está en "A > B > C")
    #    (Optimizamos esto: buscar substring es rápido, fuzzy es lento)
    #    Solo hacemos fuzzy si falla todo lo demás para no frenar el script.
    
    # 4. Fuzzy Match (Lento, usar con precaución)
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches: return VALID_PATHS[matches[0]]
    
    return None

def process_single_row(row):
    """Procesa UNA sola fila. Esta función correrá en paralelo."""
    u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo, val_titulo = row
    
    clip_conf_val = float(clip_conf or 0)
    val_titulo_val = float(val_titulo or 0) # CLIP_Valida_Titulo

    # Lógica de Prompt optimizada
    user_content = f"""PRODUCTO: {tit}
DESC: {desc[:400]}...
ATTRS: {attrs}
IA INPUTS:
- BERT: {cat_bert}
- CLIP: {cat_clip} (Confianza: {clip_conf_val:.0f}%, Validez vs Título: {val_titulo_val:.4f})
"""
    
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "max_tokens": 600
    }

    try:
        # Petición síncrona (pero se ejecutará en su propio hilo)
        r = requests.post(API_URL, json=payload, timeout=40)
        if r.status_code != 200:
            return None
        
        js = json.loads(re.sub(r'```json\s*|```', '', r.json()['choices'][0]['message']['content']).strip())
        cat_ia = js.get("categoria_final", "")
        
        # --- LÓGICA DE DECISIÓN (Validación) ---
        final_cat = find_best_match(cat_ia)
        
        # Fallbacks usando estadísticas
        if not final_cat:
            # Si IA falla, miramos si CLIP era muy bueno
            if val_titulo_val > 0.65 and cat_clip:
                final_cat = find_best_match(cat_clip)
            
            # Si no, BERT
            if not final_cat:
                final_cat = VALID_PATHS.get(re.sub(r'\s+', ' ', cat_bert or "").lower(), cat_bert)

        return (
            final_cat,
            json.dumps(js.get("json_atributos", {}), ensure_ascii=False),
            js.get("texto_limpio", ""),
            js.get("confianza_ia", 0.0),
            js.get("razonamiento", ""),
            u_hash
        )

    except Exception as e:
        # print(f"Error en {u_hash}: {e}")
        return None

def process_batch_parallel(rows):
    output = []
    # ThreadPoolExecutor lanza múltiples hilos a la vez
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Enviamos todas las tareas
        future_to_row = {executor.submit(process_single_row, row): row for row in rows}
        
        # Recogemos resultados a medida que llegan
        for future in as_completed(future_to_row):
            res = future.result()
            if res:
                output.append(res)
    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total = 0
    start_global = time.time()

    print(f"🚀 Iniciando Curador TURBO (Workers: {MAX_WORKERS})...")

    while True:
        # NOTA: Usamos score_ia IS NULL para no repetir bucles infinitos
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria, 
                   image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads 
            WHERE is_valid = 1 
              AND (categoria_final IS NULL OR score_ia IS NULL) 
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Todo limpio! No hay registros pendientes.")
            break

        batch_start = time.time()
        
        # Procesamiento Paralelo
        results = process_batch_parallel(rows)
        
        # Guardado en bloque (SQLite es rápido escribiendo en batches)
        if results:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?
                WHERE url_hash=?
            """, results)
            conn.commit()

        total += len(results)
        elapsed = time.time() - batch_start
        rate = len(results) / elapsed if elapsed > 0 else 0
        
        print(f"Batch: {len(results)}/{len(rows)} OK | ⚡ {rate:.1f} items/seg | Total: {total:,}")

    print(f"FIN. Tiempo total: {(time.time() - start_global)/60:.1f} mins")
    conn.close()

if __name__ == "__main__":
    run_curator()
