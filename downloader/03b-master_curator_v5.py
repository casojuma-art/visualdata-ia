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
BATCH_SIZE = 512
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
# PROMPT
# =================================================================
SYSTEM_PROMPT = """
Eres un asistente JSON estricto, algorítmico y probabilístico para clasificación GPC con miles de categorías.
TU OBJETIVO: Decidir la categoría oficial usando lógica semántica y thresholds dinámicos, sin reglas manuales específicas.

REGLAS ALGORÍTMICAS DE DECISIÓN (sigue este flujo exacto para cada producto):
1. Calcula similitud semántica (en escala 0-1) entre el TÍTULO y cada sugerencia:
   - Similitud alta >0.8: Prioriza esa fuente.
   - Similitud media 0.5-0.8: Combina fuentes con peso proporcional (ej. 70% título, 30% sugerencia).
   - Similitud baja <0.5: Ignora completamente esa sugerencia y genera basada solo en título/atributos.
2. Peso de fuentes: Título (peso 0.6), BERT (0.2), CLIP (0.2 si conf >0.7, sino 0).
3. Si conflicto (similitudes divergen >0.3): Genera categoría nueva usando embedding del título vs taxonomía GPC completa (elige la más cercana semánticamente).
4. Baja confianza_ia proporcionalmente a la divergencia semántica (ej. conf = 1 - max_divergencia).
5. Para texto_limpio: Siempre neutraliza eliminando precios/publicidad con regex implícito (no menciones marcas/proveedores).
6. Atributos: Fusiona algorítmicamente, priorizando originales y añadiendo nuevos si similitud >0.7.

IMPORTANTE: Todo debe ser automático y escalable — no uses juicios manuales ni listas fijas. Usa razonamiento probabilístico para 4000+ categorías.

FORMATO DE RESPUESTA:
{
  "categoria_final": "Ruta > Oficial > Completa",
  "json_atributos": {"clave": "valor"},
  "texto_limpio": "Descripción breve",
  "confianza_ia": 0.95,
  "razonamiento": "Breve explicación algorítmica (similitudes calculadas, pesos aplicados)."
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
        json_str = text[start:end+1]
        
        json_str = json_str.strip()
        if json_str.startswith("```json"):
            json_str = json_str[7:].strip()
        if json_str.startswith("```"):
            json_str = json_str[3:].strip()
        if json_str.endswith("```"):
            json_str = json_str[:-3].strip()
        
        return json.loads(json_str)
    except:
        return None

def merge_attributes(old_attrs_str, new_attrs_dict):
    try:
        if not old_attrs_str or old_attrs_str == 'null':
            old_dict = {}
        else:
            try:
                old_dict = json.loads(old_attrs_str)
            except:
                old_dict = {}
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
    
    short_tit = tit[:20] + "..." if tit and len(tit) > 20 else str(tit)

    clip_conf_val = float(clip_conf or 0)
    val_titulo_val = float(val_titulo or 0)

    user_content = f"""PRODUCTO: {tit}
DESC: {desc[:400]}...
ATTRS: {attrs}
IA INPUTS:
- BERT: {cat_bert}
- CLIP: {cat_clip} (Confianza: {clip_conf_val:.0f}%, Validez vs Título: {val_titulo_val:.4f})
"""
    # Temperatura adaptativa más agresiva en casos difíciles
    if val_titulo_val > 0.75 and clip_conf_val > 80:
        temperature = 0.2
    else:
        temperature = 0.7  # Más variedad para romper JSON rotos repetidos

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ],
        "temperature": temperature,
        "max_tokens": 1224
    }

    try:
        r = requests.post(API_URL, json=payload, timeout=90)
        if r.status_code != 200:
            print(f"⚠️ API Error {r.status_code} | {u_hash[:8]} | {short_tit}")
            return None
        
        content = r.json()['choices'][0]['message']['content']
        js = extract_json_content(content)
        
        if not js:
            print(f"❌ JSON FAIL {u_hash[:8]} | {short_tit}")
            return None

        cat_ia = js.get("categoria_final", "").strip()
        final_cat = find_best_match(cat_ia)
        
        if not final_cat and cat_ia:
            cat_norm = re.sub(r'\s+', ' ', cat_ia).strip().lower()
            leaves = [p.strip().lower() for p in cat_ia.split('>') if p.strip()]
            for leaf in reversed(leaves):
                if leaf in LEAF_TO_PATH:
                    final_cat = LEAF_TO_PATH[leaf]
                    break
            if not final_cat:
                matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.7)
                if matches:
                    final_cat = VALID_PATHS[matches[0]]

        if not final_cat:
            if val_titulo_val > 0.65 and cat_clip:
                final_cat = find_best_match(cat_clip)
                if not final_cat and cat_clip:
                    clip_norm = re.sub(r'\s+', ' ', cat_clip).strip().lower()
                    final_cat = VALID_PATHS.get(clip_norm, cat_clip)
        
        if not final_cat and cat_bert:
            bert_norm = re.sub(r'\s+', ' ', cat_bert).strip().lower()
            final_cat = VALID_PATHS.get(bert_norm, cat_bert)

        if not final_cat:
            final_cat = "Consumibles > Otros Consumibles"

        new_attrs_dict = js.get("json_atributos", {})
        final_attrs_json = merge_attributes(attrs, new_attrs_dict)

        def lca_depth(cat1, cat2):
            if not cat1 or not cat2:
                return 0
            parts1 = [p.strip().lower() for p in cat1.split('>')]
            parts2 = [p.strip().lower() for p in cat2.split('>')]
            depth = 0
            for p1, p2 in zip(parts1, parts2):
                if p1 == p2:
                    depth += 1
                else:
                    break
            return depth

        depth_bert = lca_depth(final_cat, cat_bert) if cat_bert else 0
        depth_clip = lca_depth(final_cat, cat_clip) if cat_clip else 0

        depths = [len([p for p in c.split('>') if p.strip()]) for c in [cat_bert, cat_clip, final_cat] if c]
        min_acceptable = 1

        if (depth_bert + depth_clip) < min_acceptable:
            print(f"🛡️ LCA REJECT {u_hash[:8]} | {short_tit}")
            return None

        return (
            final_cat,
            final_attrs_json,
            js.get("texto_limpio", ""),
            js.get("confianza_ia", 0.0),
            js.get("razonamiento", ""),
            u_hash
        )

    except Exception as e:
        print(f"❌ EXCEPTION {u_hash[:8]} | {short_tit} | {str(e)[:100]}")
        return None

def process_batch_parallel(rows):
    output = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_row = {executor.submit(process_single_row, row): row for row in rows}
        for future in as_completed(future_to_row):
            res = future.result()
            if res:
                output.append(res)

    # Feedback
    if output:
        import random
        sample = random.sample(output, max(5, len(output)//10))
        valid_samples = [s for s in sample if s is not None]
        rejection_rate = 1 - (len(valid_samples) / len(sample)) if sample else 0
        print(f"Feedback batch: {len(output)} procesados | "
              f"{len(valid_samples)} aceptados en muestra | "
              f"{rejection_rate:.1%} rechazados por incoherencia LCA")

    # === NUEVO: Incrementar attempts en fallidos ===
    if len(output) < len(rows):
        failed_hashes = [row[0] for row in rows if row[0] not in [res[5] for res in output]]
        if failed_hashes:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.executemany(
                "UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?",
                [(h,) for h in failed_hashes]
            )
            conn.commit()
            conn.close()
    # === FIN NUEVO ===

    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total = 0
    start_global = time.time()

    print(f"🚀 Iniciando Curador TURBO + ATRIBUTOS + LCA FILTER (Workers: {MAX_WORKERS})...")

    while True:
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
            print("¡Todo limpio! No hay registros pendientes.")
            break

        batch_start = time.time()
        results = process_batch_parallel(rows)
 
        if results:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?,
                    curator_attempts = curator_attempts + 1
                WHERE url_hash=?
            """, results)
            
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

        total += len(results)
        elapsed = time.time() - batch_start
        rate = len(results) / elapsed if elapsed > 0 else 0
        
        print(f"Batch: {len(results)}/{len(rows)} OK | ❌ Fallidos: {len(rows) - len(results)} | ⚡ {rate:.1f} items/seg | Total: {total:,}")

    print(f"FIN. Tiempo total: {(time.time() - start_global)/60:.1f} mins")
    conn.close()

if __name__ == "__main__":
    run_curator()
