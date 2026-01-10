import sqlite3
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from vllm import LLM, SamplingParams
import os

# =================================================================
# CONFIGURACIÓN
# =================================================================
DEBUG_MODE = True  # <--- MODO DEBUG ACTIVADO: Verás entrada y salida completa

# En Debug usamos batch pequeño para poder leer la consola.
# En Producción (False) usa 2048.
BATCH_SIZE = 5 if DEBUG_MODE else 2048 

MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"

# ✅ RUTA CORREGIDA A TU CSV EXISTENTE
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# PROMPT V6 (Simplificado para velocidad, lógica exacta)
# =================================================================
SYSTEM_PROMPT = """
Eres un clasificador estrictamente exacto de productos para taxonomía GPC.

REGLAS IMPRESCINDIBLES:
- Elige ÚNICAMENTE una categoría que exista EXACTAMENTE en la taxonomía GPC oficial.
- NO inventes, no modifiques, no abrevies ni traduzcas nombres de categorías.
- Usa solo rutas completas tal como aparecen en la taxonomía.

Tarea:
Con la información del producto, elige la categoría GPC más precisa.

Responde EXCLUSIVAMENTE con JSON válido y nada más:

{
  "categoria_final": "Categoría > Exacta > De > La > Taxonomía",
  "confianza_ia": 0.92,
  "razonamiento": "Máximo 10 palabras explicando la elección"
}
"""

# =================================================================
# CARGA DE TAXONOMÍA (Lógica CSV restaurada)
# =================================================================
print(f"Cargando taxonomía desde {TAXONOMY_PATH}...")
try:
    # Leemos el CSV directamente como en tu versión original
    tax_df = pd.read_csv(TAXONOMY_PATH)
    
    # Asumimos que la columna se llama 'path' (ajusta si es distinta, pero en tus scripts previos era 'path')
    if 'path' not in tax_df.columns:
        raise ValueError("El CSV no tiene la columna 'path'.")

    VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}
    LEAF_TO_PATH = {path.split('>')[-1].strip().lower(): path for path in tax_df['path'].unique()}
    VALID_KEYS_LIST = list(VALID_PATHS.keys())
    
    print(f"✅ Taxonomía cargada: {len(VALID_PATHS)} rutas válidas.")

except Exception as e:
    print(f"❌ ERROR CRÍTICO cargando CSV: {e}")
    exit()

# =================================================================
# FUNCIONES AUXILIARES
# =================================================================
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

def find_best_match(ia_category):
    if not ia_category: return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()
    
    # 1. Match Exacto
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    
    # 2. Match por Hoja
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]
    
    # 3. Match Difuso
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches: return VALID_PATHS[matches[0]]
        
    return None

def build_prompt(row):
    # Desempaquetado
    # 0:hash, 1:tit, 2:desc, 3:attrs, 4:cat_bert, 5:cat_clip, 
    # 6:conf, 7:score_cat, 8:cuerpo_Es, 9:score_prod
    
    tit = row[1]
    desc = row[2]
    attrs = row[3]
    cat_bert = row[4]
    cat_clip = row[5]
    cuerpo_largo = row[8] # cuerpo_Es

    # === LÓGICA DE CONTEXTO INTELIGENTE ===
    if (not desc or len(str(desc).strip()) < 25) and cuerpo_largo:
        info_texto = str(cuerpo_largo)[:800].replace('\n', ' ').strip()
        origen_txt = "CONTENIDO_WEB"
    else:
        info_texto = str(desc)[:500].replace('\n', ' ').strip() if desc else ""
        origen_txt = "DESCRIPCION"

    user_content = (
        f"PRODUCTO: {tit}\n"
        f"FUENTE_INFO: {origen_txt}\n"
        f"TEXTO: {info_texto}\n"
        f"ATRIBUTOS_CTX: {attrs}\n"
        f"SUGERENCIAS: [BERT: {cat_bert}] [CLIP: {cat_clip}]"
    )
    
    full_prompt = (
        f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n"
        f"{SYSTEM_PROMPT}<|eot_id|>"
        f"<|start_header_id|>user<|end_header_id|>\n\n"
        f"{user_content}<|eot_id|>"
        f"<|start_header_id|>assistant<|end_header_id|>\n\n"
    )
    return full_prompt

# =================================================================
# MOTOR PRINCIPAL vLLM
# =================================================================
def run_curator():
    print("🚀 Cargando vLLM en RTX 5090 (Modo Debug V6-TURBO CSV)...")
    
    llm = LLM(
        model=MODEL_PATH,
        gpu_memory_utilization=0.90,
        max_num_seqs=512,
        max_model_len=4096,
        tensor_parallel_size=1,
        quantization="awq",
        dtype="float16"
    )

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND (categoria_final IS NULL OR score_ia IS NULL)
          AND (curator_attempts < 2 OR curator_attempts IS NULL)
    """)
    initial_pending = cursor.fetchone()[0]
    print(f"🔥 Registros pendientes: {initial_pending:,}")
    
    if DEBUG_MODE:
        print("\n🛑 AVISO: MODO DEBUG ACTIVO.")
        print("🛑 El proceso será LENTO para permitir lectura.\n")

    total_processed = 0

    while True:
        # QUERY
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria,
                   image_suggest_category, confidence, score_category, cuerpo_Es, score_product
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

        prompts_list = []
        sampling_params_list = []

        for row in rows:
            prompts_list.append(build_prompt(row))
            
            # Temperatura Dinámica
            clip_conf = float(row[6] or 0)
            val_titulo = float(row[9] or 0)
            temp = 0.1 if val_titulo > 0.75 and clip_conf > 80 else 0.7
            
            sampling_params_list.append(SamplingParams(temperature=temp, top_p=0.9, max_tokens=300))

        # === VISUALIZACIÓN DEBUG ENTRADA ===
        if DEBUG_MODE:
            print("┌" + "─"*78 + "┐")
            print("│ 🔍 DEBUG: PROMPT DE ENTRADA (Muestra 1er item)                          │")
            print("└" + "─"*78 + "┘")
            print(prompts_list[0]) 
            print("="*80)

        # Inferencia
        outputs = llm.generate(prompts_list, sampling_params_list)
        
        updates = []
        failed_hashes = []

        for i, output in enumerate(outputs):
            row = rows[i]
            generated_text = output.outputs[0].text
            
            # === VISUALIZACIÓN DEBUG SALIDA ===
            if DEBUG_MODE:
                print(f"\n>>>> RESPUESTA RAW (Item {i}):")
                print(generated_text.strip())
                print("-" * 40)

            js = extract_json_content(generated_text)
            
            if js:
                cat_ia = js.get("categoria_final", "").strip()
                final_cat = find_best_match(cat_ia)

                # Fallback V6 (Rescate)
                if not final_cat and cat_ia:
                    leaves = [p.strip().lower() for p in cat_ia.split('>') if p.strip()]
                    for leaf in reversed(leaves):
                        if leaf in LEAF_TO_PATH:
                            final_cat = LEAF_TO_PATH[leaf]; break
                    if not final_cat:
                        matches = get_close_matches(re.sub(r'\s+', ' ', cat_ia).strip().lower(), VALID_KEYS_LIST, n=1, cutoff=0.7)
                        if matches: final_cat = VALID_PATHS[matches[0]]
                
                # Fallback CLIP/BERT
                if not final_cat and float(row[9] or 0) > 0.65 and row[5]:
                    cat_clip_clean = re.sub(r'\s+', ' ', row[5]).strip().lower()
                    final_cat = find_best_match(row[5]) or VALID_PATHS.get(cat_clip_clean, row[5])
                
                if not final_cat and row[4]:
                    cat_bert_clean = re.sub(r'\s+', ' ', row[4]).strip().lower()
                    final_cat = VALID_PATHS.get(cat_bert_clean, row[4])
                
                if not final_cat:
                    final_cat = "Consumibles > Otros Consumibles"

                if DEBUG_MODE:
                    print(f"✅ CATEGORÍA GUARDADA: {final_cat}")

                updates.append((
                    final_cat, 
                    js.get("confianza_ia", 0.0), 
                    js.get("razonamiento", ""), 
                    row[0]
                ))
            else:
                if DEBUG_MODE: print("❌ FALLO AL PARSEAR JSON")
                failed_hashes.append(row[0])

        if updates:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, score_ia=?, razonamiento_ia=?, 
                    curator_attempts = COALESCE(curator_attempts, 0) + 1 
                WHERE url_hash=?
            """, updates)
            
        if failed_hashes:
            cursor.executemany("UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?", [(h,) for h in failed_hashes])
            
        conn.commit()

        total_processed += len(updates)
        if DEBUG_MODE:
            print(f"\n🏁 Batch completado. Total procesados: {total_processed}")
            time.sleep(2)

    conn.close()

if __name__ == "__main__":
    run_curator()
