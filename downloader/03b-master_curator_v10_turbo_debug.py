import sqlite3
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from vllm import LLM, SamplingParams

# =================================================================
# CONFIGURACIÓN DEBUG
# =================================================================
BATCH_SIZE = 10  # Muy bajo para poder leer la pantalla
MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# SYSTEM PROMPT V6 (EL QUE FUNCIONABA BIEN)
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
print("Cargando taxonomía...")
try:
    tax_df = pd.read_csv(TAXONOMY_PATH)
    VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}
    LEAF_TO_PATH = {path.split('>')[-1].strip().lower(): path for path in tax_df['path'].unique()}
    VALID_KEYS_LIST = list(VALID_PATHS.keys())
except Exception as e:
    print(f"❌ ERROR CRÍTICO cargando CSV: {e}")
    exit()

def extract_json_content(text):
    try:
        start = text.find('{')
        if start == -1: return None
        end = text.rfind('}', start)
        if end == -1: return None
        json_str = text[start:end+1].strip()
        # Limpieza agresiva de markdown
        if json_str.startswith("```json"): json_str = json_str[7:].strip()
        if json_str.startswith("```"): json_str = json_str[3:].strip()
        if json_str.endswith("```"): json_str = json_str[:-3:].strip()
        return json.loads(json_str)
    except:
        return None

def merge_attributes(old_attrs_str, new_attrs_dict):
    try: old_dict = {} if not old_attrs_str or old_attrs_str == 'null' else json.loads(old_attrs_str)
    except: old_dict = {}
    if not isinstance(old_dict, dict): old_dict = {}
    if not isinstance(new_attrs_dict, dict): new_attrs_dict = {}
    
    # Lógica V6 simple de merge
    final_dict = new_attrs_dict.copy()
    final_dict.update(old_dict)
    return json.dumps(final_dict, ensure_ascii=False)

def find_best_match(ia_category):
    if not ia_category: return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches: return VALID_PATHS[matches[0]]
    return None

def build_prompt(row):
    tit, desc, attrs, cat_bert, cat_clip = row[1], row[2], row[3], row[4], row[5]
    user_content = f"PRODUCTO: {tit}\nDESC: {desc[:400] if desc else ''}\nATTRS: {attrs}\nBERT: {cat_bert}\nCLIP: {cat_clip}"
    
    # FORMATO LLAMA 3 SIN <|begin_of_text|> PARA EVITAR DOBLE INICIO
    # vLLM a veces inyecta el BOS automáticamente. Probamos estructura limpia.
    full_prompt = (
        f"<|start_header_id|>system<|end_header_id|>\n\n"
        f"{SYSTEM_PROMPT}<|eot_id|>"
        f"<|start_header_id|>user<|end_header_id|>\n\n"
        f"{user_content}<|eot_id|>"
        f"<|start_header_id|>assistant<|end_header_id|>\n\n"
    )
    return full_prompt

# =================================================================
# MOTOR DEBUG
# =================================================================
def run_curator():
    print("🚀 Cargando vLLM en MODO DEBUG (v10 Turbo + Cerebro v6)...")
    
    llm = LLM(
        model=MODEL_PATH,
        gpu_memory_utilization=0.90,
        max_num_seqs=256,
        max_model_len=4096,
        tensor_parallel_size=1,
        quantization="awq",
        dtype="float16"
    )

    # Parámetros muy conservadores para testear
    # Stop token id 128009 es <|eot_id|> en Llama 3
    sampling_params = SamplingParams(temperature=0.1, top_p=0.9, max_tokens=1024, stop_token_ids=[128001, 128009])

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🔍 Buscando registros pendientes...")

    while True:
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, 
                   COALESCE(atributos_originales, atributos) as atributos, 
                   categoria, image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads
            WHERE is_valid = 1
              AND (categoria_final IS NULL OR score_ia IS NULL)
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Todo limpio!")
            break

        prompts = [build_prompt(row) for row in rows]
        print(f"\n >> Generando {len(prompts)} items...")
        
        outputs = llm.generate(prompts, sampling_params)
        
        updates = []
        
        # === BUCLE DE VISUALIZACIÓN DEBUG ===
        for i, output in enumerate(outputs):
            row = rows[i]
            generated_text = output.outputs[0].text
            
            print(f"\n🔵 [ITEM {i+1}] Título: {row[1][:50]}...")
            print(f"🔸 RAW OUTPUT VLLM:\n{generated_text}\n{'='*40}")
            
            js = extract_json_content(generated_text)
            
            if js:
                print("✅ JSON PARSEADO OK")
                print(f"   Categoría IA: {js.get('categoria_final')}")
                print(f"   Atributos IA: {js.get('json_atributos')}")
                
                # Logica de guardado (simplificada para debug)
                cat_ia = js.get("categoria_final", "").strip()
                final_cat = find_best_match(cat_ia)
                if not final_cat: final_cat = "Consumibles > Otros Consumibles" # Fallback básico debug
                
                updates.append((final_cat, merge_attributes(row[3], js.get("json_atributos")), js.get("texto_limpio", ""), js.get("confianza_ia", 0.0), js.get("razonamiento", ""), row[0]))
            else:
                print("❌ ERROR: NO SE ENCONTRÓ JSON VÁLIDO")
                # No guardamos nada en debug para poder reintentar corregir el código

        # Solo actualizamos si hay éxito
        if updates:
            print(f"\n💾 Guardando {len(updates)} registros en BBDD...")
            cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?, curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash=?", updates)
            conn.commit()
            
        # Pausa para que te de tiempo a leer
        print("\n⏸️  Pausa de 5 segundos para leer pantalla...")
        time.sleep(5)

    conn.close()

if __name__ == "__main__":
    run_curator()
