import sqlite3
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from vllm import LLM, SamplingParams

# =================================================================
# CONFIGURACIÓN (AJUSTADA PARA RTX 5090 32GB)
# =================================================================
BATCH_SIZE = 1000  # Tamaño seguro para no saturar VRAM
MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# TU PROMPT V10 (INTACTO)
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

# =================================================================
# FUNCIONES AUXILIARES
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
        if key in old_dict: final_dict[key].update(old_dict[key])
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

def build_prompt(row):
    tit, desc, attrs, cat_bert, cat_clip = row[1], row[2], row[3], row[4], row[5]
    user_content = f"PRODUCTO: {tit}\nDESC: {desc[:400] if desc else ''}\nATTRS: {attrs}\nBERT: {cat_bert}\nCLIP: {cat_clip}"
    full_prompt = (
        f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n"
        f"{SYSTEM_PROMPT}<|eot_id|>"
        f"<|start_header_id|>user<|end_header_id|>\n\n"
        f"{user_content}<|eot_id|>"
        f"<|start_header_id|>assistant<|end_header_id|>\n\n"
    )
    return full_prompt

# =================================================================
# MOTOR PRINCIPAL (CON TEMPERATURA DINÁMICA DE V6)
# =================================================================
def run_curator():
    print("🚀 Cargando vLLM en RTX 5090 (Modo Inteligente v6 + v10)...")
    
    # CONFIGURACIÓN SEGURA PARA 5090
    llm = LLM(
        model=MODEL_PATH,
        gpu_memory_utilization=0.90,  # Dejamos 10% libre para evitar OOM
        max_num_seqs=256,             # Limitamos concurrencia para estabilidad
        max_model_len=4096,
        tensor_parallel_size=1,
        quantization="awq",
        dtype="float16"
    )

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE is_valid = 1 AND (categoria_final IS NULL OR score_ia IS NULL) AND (curator_attempts < 2 OR curator_attempts IS NULL)")
    initial_pending = cursor.fetchone()[0]
    print(f"🔥 Registros pendientes: {initial_pending:,}")

    total_processed = 0
    start_global = time.time()

    while True:
        # CONSULTA IDÉNTICA A TU V10/V6
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

        # === AQUÍ ESTÁ LA MAGIA DE V6 EN VLLM ===
        # Preparamos lista de prompts Y lista de SamplingParams personalizados por fila
        prompts_list = []
        sampling_params_list = []

        for row in rows:
            # 1. Construir Prompt
            prompts_list.append(build_prompt(row))
            
            # 2. Calcular Temperatura EXACTA DE V6
            clip_conf = float(row[6] or 0)     # confidence
            val_titulo = float(row[9] or 0)    # score_product
            
            # Lógica v6: Si es muy seguro -> 0.1, si duda -> 0.7
            temp = 0.1 if val_titulo > 0.75 and clip_conf > 80 else 0.7
            
            sampling_params_list.append(SamplingParams(temperature=temp, top_p=0.9, max_tokens=1300))

        print(f" >> Generando {len(prompts_list)} items (Temps dinámicas: 0.1/0.7)...")
        
        # Pasamos las dos listas a vLLM
        outputs = llm.generate(prompts_list, sampling_params_list)
        
        updates = []
        failed_hashes = []

        for i, output in enumerate(outputs):
            row = rows[i]
            # Fallbacks
            cat_bert = row[4]
            cat_clip = row[5]
            val_titulo = float(row[9] or 0)
            
            generated_text = output.outputs[0].text
            js = extract_json_content(generated_text)
            
            if js:
                cat_ia = js.get("categoria_final", "").strip()
                final_cat = find_best_match(cat_ia)

                if not final_cat and cat_ia:
                    leaves = [p.strip().lower() for p in cat_ia.split('>') if p.strip()]
                    for leaf in reversed(leaves):
                        if leaf in LEAF_TO_PATH:
                            final_cat = LEAF_TO_PATH[leaf]; break
                    if not final_cat:
                        matches = get_close_matches(re.sub(r'\s+', ' ', cat_ia).strip().lower(), VALID_KEYS_LIST, n=1, cutoff=0.7)
                        if matches: final_cat = VALID_PATHS[matches[0]]
                
                if not final_cat and val_titulo > 0.65 and cat_clip:
                    final_cat = find_best_match(cat_clip) or VALID_PATHS.get(re.sub(r'\s+', ' ', cat_clip).strip().lower(), cat_clip)
                if not final_cat and cat_bert:
                    final_cat = VALID_PATHS.get(re.sub(r'\s+', ' ', cat_bert).strip().lower(), cat_bert)
                if not final_cat:
                    final_cat = "Consumibles > Otros Consumibles"

                updates.append((final_cat, merge_attributes(row[3], js), js.get("texto_limpio", ""), js.get("confianza_ia", 0.0), js.get("razonamiento", ""), row[0]))
            else:
                failed_hashes.append(row[0])

        if updates:
            cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?, curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash=?", updates)
        if failed_hashes:
            cursor.executemany("UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?", [(h,) for h in failed_hashes])
            
        conn.commit()

        count = len(updates)
        total_processed += count
        elapsed_global = time.time() - start_global
        avg_rate = total_processed / elapsed_global if elapsed_global > 0 else 0
        eta = (initial_pending - total_processed) / avg_rate / 60 if avg_rate > 0 else 0
        print(f"⚡ Global: {avg_rate:.1f} items/s | ETA: {eta:.0f} mins | Procesados: {total_processed}")
    conn.close()

if __name__ == "__main__":
    run_curator()
