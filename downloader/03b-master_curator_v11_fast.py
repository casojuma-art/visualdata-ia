import sqlite3
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from vllm import LLM, SamplingParams

# =================================================================
# CONFIGURACIÓN V11 (RTX 5090 - ÁRBITRO + REDACTOR)
# =================================================================
#DEBUG_MODE = True  
DEBUG_MODE = False      # Desactiva los prints para no frenar la CPU
BATCH_SIZE = 400 if DEBUG_MODE else 1200 
MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

SYSTEM_PROMPT = """
Eres un experto clasificador taxonómico de productos para taxonomía GPC en Español de España y redactor de contenido para e-commerce.

ROL DE ÁRBITRO:
Recibirás información del producto y sugerencias de otros modelos (BERT y CLIP).
Tu tarea principal es evaluar la coherencia de todos los datos y decidir la categoría GPC (Google Product Taxonomy) ÚNICA y EXACTA.
- Si BERT y CLIP coinciden y tiene sentido, úsala.
- Si discrepan, usa tu criterio analizando Título y Descripción para desempatar.
- JAMÁS inventes categorías. Usa solo las rutas oficiales completas.

ROL DE REDACTOR (texto_limpio):
Genera una ficha comercial "texto_limpio" de alta calidad (máx 300 palabras).
- FUSIONA la información del Título, Descripción y el Cuerpo del texto.
- Crea un texto fluido, vendedor y sin duplicados.
- NO incluyas precios, marcas, enlaces ni códigos internos.

Responde EXCLUSIVAMENTE con este JSON:
{
  "categoria_final": "Ruta > Completa > De > La > Taxonomía",
  "texto_limpio": "Texto comercial fusionado y limpio...",
  "confianza_ia": 0.95,
  "razonamiento": "Explica por qué elegiste esta categoría frente a las sugerencias BERT/CLIP"
}
"""

# --- CARGA DE TAXONOMÍA ---
print("Cargando taxonomía...")
tax_df = pd.read_csv(TAXONOMY_PATH)
VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}
LEAF_TO_PATH = {path.split('>')[-1].strip().lower(): path for path in tax_df['path'].unique()}
VALID_KEYS_LIST = list(VALID_PATHS.keys())

def extract_json_content(text):
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start == -1 or end == -1: return None
        return json.loads(text[start:end+1].strip())
    except: return None

def find_best_match(ia_category):
    if not ia_category: return None
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    return VALID_PATHS[matches[0]] if matches else None

def build_prompt(row):
    # url_hash, titulo, descripcion, atributos, categoria (BERT), image_suggest_category (CLIP), cuerpo_limpio
    u_hash, tit, desc, attrs, cat_bert, cat_clip, cuerpo = row[0], row[1], row[2], row[3], row[4], row[5], row[8]
    user_content = f"PRODUCTO: {tit}\nDESC: {desc[:400] if desc else ''}\nCUERPO_PREVIO: {cuerpo[:400] if cuerpo else ''}\nBERT: {cat_bert}\nCLIP: {cat_clip}"
    return (f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n{SYSTEM_PROMPT}<|eot_id|>"
            f"<|start_header_id|>user<|end_header_id|>\n\n{user_content}<|eot_id|>"
            f"<|start_header_id|>assistant<|end_header_id|>\n\n")

def run_curator():
    llm = LLM(model=MODEL_PATH, gpu_memory_utilization=0.90, quantization="awq_marlin", dtype="float16",enforce_eager=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    while True:
        # Seleccionamos también cuerpo_limpio para pasárselo a la IA
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria, image_suggest_category, confidence, score_product, cuerpo_limpio
            FROM downloads WHERE is_valid = 1 AND categoria_final IS NULL LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows: break

        prompts, params = [], []
        for row in rows:
            prompts.append(build_prompt(row))
            # Temp dinámica: 0.1 si CLIP y Título están seguros, si no 0.7 para que la IA "piense"
            temp = 0.1 if (float(row[7] or 0) > 0.80 and float(row[6] or 0) > 85) else 0.7
            params.append(SamplingParams(temperature=temp, max_tokens=1024))

        outputs = llm.generate(prompts, params)
        updates = []

        for i, output in enumerate(outputs):
            res_text = output.outputs[0].text
            js = extract_json_content(output.outputs[0].text)
            row = rows[i]

            # --- CAMBIO AQUÍ PARA VER MÁS ---
            if DEBUG_MODE and i < 5: 
                print(f"\n🚀 [DEBUG] HASH: {row[0][:10]}", flush=True)
                print(f"📄 TEXTO GENERADO:\n{res_text}\n", flush=True)
                if js:
                    print(f"✅ JSON PARSEADO: {js.get('categoria_final')}", flush=True)
                else:
                    print("❌ ERROR: No se pudo parsear JSON", flush=True)            
            if js:
                final_cat = find_best_match(js.get("categoria_final"))
                # Fallback jerárquico si la IA falla
                if not final_cat: final_cat = find_best_match(row[5]) or row[4] or "Consumibles > Otros Consumibles"
                
                updates.append((
                    final_cat, 
                    js.get("texto_limpio", ""), 
                    js.get("confianza_ia", 0.0), 
                    js.get("razonamiento", ""), 
                    row[0]
                ))

        if updates:
            cursor.executemany("""
                UPDATE downloads SET categoria_final=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=? 
                WHERE url_hash=?
            """, updates)
            conn.commit()
            print(f"📦 Batch completado. {len(updates)} productos arbitrados.")

    conn.close()

if __name__ == "__main__":
    run_curator()
