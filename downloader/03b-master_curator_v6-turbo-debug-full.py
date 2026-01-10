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
DEBUG_MODE = True   # <--- ACTIVADO: Verás entrada DB completa y salida IA

# En Debug, batch=1 para leer bien. En Producción, batch=2048
BATCH_SIZE = 1 if DEBUG_MODE else 2048 

MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv" # Usamos tu CSV confirmado

# =================================================================
# PROMPT: EL ÁRBITRO (V6 Lógica Original + Resumen Rico)
# =================================================================
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

# =================================================================
# CARGA DE TAXONOMÍA
# =================================================================
print(f"Cargando taxonomía desde {TAXONOMY_PATH}...")
try:
    tax_df = pd.read_csv(TAXONOMY_PATH)
    if 'path' not in tax_df.columns: raise ValueError("Falta columna 'path' en el CSV")
    
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
    
    # 1. Exacto
    if cat_norm in VALID_PATHS: return VALID_PATHS[cat_norm]
    
    # 2. Hoja
    leaf = cat_norm.split('>')[-1].strip()
    if leaf in LEAF_TO_PATH: return LEAF_TO_PATH[leaf]
    
    # 3. Fuzzy
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches: return VALID_PATHS[matches[0]]
    
    return None

def build_prompt(row):
    # Desempaquetado de DB
    u_hash, tit, desc, attrs, cat_bert, cat_clip, conf_clip, score_bert, cuerpo_es, score_prod = row

    # --- DEBUG: MOSTRAR DATOS DB ---
    if DEBUG_MODE:
        print("\n" + "🟦"*20 + f" DATO ENTRADA DB ({u_hash[:6]}) " + "🟦"*20)
        print(f"📌 TITULO:       {tit}")
        print(f"📌 DESC (BD):    {str(desc)[:100]}... [len={len(str(desc))}]")
        print(f"📌 CUERPO (BD):  {str(cuerpo_es)[:100]}... [len={len(str(cuerpo_es)) if cuerpo_es else 0}]")
        print(f"📌 ATRIBUTOS:    {attrs}")
        print(f"🤖 SUG. BERT:    {cat_bert} (Score: {score_bert})")
        print(f"🖼️ SUG. CLIP:    {cat_clip} (Conf: {conf_clip})")
        print("-" * 80)

    # PREPARAR CONTEXTO PARA EL TEXTO LIMPIO
    # Unimos todo para que la IA tenga de donde sacar las 300 palabras
    cuerpo_safe = str(cuerpo_es) if cuerpo_es else ""
    desc_safe = str(desc) if desc else ""
    
    # Limitamos cuerpo a 1500 chars para no saturar tokens pero dar info de sobra
    contexto_completo = f"DESCRIPCIÓN: {desc_safe}\n\nDETALLE TÉCNICO/CUERPO: {cuerpo_safe[:1500]}"

    user_content = (
        f"DATOS DEL PRODUCTO:\n"
        f"Título: {tit}\n"
        f"Atributos Técnicos: {attrs}\n\n"
        f"CONTENIDO PARA RESUMIR:\n{contexto_completo}\n\n"
        f"SUGERENCIAS DE CATEGORÍA (Úsalas de guía):\n"
        f"- Modelo Texto (BERT): {cat_bert}\n"
        f"- Modelo Imagen (CLIP): {cat_clip}"
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
    print("🚀 Cargando vLLM en RTX 5090 (Modo ÁRBITRO + TEXTO EXTENDIDO)...")
    
    llm = LLM(
        model=MODEL_PATH,
        gpu_memory_utilization=0.90,
        max_num_seqs=256,
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

    while True:
        # Recuperamos todas las columnas necesarias
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
            
            # Temperatura dinámica para decidir
            clip_conf = float(row[6] or 0)
            val_titulo = float(row[9] or 0)
            # Si hay dudas (CLIP bajo), subimos temp para que razone más. Si es claro, baja.
            temp = 0.1 if val_titulo > 0.75 and clip_conf > 80 else 0.5
            
            # Max tokens generoso (1000) para permitir el texto largo de 300 palabras
            sampling_params_list.append(SamplingParams(temperature=temp, top_p=0.9, max_tokens=1000))

        if DEBUG_MODE: print("\n🤔 Pensando (Inferencia vLLM)...")
        outputs = llm.generate(prompts_list, sampling_params_list)
        
        updates = []
        failed_hashes = []

        for i, output in enumerate(outputs):
            row = rows[i]
            generated_text = output.outputs[0].text
            
            if DEBUG_MODE:
                print("\n" + "🟩"*20 + f" SALIDA IA ({row[0][:6]}) " + "🟩"*20)
                print(generated_text.strip())
                print("=" * 80)

            js = extract_json_content(generated_text)
            
            if js:
                cat_ia = js.get("categoria_final", "").strip()
                final_cat = find_best_match(cat_ia)
                texto_limpio = js.get("texto_limpio", "").strip()

                # --- Lógica de Rescate V6 ---
                if not final_cat and cat_ia:
                    # Intentar por partes (padres)
                    leaves = [p.strip().lower() for p in cat_ia.split('>') if p.strip()]
                    for leaf in reversed(leaves):
                        if leaf in LEAF_TO_PATH:
                            final_cat = LEAF_TO_PATH[leaf]; break
                    # Intentar fuzzy
                    if not final_cat:
                        matches = get_close_matches(re.sub(r'\s+', ' ', cat_ia).strip().lower(), VALID_KEYS_LIST, n=1, cutoff=0.7)
                        if matches: final_cat = VALID_PATHS[matches[0]]
                
                # Fallbacks a sugerencias si IA falla totalmente
                if not final_cat and float(row[9] or 0) > 0.65 and row[5]:
                    final_cat = find_best_match(row[5]) or VALID_PATHS.get(re.sub(r'\s+', ' ', row[5]).strip().lower(), row[5])
                
                if not final_cat and row[4]:
                    final_cat = VALID_PATHS.get(re.sub(r'\s+', ' ', row[4]).strip().lower(), row[4])
                
                if not final_cat:
                    final_cat = "Consumibles > Otros Consumibles"
                
                if DEBUG_MODE:
                    print(f"🏆 CAT FINAL: {final_cat}")

                updates.append((
                    final_cat,
                    texto_limpio, 
                    js.get("confianza_ia", 0.0), 
                    js.get("razonamiento", ""), 
                    row[0]
                ))
            else:
                if DEBUG_MODE: print("❌ JSON ROTO")
                failed_hashes.append(row[0])

        if updates:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?, 
                    curator_attempts = COALESCE(curator_attempts, 0) + 1 
                WHERE url_hash=?
            """, updates)
            
        if failed_hashes:
            cursor.executemany("UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?", [(h,) for h in failed_hashes])
            
        conn.commit()

        if DEBUG_MODE:
            print("\n⏸️ Pausa de inspección (Ctrl+C para salir)...")
            time.sleep(5)

    conn.close()

if __name__ == "__main__":
    run_curator()
