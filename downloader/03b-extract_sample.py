import sqlite3
import asyncio
import aiohttp
import pandas as pd
import re
import difflib
import json
from tqdm.asyncio import tqdm

# =================================================================
# CONFIGURACIÓN (Node 211 - RTX 5090)
# =================================================================
CONCURRENCY_LIMIT = 100 
API_URL = "http://172.17.0.1:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

def normalize_path(path_str):
    if not path_str: return ""
    return re.sub(r'\s+', ' ', str(path_str)).replace(' > ', '>').replace('> ', '>').strip().lower()

# =================================================================
# CARGA DE TAXONOMÍA
# =================================================================
print("📖 Cargando Taxonomía y activando Validación Cruzada...")
tax_df = pd.read_csv(TAXONOMY_PATH)
VALID_PATHS_MAP = {normalize_path(p): p for p in tax_df['path'].unique()}
VALID_PATHS_LIST = list(VALID_PATHS_MAP.keys())

# =================================================================
# MOTOR DE CURACIÓN CON DEEP CONTEXT
# =================================================================
async def master_process(session, semaphore, row):
    u_hash, tit, desc, attrs_orig, cat_bert, cat_clip, s_cat, s_prod, s_qual = row
    async with semaphore:
        # PROMPT DE JERARQUÍA DE EVIDENCIA
        system_msg = (
            "Eres un Árbitro de Taxonomía. Tu objetivo es determinar la CATEGORIA_FINAL oficial de Google.\n"
            "JERARQUÍA DE VERDAD:\n"
            "1. EL TÍTULO Y LA DESCRIPCIÓN MANDAN: Si el texto menciona nombres específicos y datos técnicos (medidas, dosis, materiales), "
            "esa es la verdad absoluta. CLIP solo se usa para confirmar o si el título es vago.\n"
            "2. ESCEPTICISMO ANTE SCORES ALTOS: Si CLIP da confianza alta (0.9-1.0) pero sugiere algo ilógico para el título "
            "(ej. sugiere 'limpieza' para un 'anillo'), DESCARTA la visión y usa el texto.\n"
            "3. FORMATO ESTRICTO: Responde solo CATEGORIA_FINAL:, JSON_ATRIBUTOS: y TEXTO_LIMPIO:."
        )
        
        # Enviamos TODA la información para que la IA tenga contexto total
        user_msg = (
            f"--- DATOS DEL PRODUCTO ---\n"
            f"TÍTULO: {tit}\n"
            f"DESCRIPCIÓN: {desc}\n"
            f"ATRIBUTOS TÉCNICOS: {attrs_orig}\n\n"
            f"--- SUGERENCIAS PREVIAS ---\n"
            f"TEXTO (BERT): {cat_bert}\n"
            f"VISIÓN (CLIP): {cat_clip} (Confianza: {s_prod:.4f})\n\n"
            f"Instrucción: Arbitra y genera la ficha técnica definitiva."
        )
        
        payload = {
            "model": MODEL_NAME,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ],
            "temperature": 0.1
        }
        
        try:
            async with session.post(API_URL, json=payload, timeout=50) as r:
                if r.status == 200:
                    res = await r.json()
                    raw = res['choices'][0]['message']['content']
                    
                    # Extracción y Limpieza de "meta-charla"
                    c_f = raw.split("CATEGORIA_FINAL:")[1].split("JSON_ATRIBUTOS:")[0].strip()
                    a_f = raw.split("JSON_ATRIBUTOS:")[1].split("TEXTO_LIMPIO:")[0].strip()
                    t_f = raw.split("TEXTO_LIMPIO:")[1].strip()
                    t_f = t_f.split("Por lo tanto")[0].split("Nota:")[0].split("Punto clave:")[0].strip()
                    
                    norm_c_f = normalize_path(c_f)
                    
                    # 1. Validación de Ruta
                    if norm_c_f in VALID_PATHS_LIST:
                        return u_hash, VALID_PATHS_MAP[norm_c_f], a_f, t_f
                    
                    # 2. Rescate Fuzzy
                    matches = difflib.get_close_matches(norm_c_f, VALID_PATHS_LIST, n=1, cutoff=0.85)
                    if matches:
                        return u_hash, VALID_PATHS_MAP[matches[0]], a_f, t_f
                    
                    # 3. Fallback de Seguridad
                    final_choice = cat_clip if s_prod > 0.95 else cat_bert
                    return u_hash, final_choice, a_f, t_f
        except: pass
    return u_hash, None, None, None

async def run_master():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url_hash, titulo, descripcion, atributos, categoria, 
               image_suggest_category, score_category, score_product, score_quality 
        FROM downloads WHERE is_valid = 1 AND categoria_final IS NULL
    """)
    filas = cursor.fetchall()
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    success, fail = 0, 0
    
    print(f"🚀 [03b] Iniciando Arbitraje Deep Context sobre {len(filas):,} registros...")
    
    async with aiohttp.ClientSession() as session:
        tasks = [master_process(session, semaphore, row) for row in filas]
        batch = []
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="it")
        
        for f in pbar:
            u_hash, cat, attr, text = await f
            if cat:
                batch.append((cat, attr, text, u_hash))
                success += 1
            else: fail += 1
            
            if (success + fail) % 20 == 0:
                y = (success/(success+fail+0.1))*100
                pbar.set_description(f"Yield: {y:.1f}% | OK: {success}")

            if len(batch) >= 100:
                cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=? WHERE url_hash=?", batch)
                conn.commit()
                batch = []
        
        if batch:
            cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=? WHERE url_hash=?", batch)
            conn.commit()
    conn.close()

if __name__ == "__main__":
    asyncio.run(run_master())
