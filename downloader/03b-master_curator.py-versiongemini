import sqlite3
import asyncio
import aiohttp
import pandas as pd
import re
import difflib
import json
import os
from tqdm.asyncio import tqdm

# =================================================================
# CONFIGURACIÓN (Nodo 211 - RTX 5090)
# =================================================================
CONCURRENCY_LIMIT = 100 
API_URL = "http://172.17.0.1:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

def normalize_path(p):
    if not p: return ""
    return re.sub(r'\s+', ' ', str(p)).replace(' > ', '>').strip().lower()

def get_l2_family(path):
    """Extrae la familia de nivel 2 para detectar conflictos de vertical."""
    parts = str(path).split(' > ')
    return " > ".join(parts[:2]).strip().lower()

# Carga de Taxonomía GPC Oficial
print("🧠 Cargando Taxonomía GPC y activando modo Veredicto Soberano...")
tax_df = pd.read_csv(TAXONOMY_PATH)
VALID_PATHS_MAP = {normalize_path(p): p for p in tax_df['path'].unique()}
VALID_PATHS_LIST = list(VALID_PATHS_MAP.keys())

# =================================================================
# PROMPT DE CLASIFICACIÓN "ESTADO CERO" (Sin influencias)
# =================================================================
# Aquí NO mencionamos a BERT, ni a CLIP, ni que hay errores.
SYSTEM_PROMPT_CLEAN = (
    "Eres un experto en taxonomía de Google Merchant Center (GPC).\n"
    "Tu tarea es asignar la categoría oficial más precisa a un producto.\n\n"
    "REGLAS:\n"
    "1. CATEGORIA_FINAL: Identifica el objeto por su Título/Descripción y usa la ruta GPC oficial.\n"
    "2. JSON_ATRIBUTOS: Extrae solo datos técnicos (marca, material, medidas). PROHIBIDO incluir la categoría aquí.\n"
    "3. TEXTO_LIMPIO: Descripción SEO técnica, sin comentarios ni asteriscos.\n"
    "4. CONFIANZA_IA: Valor de 0.0 a 1.0 según la certeza de tu clasificación.\n\n"
    "Responde solo con las etiquetas y los datos."
)

def extract_robust(text):
    """Captura datos puros ignorando cualquier razonamiento extra."""
    text = text.replace('**', '').replace('###', '').strip()
    try:
        c = re.search(r"CATEGOR[IÍ]A_?FINAL\s*:\s*(.*?)(?=JSON_ATRIBUTOS|TEXTO_LIMPIO|CONFIANZA_IA|$)", text, re.I | re.S)
        a = re.search(r"JSON_ATRIBUTOS\s*:\s*(\{.*?\})", text, re.I | re.S)
        t = re.search(r"TEXTO_LIMPIO\s*:\s*(.*?)(?=CONFIANZA_IA|Razonamiento|Nota|$)", text, re.I | re.S)
        s = re.search(r"CONFIANZA_?IA\s*:\s*([\d\.]+)", text, re.I)
        
        cat = c.group(1).strip() if c else None
        attr = a.group(1).strip() if a else "{}"
        txt = t.group(1).strip() if t else None
        score = float(s.group(1).strip()) if s else 0.0
        
        if txt: txt = txt.split("\n\n")[0].strip()
        return cat, attr, txt, score
    except: return None, None, None, 0.0

async def master_process(session, semaphore, row):
    u_hash, tit, desc, attrs_orig, cat_bert, cat_clip, conf_clip, s_cat, cuerpo = row
    
    # DETECCIÓN SILENCIOSA DE CONFLICTO
    # Si las familias L2 no coinciden, forzamos la "Pregunta Nueva".
    is_clash = get_l2_family(cat_bert) != get_l2_family(cat_clip)

    async with semaphore:
        if is_clash or s_cat < 0.3:
            # --- CASO DE CONFLICTO: PREGUNTA NUEVA (CERO MEMORIA) ---
            user_msg = (
                f"FICHA TÉCNICA DEL PRODUCTO:\n"
                f"- TÍTULO: {tit}\n"
                f"- DESCRIPCIÓN: {str(desc)[:700]}\n"
                f"- DETALLES ADICIONALES: {str(cuerpo)[:800]}\n"
                f"- ATRIBUTOS: {attrs_orig}"
            )
        else:
            # --- CASO DE CONSENSO: VIA RÁPIDA ---
            user_msg = f"PRODUCTO: {tit}\nCATEGORÍA SUGERIDA: {cat_bert}\nValida y extrae datos."

        payload = {
            "model": MODEL_NAME, 
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT_CLEAN},
                {"role": "user", "content": user_msg}
            ],
            "temperature": 0, "max_tokens": 800
        }
        
        try:
            async with session.post(API_URL, json=payload, timeout=60) as r:
                if r.status == 200:
                    res = await r.json()
                    c_f, a_f, t_f, s_ia = extract_robust(res['choices'][0]['message']['content'])
                    if not c_f or not t_f: return u_hash, None, None, None, 0.0
                    
                    norm_c_f = normalize_path(c_f)
                    if norm_c_f in VALID_PATHS_LIST:
                        return u_hash, VALID_PATHS_MAP[norm_c_f], a_f, t_f, s_ia
                    
                    matches = difflib.get_close_matches(norm_c_f, VALID_PATHS_LIST, n=1, cutoff=0.85)
                    return u_hash, (VALID_PATHS_MAP[matches[0]] if matches else cat_bert), a_f, t_f, s_ia
        except: pass
    return u_hash, None, None, None, 0.0

async def run_master():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Prioridad: Fallos previos (score < 0.3) o registros nuevos (null)
    cursor.execute("""
        SELECT url_hash, titulo, descripcion, atributos, categoria, 
               image_suggest_category, confidence, score_category, cuerpo_limpio 
        FROM downloads WHERE is_valid = 1 AND (categoria_final IS NULL OR score_ia < 0.3)
    """)
    filas = cursor.fetchall()
    
    print(f"🚀 [03b] Iniciando Clasificación Independiente sobre {len(filas):,} registros...")
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    success = 0
    async with aiohttp.ClientSession() as session:
        tasks = [master_process(session, semaphore, row) for row in filas]
        batch = []
        pbar = tqdm(asyncio.as_completed(tasks), total=len(tasks), unit="it")
        for f in pbar:
            u_hash, cat, attr, text, s_ia = await f
            if cat:
                batch.append((cat, attr, text, s_ia, u_hash))
                success += 1
            if len(batch) >= 50:
                cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=? WHERE url_hash=?", batch)
                conn.commit()
                batch = []
                pbar.set_description(f"OK: {success}")
        if batch:
            cursor.executemany("UPDATE downloads SET categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=? WHERE url_hash=?", batch)
            conn.commit()
    conn.close()

if __name__ == "__main__":
    asyncio.run(run_master())
