import sqlite3
import requests
import time
import re
import json
import pandas as pd
from tqdm import tqdm

# =================================================================
# CONFIGURACIÓN (tu setup que ya funciona)
# =================================================================
BATCH_SIZE = 256  # Puedes probar 128 o 512 según VRAM
API_URL = "http://localhost:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# Carga taxonomía
print("Cargando taxonomía GPC...")
tax_df = pd.read_csv(TAXONOMY_PATH)
VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}

# Prompt fijo (system) - sin placeholders para evitar errores de format
SYSTEM_PROMPT = """
Eres un árbitro experto en taxonomía GPC (Google Product Category).
Tu misión es decidir la categoría oficial más precisa para el producto.

REGLAS DE DECISIÓN:
1. Si BERT y CLIP coinciden en los primeros 2 niveles → úsalos como base.
2. Si CLIP tiene confianza > 0.8 → priorízalo (la imagen es evidencia fuerte).
3. Si ambas sugerencias fallan o son vagas → analiza la ficha técnica y propone la más precisa.
4. Siempre elige la ruta GPC oficial más cercana de la taxonomía.

Responde ÚNICAMENTE con JSON válido (nada más):
{
  "categoria_final": "Ruta GPC completa",
  "json_atributos": {"clave": "valor", ...},
  "texto_limpio": "Descripción técnica neutral SEO (máx 800 caracteres)",
  "confianza_ia": 0.95,
  "razonamiento": "Breve explicación (máx 100 palabras)"
}
"""

def process_batch(rows):
    if not rows:
        return []

    messages_list = []
    hashes = []
    for u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo in rows:
        # Mensaje del usuario: solo los datos del producto
        user_content = f"""Producto a clasificar:
- Título: {tit or "Sin título"}
- Descripción: {desc or "Sin descripción"}
- Atributos: {attrs or "{}"}
- Texto largo: {cuerpo or "Sin texto largo"}
- Sugerencia BERT (texto): {cat_bert or "Ninguna"}
- Sugerencia CLIP (imagen): {cat_clip or "Ninguna"} (confianza: {clip_conf or 0.0:.2f})
"""

        messages_list.append([
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ])
        hashes.append(u_hash)

    payload = {
        "model": MODEL_NAME,
        "messages": messages_list,
        "temperature": 0,
        "max_tokens": 800
    }

    try:
        r = requests.post(API_URL, json=payload, timeout=300)
        if r.status_code != 200:
            print(f"❌ Error API {r.status_code}: {r.text[:500]}")
            return [(h, None, None, None, 0.0, "API error") for h in hashes]

        results = r.json()['choices']
        output = []
        for i, res in enumerate(results):
            try:
                content = res['message']['content'].strip()
                js = json.loads(content)
                cat = js.get("categoria_final", "")
                norm_cat = re.sub(r'\s+', ' ', cat).strip().lower()
                final_cat = VALID_PATHS.get(norm_cat, rows[i][4] if i < len(rows) else cat)  # fallback a BERT si no encuentra

                output.append((
                    final_cat,
                    json.dumps(js.get("json_atributos", {}), ensure_ascii=False),
                    js.get("texto_limpio", ""),
                    js.get("confianza_ia", 0.0),
                    js.get("razonamiento", ""),
                    hashes[i]
                ))
            except Exception as e:
                output.append((None, None, None, 0.0, f"JSON inválido: {e}", hashes[i]))
        return output
    except Exception as e:
        print(f"❌ Excepción batch: {e}")
        return [(None, None, None, 0.0, str(e), h) for h in hashes]

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    total = 0
    start = time.time()

    while True:
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria, 
                   image_suggest_category, confidence, score_category, cuerpo_limpio
            FROM downloads 
            WHERE is_valid = 1 AND (categoria_final IS NULL OR score_ia < 0.5)
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Completado! No hay más registros pendientes.")
            break

        batch_start = time.time()
        results = process_batch(rows)

        saved = 0
        batch_update = []
        for cat, attrs, text, score, reason, u_hash in results:
            if cat:
                batch_update.append((cat, attrs, text, score, reason, u_hash))
                saved += 1

        if batch_update:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?
                WHERE url_hash=?
            """, batch_update)
            conn.commit()

        total += saved
        elapsed = time.time() - batch_start
        print(f"Batch: {saved}/{len(rows)} OK | Tiempo: {elapsed:.1f}s | Total procesado: {total:,}")

    total_time = (time.time() - start) / 3600
    print(f"FIN: {total:,} items procesados en {total_time:.1f} horas")
    conn.close()

if __name__ == "__main__":
    run_curator()
