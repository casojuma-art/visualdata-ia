import sqlite3
import requests
import time
import re
import json
import pandas as pd
from difflib import get_close_matches # <--- NUEVO: Para búsqueda aproximada

# =================================================================
# CONFIGURACIÓN
# =================================================================
BATCH_SIZE = 128
API_URL = "http://localhost:8000/v1/chat/completions"
MODEL_NAME = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"

# =================================================================
# 1. CARGA INTELIGENTE DE TAXONOMÍA (Generamos índices de búsqueda)
# =================================================================
print("Cargando y optimizando taxonomía GPC...")
tax_df = pd.read_csv(TAXONOMY_PATH)

# Diccionario A: Ruta completa normalizada -> Ruta Oficial
# Ejemplo: "bricolaje > fontaneria > grifos" -> "Bricolaje > Fontanería > Grifos"
VALID_PATHS = {re.sub(r'\s+', ' ', p).strip().lower(): p for p in tax_df['path'].unique()}

# Diccionario B: Nodo Final -> Ruta Oficial (Para cuando la IA es perezosa)
# Ejemplo: "grifos" -> "Bricolaje > Fontanería > Grifos"
# Nota: Si hay duplicados (ej. "Baterías" en música y en coches), esto tomará el último cargado.
# Para este caso es aceptable, o se podría mejorar con contexto.
LEAF_TO_PATH = {}
for path in tax_df['path'].unique():
    leaf = path.split('>')[-1].strip().lower()
    LEAF_TO_PATH[leaf] = path

# Lista de claves para búsqueda difusa (fuzzy)
VALID_KEYS_LIST = list(VALID_PATHS.keys())

# =================================================================
# 2. PROMPT ENRIQUECIDO CON TUS VARIABLES
# =================================================================
SYSTEM_PROMPT = """
Eres un clasificador experto en Taxonomía de Google (GPC).
Tu objetivo es determinar la RUTA COMPLETA correcta basándote en la evidencia.

ANALISIS DE EVIDENCIA (VARIABLES):
1. CLIP_Valida_Titulo (Score 0-1): Si es alto (>0.85), la IA de visión entiende bien el producto. Confía en su sugerencia.
2. CLIP_Confianza (0-100): Cuidado. Si es muy alta (>95%) pero CLIP_Valida_Titulo es bajo, CLIP está alucinando.
3. BERT_Texto: Suele ser correcto en la categoría general, pero falla en la específica.

INSTRUCCIONES:
- Si la IA de visión tiene alto score de validación con el título, úsala para desambiguar.
- Si la IA de visión parece alucinar (ej. ve "Béisbol" en una "Bomba de agua"), ignórala totalmente.
- Devuelve la ruta más profunda y específica posible del árbol GPC.

Responde ÚNICAMENTE con JSON válido:
{
  "categoria_final": "Ruta/Oficial/Completa",
  "json_atributos": {"clave": "valor"},
  "texto_limpio": "Descripción SEO neutra",
  "confianza_ia": 0.0 a 1.0,
  "razonamiento": "Explica por qué aceptaste o rechazaste a CLIP basándote en los scores."
}
"""

def find_best_match(ia_category):
    """
    Busca la categoría en el árbol oficial usando 3 estrategias:
    1. Match Exacto.
    2. Match de Nodo Final (Hoja).
    3. Match Aproximado (Fuzzy).
    """
    if not ia_category: return None
    
    cat_norm = re.sub(r'\s+', ' ', ia_category).strip().lower()

    # ESTRATEGIA 1: Exacto
    if cat_norm in VALID_PATHS:
        return VALID_PATHS[cat_norm]

    # ESTRATEGIA 2: La IA solo devolvió el final ("Estufas")
    # Limpiamos posibles " > " sueltos
    leaf_candidate = cat_norm.split('>')[-1].strip()
    if leaf_candidate in LEAF_TO_PATH:
        return LEAF_TO_PATH[leaf_candidate]

    # ESTRATEGIA 3: Fuzzy Match (Corrige "Estufa" a "Estufas")
    # Buscamos en las rutas completas
    matches = get_close_matches(cat_norm, VALID_KEYS_LIST, n=1, cutoff=0.85)
    if matches:
        return VALID_PATHS[matches[0]]
        
    # ESTRATEGIA 4: Contenido (Si la IA dijo una sub-frase válida)
    # Buscamos si el string de la IA está contenido dentro de alguna ruta válida
    # Ej: IA dice "Suministros eléctricos > Enchufes" (le falta "de pared")
    for key in VALID_KEYS_LIST:
        if cat_norm in key:
            return VALID_PATHS[key]

    return None

def process_batch(rows):
    if not rows: return []
    output = []
    session = requests.Session()

    for row in rows:
        # Desempaquetamos TUS variables estadísticas
        u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo, val_titulo = row
        
        # Preparamos el contexto estadístico para la IA
        clip_conf_val = float(clip_conf or 0)
        val_titulo_val = float(val_titulo or 0) # CLIP_Valida_Titulo

        user_content = f"""DATOS DEL PRODUCTO:
- Título: {tit}
- Descripción: {desc}
- Atributos: {attrs}

EVIDENCIA DE IA PREVIA:
- Sugerencia BERT (Texto): "{cat_bert}"
- Sugerencia CLIP (Visión): "{cat_clip}"
  > Confianza propia CLIP: {clip_conf_val:.1f}%
  > Validación CLIP vs Título: {val_titulo_val:.4f} (Score clave)
"""

        payload = {
            "model": MODEL_NAME,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content}
            ],
            "temperature": 0.1, # Baja temperatura para ser preciso
            "max_tokens": 800
        }

        try:
            r = session.post(API_URL, json=payload, timeout=30)
            if r.status_code != 200:
                output.append((None, None, None, 0.0, f"API {r.status_code}", u_hash))
                continue

            content = r.json()['choices'][0]['message']['content']
            # Limpieza JSON agresiva
            content = re.sub(r'```json\s*|```', '', content).strip()
            
            js = json.loads(content)
            cat_ia = js.get("categoria_final", "")
            
            # --- AQUÍ ESTÁ LA MAGIA ---
            # Buscamos la ruta oficial basada en lo que dijo la IA
            final_cat_oficial = find_best_match(cat_ia)
            
            # Fallback lógico usando tus variables
            if not final_cat_oficial:
                # Si la IA falló en dar una ruta válida, decidimos entre BERT y CLIP
                # usando la variable CLIP_Valida_Titulo
                if val_titulo_val > 0.6 and cat_clip:
                    # Si CLIP valida bien contra el título, le damos una oportunidad (buscando su ruta oficial)
                    final_cat_oficial = find_best_match(cat_clip)
                
                if not final_cat_oficial:
                    # Si todo falla, volvemos a BERT (o null si bert no existe)
                    final_cat_oficial = VALID_PATHS.get(re.sub(r'\s+', ' ', cat_bert or "").lower(), cat_bert)

            output.append((
                final_cat_oficial,
                json.dumps(js.get("json_atributos", {}), ensure_ascii=False),
                js.get("texto_limpio", ""),
                js.get("confianza_ia", 0.0),
                js.get("razonamiento", ""),
                u_hash
            ))

        except Exception as e:
            output.append((None, None, None, 0.0, f"Error: {str(e)}", u_hash))
            
    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    total = 0

    while True:
        # CAMBIO IMPORTANTE: Agregamos 'score_product' (CLIP_Valida_Titulo) a la query
        # Y filtramos para NO volver a leer infinitamente lo que ya tiene score bajo (score_ia IS NULL)
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, atributos, categoria, 
                   image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads 
            WHERE is_valid = 1 
              AND (categoria_final IS NULL OR (score_ia IS NULL)) 
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("¡Completado! No hay registros pendientes.")
            break

        print(f"Procesando lote de {len(rows)} registros...")
        results = process_batch(rows)
        
        # Update
        batch_update = []
        for cat, attrs, text, score, reason, u_hash in results:
            if reason: # Solo guardamos si hubo respuesta
                batch_update.append((cat, attrs, text, score, reason, u_hash))

        if batch_update:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?
                WHERE url_hash=?
            """, batch_update)
            conn.commit()
            total += len(batch_update)
            print(f"Guardados {len(batch_update)} registros. Total: {total}")

    conn.close()

if __name__ == "__main__":
    run_curator()
