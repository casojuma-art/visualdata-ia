import sqlite3
import requests
import time
import re
import json
import pandas as pd
from difflib import get_close_matches
from concurrent.futures import ThreadPoolExecutor, as_completed

# =================================================================
# CONFIGURACIÓN
# =================================================================
DEBUG_MODE = True  # <--- MODO DEBUG: True para ver resultados, False para correr rápido

BATCH_SIZE = 4 if DEBUG_MODE else 512
MAX_WORKERS = 4  #antres 32
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
# PROMPT V9.4: MAPEO INTELIGENTE BASADO EN AUDITORÍA
# =================================================================
SYSTEM_PROMPT = """
Actúa como un experto catalogador de datos certificado por Google Merchant Center.
Tu objetivo es NORMALIZAR la información basándote estrictamente en los "ATRIBUTOS_ORIGINALES" y el texto del producto.

=== TAREA 1: CLASIFICACIÓN (GPC Español) ===
- Elige ÚNICAMENTE una categoría existente en la taxonomía oficial.
- Usa la ruta completa.

=== TAREA 2: ATRIBUTOS ESTÁNDAR (Mapeo Estricto) ===
Busca en los datos originales las claves de la izquierda (Raw) y mapealas al estándar de la derecha (Google).
Si no encuentras el dato, déjalo null.

GUÍA DE MAPEO (Prioridad):
- "color", "Color", "colores", "color tinta", "acabado"  -> "Color" (Normalizado: "Negro", "Azul"...)
- "material", "Material", "composicion", "Material joyas" -> "Material"
- "genero", "Genero"                                     -> "Sexo" (Valores: "Hombre", "Mujer", "Unisex" o null)
- "Edad", "edad-recomendada"                             -> "Grupo de edad" (Valores: "Adulto", "Niños", "Bebé" o null)
- "talla", "talla-ropa", "Tamaño", "Tamano"              -> "Talla" (Solo Moda/Calzado)
- "largo-producto", "largo", "Longitud"                  -> "Longitud del producto" (Numérico + unidad)
- "ancho-producto", "ancho", "Anchura"                   -> "Anchura del producto" (Numérico + unidad)
- "alto-producto", "alto", "Altura", "profundidad"       -> "Altura del producto" (Numérico + unidad)
- "peso"                                                 -> "Peso del producto" (Numérico + unidad)
- "eficiencia-energetica"                                -> "Nivel de eficiencia energética" ("A", "G"...)

NOTA SOBRE DIMENSIONES: Si encuentras "dimensiones" (ej: "150x40 mm"), intenta separarlo en Longitud/Anchura/Altura si es obvio. Si no, ponlo en detalles.

=== TAREA 3: DETALLES Y MARKETING ===
- "detalles_producto": Mueve AQUÍ cualquier atributo que NO sea estándar (ej: "Tipo", "llanta", "coleccion", "formato", "trazo", "Uso", "Impresion"). Formato Clave-Valor.
- "aspectos_destacados": 3-5 frases cortas de venta (bullet points).

Responde EXCLUSIVAMENTE con este JSON válido:

{
  "categoria_final": "Ruta > De > La > Taxonomía",
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
      "Otra_Característica": "Valor"
  },
  "aspectos_destacados": [
      "Beneficio 1",
      "Beneficio 2"
  ],
  "texto_limpio": "Descripción comercial limpia",
  "confianza_ia": 0.95,
  "razonamiento": "Explicación breve"
}
"""

# =================================================================
# FUNCIONES AUXILIARES
# =================================================================
def extract_json_content(text):
    try:
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            json_str = match.group(0)
            if json_str.startswith("```json"): json_str = json_str[7:]
            if json_str.endswith("```"): json_str = json_str[:-3]
            return json.loads(json_str)
        return None
    except:
        return None

def merge_attributes(old_attrs_str, new_data):
    try:
        old_dict = json.loads(old_attrs_str) if old_attrs_str and old_attrs_str != 'null' else {}
    except:
        old_dict = {}
    if not isinstance(old_dict, dict): old_dict = {}
    if "clave" in old_dict: del old_dict["clave"]

    final_structure = {
        "atributos_estandar": new_data.get("atributos_estandar", {}),
        "detalles_producto": new_data.get("detalles_producto", {}),
        "aspectos_destacados": new_data.get("aspectos_destacados", []),
        "raw_feed": old_dict
    }
    return json.dumps(final_structure, ensure_ascii=False)

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

def process_single_row(row):
    u_hash, tit, desc, attrs, cat_bert, cat_clip, clip_conf, score_cat, cuerpo, val_titulo = row
    
    # En el contexto, enfatizamos los atributos originales
    contexto_user = f"""
    PRODUCTO: {tit}
    DESCRIPCIÓN: {desc[:800] if desc else (cuerpo[:800] if cuerpo else "")}
    ATRIBUTOS_ORIGINALES: {attrs}
    SUGERENCIA VISUAL: {cat_clip}
    """

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": contexto_user}
        ],
        "temperature": 0.1, 
        "max_tokens": 1400
    }

    try:
        r = requests.post(API_URL, json=payload, timeout=90)
        if r.status_code != 200: return None

        content = r.json()['choices'][0]['message']['content']
        js = extract_json_content(content)
        
        if not js: return None

        cat_ia = js.get("categoria_final", "").strip()
        final_cat = find_best_match(cat_ia)
        
        if not final_cat and float(clip_conf or 0) > 80: final_cat = find_best_match(cat_clip)
        if not final_cat and cat_bert: final_cat = find_best_match(cat_bert)
        if not final_cat: final_cat = "Consumibles > Otros Consumibles"

        atributos_finales = merge_attributes(attrs, js)

        if DEBUG_MODE:
            print("\n" + "="*60)
            print(f"📦 {tit[:45]}...")
            print(f"📂 {final_cat}")
            std = js.get('atributos_estandar', {})
            # Vista rápida de mapeo
            print(f"📏 L:{std.get('Longitud del producto')} W:{std.get('Anchura del producto')} H:{std.get('Altura del producto')} | ⚖️ {std.get('Peso del producto')}")
            print(f"🎨 Color:{std.get('Color')} | Mat:{std.get('Material')} | 🚻 {std.get('Sexo')} | 👶 {std.get('Grupo de edad')}")
            print("-" * 20)
            print(f"🔧 DETALLES EXTRA (Top 3): {list(js.get('detalles_producto', {}).keys())[:3]}...")
            print(f"📢 MARKETING: {js.get('aspectos_destacados', [])[0] if js.get('aspectos_destacados') else ''}")
            print("="*60)

        return (
            final_cat,
            atributos_finales,
            js.get("texto_limpio", desc),
            js.get("confianza_ia", 0.0),
            js.get("razonamiento", ""),
            u_hash
        )

    except Exception as e:
        if DEBUG_MODE: print(f"❌ Error: {e}")
        return None

def process_batch_parallel(rows):
    output = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_row, row): row for row in rows}
        for future in as_completed(futures):
            res = future.result()
            if res: output.append(res)
    return output

def run_curator():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    print(f"🚀 Iniciando Curador V9.4 (Mapeo Auditado)")
    
    while True:
        cursor.execute(f"""
            SELECT url_hash, titulo, descripcion, 
                   COALESCE(atributos_originales, atributos) as atributos_raw, 
                   categoria, image_suggest_category, confidence, score_category, cuerpo_limpio, score_product
            FROM downloads
            WHERE is_valid = 1
              AND (categoria_final IS NULL OR score_ia IS NULL)
              AND (curator_attempts < 2 OR curator_attempts IS NULL)
            LIMIT {BATCH_SIZE}
        """)
        rows = cursor.fetchall()
        if not rows:
            print("💤 Todo procesado.")
            break

        results = process_batch_parallel(rows)
        
        if results:
            cursor.executemany("""
                UPDATE downloads SET 
                    categoria_final=?, atributos=?, cuerpo_limpio=?, score_ia=?, razonamiento_ia=?,
                    curator_attempts = COALESCE(curator_attempts, 0) + 1
                WHERE url_hash=?
            """, results)
            
            successful = set(r[5] for r in results)
            failed = [r[0] for r in rows if r[0] not in successful]
            if failed:
                 cursor.executemany("UPDATE downloads SET curator_attempts = COALESCE(curator_attempts, 0) + 1 WHERE url_hash = ?", [(h,) for h in failed])
            
            conn.commit()
            if not DEBUG_MODE: print(f"✅ Lote de {len(results)} items guardado.")

    conn.close()

if __name__ == "__main__":
    run_curator()
