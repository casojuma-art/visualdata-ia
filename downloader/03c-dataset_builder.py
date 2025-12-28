import sqlite3
import json
import os
from tqdm import tqdm

# =================================================================
# CONFIGURACIÓN (Node 211)
# =================================================================
DB_PATH = "/lab/visualdata-ia/db/registry.db"
IMG_BASE_PATH = "/lab/visualdata-ia/imagenes_in"
OUTPUT_JSONL = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"

# UMBRAL DE CALIDAD: Solo registros con alta confianza de la IA pasan al entrenamiento
MIN_SCORE_IA = 0.95

def build_dataset():
    """
    Construye el Dataset de Oro filtrando por el score de confianza de la IA.
    """
    print(f"🚀 [03c] Generando Dataset de Oro (Umbral Calidad: {MIN_SCORE_IA})...")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: DB no encontrada en {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Query que incluye el score_ia para filtrado
    query = """
        SELECT 
            url_hash, titulo, categoria_final, atributos, cuerpo_limpio,
            categoria, image_suggest_category, score_product, score_ia
        FROM downloads 
        WHERE is_valid = 1 
          AND categoria_final IS NOT NULL 
          AND score_ia >= ?;
    """
    
    cursor.execute(query, (MIN_SCORE_IA,))
    filas = cursor.fetchall()
    
    if not filas:
        print(f"⚠️ No hay registros con score_ia >= {MIN_SCORE_IA}. Baja el umbral o procesa más datos.")
        conn.close()
        return

    print(f"📦 Filtrados {len(filas):,} registros de alta calidad. Generando JSONL...")
    os.makedirs(os.path.dirname(OUTPUT_JSONL), exist_ok=True)

    success_count = 0
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for row in tqdm(filas, unit="reg"):
            u_hash, tit, cat_f, attr_f, text_f, cat_b, cat_c, s_prod, s_ia = row
            
            # Ruta física de la imagen
            path_img = os.path.join(IMG_BASE_PATH, u_hash[:2], u_hash[2:4], f"{u_hash}.jpg")
            
            if os.path.exists(path_img):
                # ESTRUCTURA DE ENTRENAMIENTO VLM
                # Incluimos los errores de BERT/CLIP en la entrada para que la IA aprenda a corregirlos
                entry = {
                    "id": u_hash,
                    "image": path_img,
                    "conversations": [
                        {
                            "from": "human",
                            "value": (
                                f"<image>\nAnaliza este producto:\n"
                                f"- Título: {tit}\n"
                                f"- Sugerencia Texto: {cat_b}\n"
                                f"- Sugerencia Visión: {cat_c} (Confianza Visual: {s_prod:.2f})\n\n"
                                f"Dictamina la categoría oficial, extrae atributos y limpia la descripción."
                            )
                        },
                        {
                            "from": "gpt",
                            "value": (
                                f"CATEGORIA_FINAL: {cat_f}\n"
                                f"JSON_ATRIBUTOS: {attr_f}\n"
                                f"TEXTO_LIMPIO: {text_f}"
                            )
                        }
                    ]
                }
                f.write(json.dumps(entry, ensure_ascii=False) + '\n')
                success_count += 1

    conn.close()
    
    # Informe de calidad
    talla_fichero = os.path.getsize(OUTPUT_JSONL) / (1024 * 1024) # MB
    print(f"\n✅ Dataset de Oro listo para Fase 04.")
    print(f"📊 Registros exportados: {success_count}")
    print(f"⚖️ Tamaño: {talla_fichero:.2f} MB")
    print(f"📍 Ruta: {OUTPUT_JSONL}")

if __name__ == "__main__":
    build_dataset()
