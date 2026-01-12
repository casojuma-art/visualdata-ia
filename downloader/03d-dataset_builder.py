import sqlite3
import json
import os

# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"
OUTPUT_JSONL = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
IMG_BASE_PATH = "/lab/visualdata-ia/imagenes_in"

def build_dataset():
    print("🎓 INICIANDO LA UNIVERSIDAD (Generador de Dataset VLM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # QUERY AUDITADA:
    # 1. is_valid=1 (Imagen visualmente correcta)
    # 2. categoria_final IS NOT NULL (Pasó por el Árbitro 03b)
    # 3. cuerpo_limpio IS NOT NULL (Pasó por el Redactor 03b)
    # 4. atributos SIN error_json (Pasó por el Extractor 03c)
    print("⏳ Ejecutando consulta maestra...")
    cursor.execute("""
        SELECT url_hash, titulo, cuerpo_limpio, atributos, categoria_final 
        FROM downloads 
        WHERE is_valid = 1 
          AND categoria_final IS NOT NULL 
          AND cuerpo_limpio IS NOT NULL 
          AND atributos IS NOT NULL 
          AND atributos NOT LIKE '%error_json%'
    """)
    
    rows = cursor.fetchall()
    print(f"📊 Registros Calificados para Entrenar: {len(rows)}")
    
    count = 0
    with open(OUTPUT_JSONL, 'w', encoding='utf-8') as f:
        for row in rows:
            # Reconstrucción de ruta de imagen (Validada en 00-descargaimagenes)
            h = row['url_hash']
            img_path = os.path.join(IMG_BASE_PATH, h[:2], h[2:4], f"{h}.jpg")
            
            # Doble check de existencia física
            if not os.path.exists(img_path):
                continue

            try:
                # Validar que 'atributos' sea JSON real y no texto roto
                attrs_json = json.loads(row['atributos'])
                
                # CONSTRUCCIÓN DE LA RESPUESTA PERFECTA (TARGET)
                respuesta_ideal = json.dumps({
                    "titulo": row['titulo'],
                    "categoria": row['categoria_final'],      # <--- DATO CURADO
                    "atributos": attrs_json,                  # <--- DATO CURADO
                    "descripcion_tecnica": row['cuerpo_limpio'] # <--- DATO CURADO
                }, ensure_ascii=False)

                # Prompt de entrenamiento
                conversations = [
                    {
                        "from": "human",
                        "value": f"<image>\nAnaliza este producto:\n- Título: {row['titulo']}\nDictamina la categoría oficial completa, extrae todos los atributos visuales en JSON y limpia la descripción."
                    },
                    {
                        "from": "gpt",
                        "value": respuesta_ideal
                    }
                ]

                entry = {
                    "id": h,
                    "image": img_path,
                    "conversations": conversations
                }
                
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                count += 1
                
            except Exception as e:
                # Si falla el json.loads, es un atributo corrupto que se nos pasó. Lo saltamos.
                continue

    print(f"✅ Dataset generado en: {OUTPUT_JSONL}")
    print(f"📚 Total de muestras escritas: {count}")
    conn.close()

if __name__ == "__main__":
    build_dataset()
