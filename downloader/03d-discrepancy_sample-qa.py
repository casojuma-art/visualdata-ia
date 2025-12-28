import sqlite3
import pandas as pd
import os

# CONFIGURACIÓN (Node 211)
DB_PATH = "/lab/visualdata-ia/db/registry.db"
IMG_BASE_PATH = "/lab/visualdata-ia/imagenes_in"
OUTPUT_SAMPLE = "/lab/visualdata-ia/data_preparada/auditoria_aleatoria_300.csv"

def generar_auditoria_aleatoria():
    """
    Genera una muestra aleatoria de 300 registros para auditoría de calidad.
    Incluye todos los niveles de coincidencia entre BERT y CLIP.
    """
    print("🧪 [03d-QA] Generando auditoría aleatoria de 300 registros...")
    
    if not os.path.exists(os.path.dirname(OUTPUT_SAMPLE)):
        os.makedirs(os.path.dirname(OUTPUT_SAMPLE))

    conn = sqlite3.connect(DB_PATH)
    
    # Selección aleatoria pura de registros válidos
    query = """
        SELECT 
            url_hash, titulo, 
            categoria as cat_bert, 
            image_suggest_category as cat_clip, 
            score_category, score_product, confidence, score_quality
        FROM downloads 
        WHERE is_valid = 1
        ORDER BY RANDOM() 
        LIMIT 300
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        print("❌ No hay registros validados para auditar.")
        return

    # Construcción de ruta física para inspección visual
    df['ruta_fisica_imagen'] = df['url_hash'].apply(
        lambda h: os.path.join(IMG_BASE_PATH, h[:2], h[2:4], f"{h}.jpg")
    )

    # Exportación con separador ';' para compatibilidad
    df.to_csv(OUTPUT_SAMPLE, sep=';', index=False, encoding='utf-8')
    
    print(f"✅ Auditoría generada: {OUTPUT_SAMPLE}")
    print("\n📋 OBJETIVO DE ESTA MUESTRA:")
    print("- Analizar casos de éxito (BERT y CLIP coinciden).")
    print("- Analizar casos de duda (Coinciden en L2 pero no en L4).")
    print("- Analizar si los scores de calidad reflejan la realidad visual.")

if __name__ == "__main__":
    generar_auditoria_aleatoria()
