import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def auditar_calidad():
    conn = sqlite3.connect(DB_PATH)
    
    print("🕵️ Buscando posibles 'LOGOS o PLACEHOLDERS' colados como VÁLIDOS...")
    print("=" * 80)

    # Buscamos imágenes válidas donde el score de basura no es despreciable (> 0.3)
    query = """
        SELECT url_hash, score_product, score_watermark, score_placeholder, image_suggest_category
        FROM downloads 
        WHERE is_valid = 1 
          AND (score_watermark > 0.3 OR score_placeholder > 0.3)
        ORDER BY score_watermark DESC
        LIMIT 10
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("✅ No se han encontrado sospechosas con los filtros actuales.")
    else:
        print(df.to_string(index=False))
        print("\n⚠️ Si ves registros aquí, el filtro está siendo demasiado permisivo.")

    conn.close()

if __name__ == "__main__":
    auditar_calidad()
