import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def ver_sanos():
    conn = sqlite3.connect(DB_PATH)
    
    print("🔎 BUSCANDO REGISTROS CON CLIP CORRECTO (Texto + Confianza)...")
    
    # Seleccionamos 10 aleatorios que TENGAN categoría
    df = pd.read_sql_query("""
        SELECT url_hash, confidence, image_suggest_category, titulo 
        FROM downloads 
        WHERE is_valid = 1 
          AND image_suggest_category IS NOT NULL 
          AND image_suggest_category != ''
          AND confidence > 80
        ORDER BY RANDOM()
        LIMIT 10
    """, conn)
    
    if not df.empty:
        # Ajustamos el ancho para que se lea bien en consola
        pd.set_option('display.max_colwidth', 60)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print(f"\n✅ ENCONTRADOS {len(df)} EJEMPLOS SANOS:\n")
        print(df[['confidence', 'image_suggest_category', 'titulo']].to_string(index=False))
    else:
        print("⚠️ INCREÍBLE: No se encontraron registros sanos (esto contradeciría el auditor).")

    conn.close()

if __name__ == "__main__":
    ver_sanos()
