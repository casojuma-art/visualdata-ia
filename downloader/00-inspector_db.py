import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def inspect():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Ver nombres de columnas
    print("🔍 ESTRUCTURA DE LA TABLA 'downloads':")
    cursor.execute("PRAGMA table_info(downloads)")
    cols = cursor.fetchall()
    col_names = [c[1] for c in cols]
    print(col_names)
    
    # 2. Ver una fila que tenga confianza alta de CLIP para ver dónde está el texto
    print("\n🔍 MUESTRA DE DATOS (Fila con confidence > 90):")
    try:
        # Intentamos sacar todo para ver qué columna tiene el texto de CLIP
        df = pd.read_sql_query("SELECT * FROM downloads WHERE confidence > 90 LIMIT 1", conn)
        if not df.empty:
            row = df.iloc[0]
            for col in col_names:
                print(f"  👉 {col}: {row[col]}")
        else:
            print("⚠️ No encontré filas con confidence > 90. Muestro cualquiera:")
            df = pd.read_sql_query("SELECT * FROM downloads LIMIT 1", conn)
            print(df.iloc[0])
            
    except Exception as e:
        print(f"Error leyendo datos: {e}")
        
    conn.close()

if __name__ == "__main__":
    inspect()
