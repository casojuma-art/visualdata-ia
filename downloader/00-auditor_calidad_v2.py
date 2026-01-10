import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def auditar_limpieza():
    conn = sqlite3.connect(DB_PATH)
    
    print("\n🛡️ AUDITORÍA DE CALIDAD POST-AJUSTE (Umbral Bad: 0.60 | Match: 0.22)")
    print("=" * 90)

    # 1. ¿Cuántas estamos rechazando ahora?
    query_rechazos = "SELECT COUNT(*) FROM downloads WHERE is_valid = 0"
    total_rechazos = pd.read_sql_query(query_rechazos, conn).iloc[0,0]
    
    # 2. Ver las últimas rechazadas para confirmar que son "basura"
    print(f"\n❌ TOTAL RECHAZADAS ACTUALMENTE: {total_rechazos}")
    print("\n🔍 ÚLTIMOS RECHAZOS (Confirmar si son logos/ruido):")
    query_ultimos_rechazos = """
        SELECT url_hash, score_product, score_watermark, score_placeholder, score_quality
        FROM downloads 
        WHERE is_valid = 0 
        ORDER BY rowid DESC 
        LIMIT 5
    """
    df_r = pd.read_sql_query(query_ultimos_rechazos, conn)
    print(df_r.to_string(index=False))

    # 3. Ver las aceptadas con score de watermark al límite (0.5 - 0.6)
    print("\n⚠️ ALERTAS: ACEPTADAS EN EL LÍMITE (0.5 < Watermark < 0.6):")
    query_alertas = """
        SELECT url_hash, score_watermark, image_suggest_category
        FROM downloads 
        WHERE is_valid = 1 AND score_watermark > 0.5
        LIMIT 5
    """
    df_a = pd.read_sql_query(query_alertas, conn)
    if df_a.empty:
        print("✅ No hay imágenes sospechosas en el nuevo umbral.")
    else:
        print(df_a.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    auditar_limpieza()
