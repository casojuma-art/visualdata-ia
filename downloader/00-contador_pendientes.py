import sqlite3

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def contar_estado():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("📊 ESTADO DE CLASIFICACIÓN CLIP (Fantasmas vs Reales)")
    print("=" * 60)

    # 1. Caso Crítico: Válidas pero sin categoría (Los "Fantasmas" que hay que arreglar)
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND (image_suggest_category IS NULL OR image_suggest_category = '')
    """)
    validas_sin_cat = cursor.fetchone()[0]

    # 2. Caso Informativo: Rechazadas sin categoría (Normal, si es mala foto igual ni clasifica)
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 0 
          AND (image_suggest_category IS NULL OR image_suggest_category = '')
    """)
    invalidas_sin_cat = cursor.fetchone()[0]

    # 3. Totales para contexto
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE is_valid = 1")
    total_validas = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE is_valid = 0")
    total_invalidas = cursor.fetchone()[0]

    # Resultados
    print(f"✅ VÁLIDAS (is_valid=1):")
    print(f"   - Total en DB:       {total_validas:,}")
    print(f"   - Con Categoría OK:  {total_validas - validas_sin_cat:,}")
    print(f"   - SIN CATEGORÍA ⚠️:  {validas_sin_cat:,}  <-- ESTAS SON LAS QUE EL VALIDADOR DEBE ARREGLAR")

    print(f"\n❌ RECHAZADAS (is_valid=0):")
    print(f"   - Total en DB:       {total_invalidas:,}")
    print(f"   - Sin Categoría:     {invalidas_sin_cat:,}")

    conn.close()

if __name__ == "__main__":
    contar_estado()
