import sqlite3

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"

def analyze_attributes_coverage():
    print(f"📊 AUDITORÍA DE COBERTURA: 'atributos_originales'")
    print(f"📂 Base de datos: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # 1. Total absoluto de productos
        cursor.execute("SELECT COUNT(*) FROM downloads")
        total_rows = cursor.fetchone()[0]

        # 2. Productos que tienen la fuente "pura" (atributos_originales)
        # Filtramos nulos, cadenas vacías y JSONs vacíos {} o []
        cursor.execute("""
            SELECT COUNT(*) 
            FROM downloads 
            WHERE atributos_originales IS NOT NULL 
              AND atributos_originales != ''
              AND atributos_originales != '{}'
              AND atributos_originales != '[]'
              AND length(atributos_originales) > 2
        """)
        clean_source_rows = cursor.fetchone()[0]

        # 3. Productos que tienen la columna "sucia" rellena (atributos)
        cursor.execute("""
            SELECT COUNT(*) 
            FROM downloads 
            WHERE atributos IS NOT NULL 
              AND atributos != ''
              AND atributos != '{}'
              AND length(atributos) > 2
        """)
        dirty_target_rows = cursor.fetchone()[0]

        conn.close()

        # --- REPORTE ---
        print("\n📈 ESTADÍSTICAS:")
        print(f"   • Total Productos:            {total_rows:,}")
        print(f"   • Con 'atributos_originales': {clean_source_rows:,} ({(clean_source_rows/total_rows)*100:.1f}%) -> FUENTE PURA ✅")
        print(f"   • Con 'atributos' (sucios):   {dirty_target_rows:,} ({(dirty_target_rows/total_rows)*100:.1f}%) -> A SOBRESCRIBIR ⚠️")

        print("\n🧠 CONCLUSIÓN:")
        if clean_source_rows > 0:
            print(f"   Podemos recuperar y limpiar {clean_source_rows:,} productos usando la fuente original.")
            if clean_source_rows < total_rows:
                print("   ⚠️ OJO: Hay productos sin datos originales. Para esos, dependeremos 100% del cuerpo y título.")
        else:
            print("   ❌ MALA NOTICIA: Parece que la columna 'atributos_originales' está vacía masivamente.")

    except Exception as e:
        print(f"❌ Error al conectar: {e}")

if __name__ == "__main__":
    analyze_attributes_coverage()
