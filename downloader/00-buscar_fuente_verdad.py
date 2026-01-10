import sqlite3
import json

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"

def find_real_source():
    print(f"🕵️ BUSCANDO DATOS CRUDOS EN 'atributos_originales'...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Buscamos un registro que NO tenga el JSON vacío ni sea nulo
        # Y que tampoco sea el string "{}"
        cursor.execute("""
            SELECT url_hash, titulo, atributos, atributos_originales 
            FROM downloads 
            WHERE atributos_originales IS NOT NULL 
              AND length(atributos_originales) > 5
              AND atributos_originales != '{}'
              AND atributos_originales != '[]'
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            print(f"\n✅ ¡ENCONTRADO REGISTRO CON DATOS ORIGINALES!")
            print(f"   Hash: {row['url_hash']}")
            print(f"   Título: {row['titulo']}")
            print("-" * 60)
            print(f"📝 CONTENIDO DE 'atributos_originales' (FUENTE PURA):")
            print(row['atributos_originales'])
            print("-" * 60)
            print(f"🗑️ CONTENIDO DE 'atributos' (A SOBRESCRIBIR):")
            print(f"{str(row['atributos'])[:200]}...") 
        else:
            print(f"\n⚠️ ALERTA: No se encontró NINGÚN registro con 'atributos_originales' relleno.")
            print("   Posibles causas:")
            print("   1. La columna se borró o nunca se migró.")
            print("   2. Los datos originales están en otra columna (quizás 'raw_data' o similar? no vimos ninguna en el inspector).")
            print("   3. Solo tenemos título y cuerpo para trabajar.")

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    find_real_source()
