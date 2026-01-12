import sqlite3
import json

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"
TARGET_HASH = "5dda6281f4077130903468513083c83c6cef3df2461b512798696dad96eb93d6"

def inspect_full_row():
    print(f"🕵️ AUTOPSIA DEL HASH MALDITO: {TARGET_HASH[:10]}...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM downloads WHERE url_hash = ?", (TARGET_HASH,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        print("❌ No se encontró el registro.")
        return

    print("\n" + "═"*100)
    print(f"📦 TÍTULO: {row['titulo']}")
    print("-" * 100)
    print(f"📂 CATEGORÍA FINAL: {row['categoria_final']}")
    print("-" * 100)
    print(f"📝 DESCRIPCIÓN (Raw):")
    print(row['descripcion'])
    print("-" * 100)
    print(f"📄 CUERPO LIMPIO (Raw):")
    print(row['cuerpo_limpio'])
    print("-" * 100)
    print(f"🛠️ ATRIBUTOS ORIGINALES (Raw):")
    print(row['atributos_originales'])
    print("═"*100 + "\n")

if __name__ == "__main__":
    inspect_full_row()
