import sqlite3
import json

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"
# Usamos el hash de la Medalla de Oro que vimos antes para comparar
TARGET_HASH = "588991415e6c1eee9f2748fffa0c4faf33b06455c4b8d0414fb8f64ab183ba7c"

def inspect_columns():
    print(f"🔍 INSPECTOR FORENSE DE BASE DE DATOS")
    print(f"📂 Conectando a: {DB_PATH}")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Obtener nombres reales de las columnas
        cursor.execute("PRAGMA table_info(downloads)")
        columns_info = cursor.fetchall()
        all_columns = [col['name'] for col in columns_info]
        
        print(f"\n📋 TABLA 'downloads' TIENE {len(all_columns)} COLUMNAS:")
        print(f"{all_columns}")

        # 2. Extraer todos los datos del registro objetivo
        cursor.execute("SELECT * FROM downloads WHERE url_hash = ?", (TARGET_HASH,))
        row = cursor.fetchone()

        if row:
            print(f"\n📦 DATOS COMPLETOS PARA EL HASH: {TARGET_HASH[:8]}...")
            print("="*60)
            for col in all_columns:
                val = row[col]
                # Mostramos el contenido, recortando si es muy largo para facilitar lectura
                if isinstance(val, str) and len(val) > 150:
                    val_display = f"{val[:150]}... [Total: {len(val)} chars]"
                else:
                    val_display = val
                
                # Resaltamos las columnas que parecen contener atributos
                prefix = "  👉 "
                if "tribut" in col.lower() or "json" in col.lower() or "feed" in col.lower():
                    prefix = "  🔥 " 
                
                print(f"{prefix}{col}: {val_display}")
            print("="*60)
        else:
            print("❌ No se encontró el registro con ese hash.")

        conn.close()

    except Exception as e:
        print(f"❌ Error crítico: {e}")

if __name__ == "__main__":
    inspect_columns()
