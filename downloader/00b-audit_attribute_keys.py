import sqlite3
import json
import os
from collections import Counter

# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"

def audit_keys():
    print(f"🕵️‍♂️ Auditando claves de atributos en: {DB_PATH}")
    
    if not os.path.exists(DB_PATH):
        print("❌ Error: No se encuentra la BBDD.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Intentamos leer de 'atributos_originales' (backup)
    col_name = "atributos_originales"
    try:
        cursor.execute(f"SELECT {col_name} FROM downloads WHERE is_valid=1 LIMIT 1")
        cursor.fetchone()
    except:
        print("⚠️  Columna 'atributos_originales' no detectada. Usando 'atributos'.")
        col_name = "atributos"

    print(f"📂 Analizando columna fuente: '{col_name}'...")
    
    cursor.execute(f"SELECT {col_name} FROM downloads WHERE is_valid=1")
    rows = cursor.fetchall()
    conn.close()

    total_registros = len(rows)
    key_counter = Counter()
    example_values = {} 
    
    empty_count = 0
    error_count = 0

    print(f"📊 Procesando {total_registros:,} registros...")

    for row in rows:
        raw_json = row[0]
        if not raw_json or raw_json == '{}':
            empty_count += 1
            continue
            
        try:
            data = json.loads(raw_json)
            if isinstance(data, dict):
                # Filtramos claves basura técnicas
                keys = [k for k in data.keys() if k not in ["clave", "valor"]]
                key_counter.update(keys)
                
                # Guardamos un ejemplo visual
                for k, v in data.items():
                    if k not in example_values and v:
                        example_values[k] = str(v)[:40] 
        except:
            error_count += 1

    # RESULTADOS
    print("\n" + "="*100)
    print(f"TOP 200 ATRIBUTOS MÁS FRECUENTES ({col_name})")
    print("="*100)
    print(f"{'CLAVE (Original)':<40} | {'FREQ':<8} | {'% APARICIÓN':<12} | {'EJEMPLO VALOR'}")
    print("-" * 100)

    for key, count in key_counter.most_common(200):
        percent = (count / total_registros) * 100
        example = example_values.get(key, "")
        print(f"{key:<40} | {count:<8} | {percent:6.2f}%      | {example}")

    print("-" * 100)

if __name__ == "__main__":
    audit_keys()
