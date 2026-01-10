import sqlite3
import json
import os
from collections import Counter

DB_PATH = "/lab/visualdata-ia/db/registry.db"

# Estas son las claves que detectamos en el paso anterior
KEYS_SEXO = ["genero", "Genero", "sexo", "gender", "sex"]
KEYS_EDAD = ["Edad", "edad", "edad-recomendada", "grupo de edad", "age", "rango_edad"]

def inspect_values():
    print(f"🕵️‍♂️ Inspeccionando valores para SEXO y EDAD en: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Intentamos leer de la copia de seguridad, si no de la normal
    col = "atributos_originales"
    try:
        cursor.execute(f"SELECT {col} FROM downloads WHERE is_valid=1")
    except:
        col = "atributos"
        cursor.execute(f"SELECT {col} FROM downloads WHERE is_valid=1")

    rows = cursor.fetchall()
    conn.close()

    val_sexo = Counter()
    val_edad = Counter()
    
    total_found_sexo = 0
    total_found_edad = 0

    print(f"📊 Analizando {len(rows):,} registros...")

    for row in rows:
        raw = row[0]
        if not raw or raw == '{}': continue
        
        try:
            data = json.loads(raw)
            if not isinstance(data, dict): continue
            
            # Buscar valores de SEXO
            for k in KEYS_SEXO:
                if k in data and data[k]:
                    val = str(data[k]).strip()
                    val_sexo[val] += 1
                    total_found_sexo += 1
                    break # Solo cogemos el primero que aparezca por prioridad

            # Buscar valores de EDAD
            for k in KEYS_EDAD:
                if k in data and data[k]:
                    val = str(data[k]).strip()
                    val_edad[val] += 1
                    total_found_edad += 1
                    break

        except:
            continue

    # --- IMPRIMIR RESULTADOS ---
    print("\n" + "="*60)
    print(f"🚻 VALORES DE SEXO ENCONTRADOS (Total: {total_found_sexo})")
    print("="*60)
    for val, count in val_sexo.most_common(30):
        print(f"{val:<30} | {count}")

    print("\n" + "="*60)
    print(f"👶 VALORES DE EDAD ENCONTRADOS (Total: {total_found_edad})")
    print("="*60)
    for val, count in val_edad.most_common(30):
        print(f"{val:<30} | {count}")

if __name__ == "__main__":
    inspect_values()
