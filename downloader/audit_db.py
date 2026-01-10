import sqlite3
import json
import os

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def audit_attributes():
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: No encuentro la base de datos en {DB_PATH}")
        return

    print(f"🕵️‍♂️ Auditando atributos en: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Buscamos registros 'Válidos' (Score IA alto) para ver qué calidad tienen
    cursor.execute("""
        SELECT atributos 
        FROM downloads 
        WHERE is_valid = 1 
          AND score_ia >= 0.95
    """)
    rows = cursor.fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print("⚠️ No hay registros con score_ia >= 0.95 para auditar.")
        return

    vacios = 0
    basura = 0  # Ej: {"clave": "valor"}
    buenos = 0
    ejemplos_buenos = []

    print(f"📊 Analizando {total} registros candidatos...")

    for row in rows:
        raw_attr = row[0]
        try:
            attr_dict = json.loads(raw_attr) if raw_attr else {}
        except:
            attr_dict = {}

        # 1. Chequeo de Vacío
        if not attr_dict:
            vacios += 1
            continue
        
        # 2. Chequeo de Basura (Placeholder común)
        # A veces se guarda como string '{"clave": "valor"}' literal
        str_val = str(attr_dict).lower()
        if "clave" in str_val and "valor" in str_val:
            basura += 1
            continue
        
        # 3. Son buenos (tienen algo distinto a vacío o basura)
        buenos += 1
        if len(ejemplos_buenos) < 5:
            ejemplos_buenos.append(attr_dict)

    print("\n" + "="*30)
    print("RESULTADOS DE LA AUDITORÍA")
    print("="*30)
    # Aquí estaba el error, ya corregido:
    print(f"🔴 Vacíos:             {vacios} ({vacios/total*100:.1f}%)")
    print(f"🟠 Basura (Placeholder): {basura} ({basura/total*100:.1f}%)")
    print(f"🟢 Útiles (Con datos):   {buenos} ({buenos/total*100:.1f}%)")
    print("="*30)
    
    if ejemplos_buenos:
        print("\nEjemplos de atributos BUENOS encontrados:")
        for ex in ejemplos_buenos:
            print(f" - {json.dumps(ex, ensure_ascii=False)}")
    else:
        print("\n❌ NO SE ENCONTRARON ATRIBUTOS BUENOS.")

if __name__ == "__main__":
    audit_attributes()
