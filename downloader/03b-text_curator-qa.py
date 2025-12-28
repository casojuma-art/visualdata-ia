import sqlite3

DB_PATH = "/lab/visualdata-ia/db/registry.db"
# Palabras que indican que la IA está "charlando" en lugar de procesar
FORBIDDEN_WORDS = ['Lo siento', 'Aquí tienes', 'He eliminado', 'Versión mejorada', '[', ']', 'REGLAS DE LIMPIEZA']

def check_quality():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE cuerpo_limpio IS NOT NULL")
    total = cursor.fetchone()[0]
    
    if total == 0:
        print("No hay registros procesados todavía.")
        return

    errors = 0
    for word in FORBIDDEN_WORDS:
        cursor.execute("SELECT COUNT(*) FROM downloads WHERE cuerpo_limpio LIKE ?", (f'%{word}%',))
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"⚠️ Palabra prohibida '{word}' encontrada en {count} registros.")
            errors += count
            
    success_rate = ((total - errors) / total) * 100
    print(f"\n📊 REGISTROS ANALIZADOS: {total:,}")
    print(f"✅ TASA DE 'VERDAD ABSOLUTA': {success_rate:.2f}%")
    conn.close()

if __name__ == "__main__":
    check_quality()
