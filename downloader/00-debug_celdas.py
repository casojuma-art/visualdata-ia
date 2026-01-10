import sqlite3

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def analizar_vacios():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🔍 ANALIZANDO REGISTROS VÁLIDOS SIN CATEGORÍA...")
    
    # Buscamos 5 casos donde no se vea categoría
    cursor.execute("""
        SELECT url_hash, image_suggest_category 
        FROM downloads 
        WHERE is_valid = 1 
          AND (image_suggest_category IS NULL OR image_suggest_category = '')
        LIMIT 5
    """)
    
    rows = cursor.fetchall()
    
    if not rows:
        print("✅ No hay registros vacíos. Todo correcto.")
    else:
        print(f"⚠️ Encontrados {len(rows)} ejemplos sospechosos. Analizando contenido exacto:")
        for hash_id, cat in rows:
            tipo = type(cat)
            contenido_repr = repr(cat) # Esto muestra caracteres ocultos
            print(f"   Hash: {hash_id[:8]}... | Tipo: {tipo} | Contenido exacto: {contenido_repr}")

    conn.close()

if __name__ == "__main__":
    analizar_vacios()
