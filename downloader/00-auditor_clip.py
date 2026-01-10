import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def auditar_clip():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("🕵️‍♂️ INICIANDO AUDITORÍA DE DATOS CLIP...")
    print("-" * 60)

    # 1. Total de registros válidos
    cursor.execute("SELECT COUNT(*) FROM downloads WHERE is_valid = 1")
    total_validos = cursor.fetchone()[0]
    print(f"📦 Total imágenes válidas en DB:      {total_validos:,}")

    # 2. Registros con CLIP "Sano" (Tiene confianza Y tiene texto)
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND confidence > 0 
          AND image_suggest_category IS NOT NULL 
          AND image_suggest_category != ''
    """)
    clip_sanos = cursor.fetchone()[0]
    print(f"✅ CLIP Sano (Confianza + Texto):    {clip_sanos:,}")

    # 3. Registros "FANTASMA" (Tiene confianza PERO NO tiene texto)
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND confidence > 0 
          AND (image_suggest_category IS NULL OR image_suggest_category = '')
    """)
    clip_fantasmas = cursor.fetchone()[0]
    print(f"👻 CLIP Fantasma (Confianza sin Texto): {clip_fantasmas:,}")

    # 4. Registros sin datos CLIP (Confianza 0 o null)
    cursor.execute("""
        SELECT COUNT(*) FROM downloads 
        WHERE is_valid = 1 
          AND (confidence IS NULL OR confidence = 0)
    """)
    clip_vacios = cursor.fetchone()[0]
    print(f"⚪ Sin datos CLIP (Aun no procesados): {clip_vacios:,}")
    
    print("-" * 60)

    # 5. MUESTREO DE FANTASMAS (Para ver qué está pasando)
    if clip_fantasmas > 0:
        print("\n🔎 EJEMPLOS DE FANTASMAS (Confianza alta, Categoría vacía):")
        df = pd.read_sql_query("""
            SELECT url_hash, confidence, image_suggest_category, titulo 
            FROM downloads 
            WHERE is_valid = 1 
              AND confidence > 80 
              AND (image_suggest_category IS NULL OR image_suggest_category = '')
            LIMIT 5
        """, conn)
        print(df.to_string(index=False))
        
        print("\n🚨 CONCLUSIÓN PRELIMINAR:")
        if clip_sanos == 0 and clip_fantasmas > 0:
            print("⚠️ EL ERROR ES TOTAL. Ninguna imagen ha guardado la categoría de CLIP.")
            print("   El problema está en el script '02-validador.py' o en la API de validación.")
        else:
            print("⚠️ EL ERROR ES PARCIAL. Algunas imágenes fallaron al etiquetarse.")
    else:
        print("✅ Todo parece correcto. No hay registros fantasma.")

    conn.close()

if __name__ == "__main__":
    auditar_clip()
