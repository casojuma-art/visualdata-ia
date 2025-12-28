import sqlite3
import pandas as pd
import os

# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"
OUTPUT_CSV = "/lab/visualdata-ia/metadata/auditoria_fallos_ia.csv"

def extraer_fallos():
    print("🔍 Extrayendo registros con baja confianza (score_ia < 0.6)...")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: DB no encontrada en {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    query = """
        SELECT 
            titulo,
            descripcion as DESC_COMPLETA,
            atributos as ATTR_ORIGINAL,
            categoria as BERT_SUGGEST,
            image_suggest_category as CLIP_SUGGEST,
            categoria_final as IA_DECIDIO,
            score_ia as CONFIANZA_IA
        FROM downloads 
        WHERE score_ia IS NOT NULL AND score_ia < 0.6
        ORDER BY score_ia ASC
        LIMIT 100;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print("⚠️ No hay registros con baja confianza o score_ia no está poblado.")
            return

        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"✅ Auditoría de fallos lista en: {OUTPUT_CSV}")
        print(f"📊 Analizando {len(df)} registros donde la IA dudó.")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    extraer_fallos()
