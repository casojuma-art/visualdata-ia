import sqlite3
import pandas as pd
import os

# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"
OUTPUT_CSV = "/lab/visualdata-ia/metadata/auditoria_300_deep.csv"

def extraer_auditoria_completa():
    print("🎲 Generando auditoría profunda (300 muestras) con contexto total...")
    
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: DB no encontrada en {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    
    # Query que unifica datos de entrada y salida curada
    query = """
        SELECT 
            url_hash,
            titulo as TITULO_ORIGINAL,
            descripcion as DESC_ORIGINAL,
            categoria as BERT_SUGGEST,
            image_suggest_category as CLIP_SUGGEST,
            score_product as CLIP_CONF,
            categoria_final as IA_DECISION_FINAL,
            atributos as ATRIBUTOS_CURADOS,
            cuerpo_limpio as TEXTO_SEO_FINAL
        FROM downloads 
        WHERE categoria_final IS NOT NULL 
        ORDER BY RANDOM() 
        LIMIT 300;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            print("⚠️ No hay datos curados. Asegúrate de que 03b-master_curator.py esté trabajando.")
            return

        # Asegurar directorio
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

        # Exportación
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"✅ Auditoría lista en: {OUTPUT_CSV}")
        print(f"📊 Registros listos para valorar: {len(df)}")
        
    except Exception as e:
        print(f"❌ Error en la extracción: {e}")
        if conn: conn.close()

if __name__ == "__main__":
    extraer_auditoria_completa()
