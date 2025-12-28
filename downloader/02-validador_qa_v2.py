import sqlite3
import pandas as pd
import sys

# Ajusta la ruta si lo corres desde fuera o dentro del docker
# Si es desde HOST (fuera):
#DB_PATH = "/lab/developer/docker/visualdata-ia/db/registry.db"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
# Si es desde DOCKER (dentro de vi-downloader):
# DB_PATH = "/lab/visualdata-ia/db/registry.db"

def analizar_qa_v2():
    print("🔍 [QA V2] Conectando a la base de datos...")
    
    try:
        # Usamos timeout para no bloquear al proceso principal que está escribiendo
        conn = sqlite3.connect(DB_PATH, timeout=10)
        
        # Leemos solo las procesadas (is_valid no es nulo)
        query = """
        SELECT 
            titulo, 
            categoria as cat_bert, 
            image_suggest_category as cat_vision, 
            confidence,
            score_category
        FROM downloads 
        WHERE is_valid IS NOT NULL
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
    except Exception as e:
        print(f"❌ Error leyendo DB (quizás ruta incorrecta?): {e}")
        return

    total = len(df)
    if total == 0:
        print("📭 Aún no hay registros procesados en la V2.")
        return

    # --- MÉTRICAS V2 ---
    # 1. Identificar Silenciadas (cat_vision está vacío o None)
    # En V2 guardamos "" cuando la confianza es baja.
    silenciadas = df[ (df['cat_vision'].isna()) | (df['cat_vision'] == "") ]
    sugeridas = df[ (df['cat_vision'].notna()) & (df['cat_vision'] != "") ]

    n_silenciadas = len(silenciadas)
    n_sugeridas = len(sugeridas)

    print(f"\n📊 ESTADO DEL PROCESO (N={total:,} procesadas)")
    print("=" * 60)
    print(f"🤫 SILENCIADAS (Anti-Alucinación): {n_silenciadas:,} ({ (n_silenciadas/total)*100:.1f}%)")
    print(f"   -> La IA dudó (< 0.28) y prefirió no opinar. (Esto es BUENO para bombas de calor, etc)")
    print(f"🗣️  SUGERIDAS (Alta Confianza):    {n_sugeridas:,} ({ (n_sugeridas/total)*100:.1f}%)")
    print(f"   -> La IA lo vio claro (> 0.28).")
    print("=" * 60)

    # --- ANÁLISIS DE LAS SUGERIDAS (¿Tiene razón cuando habla?) ---
    if n_sugeridas > 0:
        def get_level_match(row, level):
            try:
                p_bert = row['cat_bert'].split(' > ')
                p_clip = row['cat_vision'].split(' > ')
                # Normalizamos strings por si acaso
                return p_bert[:level] == p_clip[:level]
            except: return False

        # Copia para no alterar el original
        df_sug = sugeridas.copy()
        
        match_l1 = df_sug.apply(lambda r: get_level_match(r, 1), axis=1).sum()
        match_l2 = df_sug.apply(lambda r: get_level_match(r, 2), axis=1).sum()
        match_l3 = df_sug.apply(lambda r: get_level_match(r, 3), axis=1).sum()

        print(f"\n🎯 PRECISIÓN DE LAS SUGERENCIAS (Sobre las {n_sugeridas:,} habladas):")
        print(f"✅ Coincidencia Nivel 1 (Sector):  {match_l1:,} ({ (match_l1/n_sugeridas)*100:.1f}%)")
        print(f"✅ Coincidencia Nivel 2 (Familia): {match_l2:,} ({ (match_l2/n_sugeridas)*100:.1f}%)")
        print(f"✅ Coincidencia Nivel 3 (Subfam):  {match_l3:,} ({ (match_l3/n_sugeridas)*100:.1f}%)")

        print("\n🏆 EJEMPLOS DE ÉXITO (Vision coincide con Texto):")
        exitos = df_sug[df_sug.apply(lambda r: get_level_match(r, 3), axis=1)].head(3)
        for _, row in exitos.iterrows():
             print(f"   PRODUCTO: {row['titulo'][:50]}...")
             print(f"   ✅ AMBOS: {row['cat_vision']}")
             print("-" * 20)

    # --- EJEMPLOS DE SILENCIO (Para ver qué estamos filtrando) ---
    if n_silenciadas > 0:
        print("\n🛡️  EJEMPLOS DE SILENCIO (Evitamos alucinación):")
        print("   (Estos productos tenían score < 0.28)")
        muestras_silencio = silenciadas.sample(min(5, n_silenciadas))
        for _, row in muestras_silencio.iterrows():
            print(f"   PRODUCTO: {row['titulo'][:60]}")
            print(f"   CONFIA.:  {row['score_category']:.4f} (Bajo umbral)")
            print("-" * 20)

if __name__ == "__main__":
    analizar_qa_v2()
