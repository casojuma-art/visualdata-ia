import sqlite3
import pandas as pd

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def analizar_jerarquia():
    print("🔍 [QA] Analizando Coherencia Jerárquica (L1, L2, L3, L4)...")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT categoria, image_suggest_category FROM downloads WHERE is_valid = 1"
    df = pd.read_sql_query(query, conn)
    conn.close()

    total = len(df)
    if total == 0: return

    def get_level_match(row, level):
        try:
            # Dividimos las ramas por el separador de tu taxonomía
            p_bert = row['categoria'].split(' > ')
            p_clip = row['image_suggest_category'].split(' > ')
            # Comparamos hasta el nivel solicitado
            return p_bert[:level] == p_clip[:level]
        except: return False

    match_l1 = df.apply(lambda r: get_level_match(r, 1), axis=1).sum()
    match_l2 = df.apply(lambda r: get_level_match(r, 2), axis=1).sum()
    match_l3 = df.apply(lambda r: get_level_match(r, 3), axis=1).sum()
    match_l4 = df.apply(lambda r: get_level_match(r, 4), axis=1).sum()

    print(f"\n📊 COHERENCIA POR NIVELES (N=7,000 hojas):")
    print(f"Total registros: {total:,}")
    print(f"✅ Nivel 1 (Sector):    {match_l1:,} ({ (match_l1/total)*100:.2f}%)")
    print(f"✅ Nivel 2 (Grupo):     {match_l2:,} ({ (match_l2/total)*100:.2f}%)")
    print(f"✅ Nivel 3 (Familia):   {match_l3:,} ({ (match_l3/total)*100:.2f}%)")
    print(f"✅ Nivel 4 (Producto):  {match_l4:,} ({ (match_l4/total)*100:.2f}%)")
    
    # Ejemplo de "Fallo Útil": coinciden en L2 pero no en L4
    fallo_util = df[df.apply(lambda r: get_level_match(r, 2) and not get_level_match(r, 4), axis=1)]
    if not fallo_util.empty:
        print("\n🧐 EJEMPLOS DE DISCREPANCIA ACEPTABLE (Coinciden en L2):")
        print(fallo_util.head(5).to_string(index=False))

if __name__ == "__main__":
    analizar_jerarquia()
