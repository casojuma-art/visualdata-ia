import sqlite3
import pandas as pd
import os

# Rutas estándar del sistema [cite: 8, 30]
TAXONOMY_PATH = "/lab/visualdata-ia/metadata/gpc_id_to_path.csv"
DB_PATH = "/lab/visualdata-ia/db/registry.db"

# 1. Cargar el mapeo de IDs de Google Merchant
if not os.path.exists(TAXONOMY_PATH):
    print(f"ERROR: No se encuentra el archivo en {TAXONOMY_PATH}")
    exit(1)

df_gpc = pd.read_csv(TAXONOMY_PATH)
df_gpc['path_clean'] = df_gpc['path'].str.strip()

# 2. Extraer estadísticas completas por categoría
try:
    conn = sqlite3.connect(DB_PATH)
    # Contamos el total de elementos y los que tienen problemas (< 0.7)
    query = """
    SELECT 
        categoria, 
        COUNT(*) as total_elementos,
        SUM(CASE WHEN score_ia < 0.7 THEN 1 ELSE 0 END) as total_dudosos,
        AVG(score_ia) as media_score
    FROM downloads 
    WHERE curator_attempts > 0 
    GROUP BY categoria
    HAVING total_dudosos > 0
    ORDER BY total_dudosos DESC;
    """
    df_stats = pd.read_sql_query(query, conn)
    conn.close()
except Exception as e:
    print(f"Error al acceder a la base de datos: {e}")
    exit(1)

# 3. Cruzar con la taxonomía
df_stats['categoria_clean'] = df_stats['categoria'].str.strip()
df_final = pd.merge(
    df_stats, 
    df_gpc, 
    left_on='categoria_clean', 
    right_on='path_clean', 
    how='left'
)

# 4. Formatear y mostrar el resultado
print(f"{'GPC_ID':<8} | {'MEDIA':<7} | {'DUDOSOS':<8} | {'TOTAL':<8} | {'% ERROR':<7} | {'CATEGORIA'}")
print("-" * 120)

for _, row in df_final.iterrows():
    gpc_id = int(row['id']) if pd.notnull(row['id']) else "N/A"
    media = f"{row['media_score']:.3f}"
    
    # Cálculo del porcentaje de error sobre el total de esa categoría
    pct_error = (row['total_dudosos'] / row['total_elementos']) * 100
    
    print(f"{gpc_id:<8} | {media:<7} | {row['total_dudosos']:<8} | {row['total_elementos']:<8} | {pct_error:>6.1f}% | {row['categoria']}")
