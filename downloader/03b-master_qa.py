import sqlite3
import json

# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"

def validar_curacion(limit=20):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Buscamos los últimos registros procesados por el Master Curator
    query = """
        SELECT titulo, categoria, image_suggest_category, categoria_final, atributos, cuerpo_limpio 
        FROM downloads 
        WHERE categoria_final IS NOT NULL 
        ORDER BY rowid DESC 
        LIMIT ?;
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("❌ No se encontraron registros procesados con 'categoria_final'.")
        return

    print(f"🔍 Auditando los últimos {len(rows)} registros...\n")
    print("-" * 100)

    for i, row in enumerate(rows, 1):
        tit, cat_b, cat_c, cat_f, attr, text = row
        
        # 1. Verificación de Limpieza de Atributos
        is_clean = True
        bad_terms = ["SCORE", "BERT_CAT", "CLIP_CAT", "CONFIDENCE", "PUNTUACION"]
        try:
            attr_json = json.loads(attr)
            attr_keys = str(attr_json.keys()).upper()
            if any(term in attr_keys for term in bad_terms):
                is_clean = False
        except:
            is_clean = "ERROR_JSON"

        # 2. Visualización de Resultados
        print(f"PRODUCTO {i}: {tit[:80]}...")
        print(f"  └─ 🏛️ CATEGORÍAS:")
        print(f"     BERT: {cat_b}")
        print(f"     CLIP: {cat_c}")
        print(f"     FINAL: {cat_f}  <-- {'✅ OK' if cat_f in [cat_b, cat_c] else '⚠️ CAMBIO IA'}")
        
        print(f"  └─ ⚙️ ATRIBUTOS (Limpios: {is_clean}):")
        print(f"     {attr}")
        
        print(f"  └─ 📝 TEXTO (SEO):")
        print(f"     {text[:150]}...")
        print("-" * 100)

if __name__ == "__main__":
    validar_curacion(10) # Validamos los últimos 10
