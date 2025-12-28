import sqlite3
import json

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def validar_registros(limit=100):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Recuperamos toda la evidencia y el resultado final
    query = """
        SELECT 
            titulo, categoria, image_suggest_category, score_product, 
            categoria_final, atributos, cuerpo_limpio 
        FROM downloads 
        WHERE categoria_final IS NOT NULL 
        ORDER BY rowid DESC 
        LIMIT ?;
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("❌ No hay registros curados. Asegúrate de que 03b-master_curator.py esté corriendo.")
        return

    print(f"📊 [QA V3] Validando los últimos {len(rows)} registros curados...\n")
    
    for i, row in enumerate(rows, 1):
        tit, cat_b, cat_c, s_prod, cat_f, attr, text = row
        
        # Verificación de "Meta-charla"
        has_meta = any(word in text.lower() for word in ["por lo tanto", "nota:", "punto clave", "seo-neutral"])
        
        # Verificación de Arbitraje
        arbitraje = "✅ IA (Propio/Fuzzy)"
        if cat_f == cat_b: arbitraje = "🏛️ Prioridad BERT"
        elif cat_f == cat_c: arbitraje = "👁️ Prioridad CLIP"

        print(f"{'='*100}")
        print(f"PRODUCTO {i}: {tit}")
        print(f"{'-'*100}")
        print(f"🏛️ BERT: {cat_b}")
        print(f"👁️ CLIP: {cat_c} (Confianza: {s_prod:.4f})")
        print(f"🎯 FINAL: {cat_f} [{arbitraje}]")
        print(f"{'-'*100}")
        
        print(f"📝 TEXTO CURADO (Limpieza Meta-charla: {'❌ FALLO' if has_meta else '✅ OK'}):")
        # Mostramos los últimos 150 caracteres para verificar el final del texto
        print(f"...{text[-300:] if len(text) > 300 else text}")
        
        print(f"\n⚙️ ATRIBUTOS:")
        print(f"{attr}")
        print(f"{'='*100}\n")

if __name__ == "__main__":
    validar_registros(10)
