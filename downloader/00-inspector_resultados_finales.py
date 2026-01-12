import sqlite3
import json

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"

def inspect_production_results():
    print(f"🕵️ INSPECTOR DE CALIDAD FINAL (V40)...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Buscamos 5 productos YA PROCESADOS (atributos no es nulo)
        cursor.execute("""
            SELECT url_hash, titulo, atributos_originales, atributos, cuerpo_limpio 
            FROM downloads 
            WHERE atributos IS NOT NULL 
              AND length(atributos) > 20
            ORDER BY RANDOM()
            LIMIT 5
        """)
        rows = cursor.fetchall()
        
        # También buscamos específicamente el "hash maldito" si ya ha sido procesado
        cursed_hash = "5dda6281f4077130903468513083c83c6cef3df2461b512798696dad96eb93d6"
        cursor.execute("SELECT url_hash, titulo, atributos_originales, atributos FROM downloads WHERE url_hash = ?", (cursed_hash,))
        cursed_row = cursor.fetchone()

        all_rows = rows + ([cursed_row] if cursed_row and cursed_row['atributos'] else [])
        conn.close()

        if not all_rows:
            print("⚠️ Aún no veo registros procesados. Espera unos segundos más.")
            return

        for row in all_rows:
            print("\n" + "═"*100)
            print(f"📦 TÍTULO: {row['titulo']}")
            if row['url_hash'] == cursed_hash:
                print("💀 (ESTE ERA EL PRODUCTO QUE DABA ERROR ANTES)")
            print("-" * 100)
            
            # INPUT
            raw_orig = row['atributos_originales']
            print(f"📥 INPUT CSV: {raw_orig if raw_orig else '(VACÍO)'}")
            
            # OUTPUT
            print("-" * 100)
            try:
                # Intentamos formatear el JSON bonito
                final_json = json.loads(row['atributos'])
                
                # Chequeo rápido de si es un error marcado
                if "status" in final_json and final_json["status"] == "error_json":
                    print(f"⚠️ ESTADO: ERROR MARCADO EN BBDD")
                    print(f"   Motivo: {final_json.get('motivo')}")
                else:
                    print(f"✅ OUTPUT V40 (JSON LIMPIO):")
                    print(json.dumps(final_json, indent=2, ensure_ascii=False))
            except:
                print(f"❌ ERROR DE FORMATO EN BBDD (Raw): {row['atributos']}")
            
            print("═"*100)

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    inspect_production_results()
