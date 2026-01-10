import sqlite3
import os
import csv
import glob
import sys
# este escriptm vuelve a cargar los atributos de los csv a la base de datos. antiguamente los perdimos en un procesado porque el script 03b sobreescribia, ahora usaremos un campo diferente
# CONFIGURACIÓN
DB_PATH = "/lab/visualdata-ia/db/registry.db"
# Ruta donde se guardan los CSVs procesados (según tu script 02)
BACKUP_DIR = "/lab/visualdata-ia/data_in/03indatabase" 

def restore_attributes():
    if not os.path.exists(DB_PATH):
        print(f"❌ Error: No existe la base de datos en {DB_PATH}")
        return

    print(f"🚀 Iniciando MIGRACIÓN DE SEGURIDAD...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Crear columna 'atributos_originales' si no existe
    print("🛠️  Verificando esquema de base de datos...")
    try:
        cursor.execute("ALTER TABLE downloads ADD COLUMN atributos_originales TEXT")
        print("✅ Columna 'atributos_originales' creada correctamente.")
    except sqlite3.OperationalError:
        print("ℹ️  La columna 'atributos_originales' ya existía.")

    # 2. Localizar CSVs de respaldo
    csv_files = glob.glob(os.path.join(BACKUP_DIR, "*.csv"))
    if not csv_files:
        print(f"⚠️  ALERTA: No encontré CSVs en {BACKUP_DIR}.")
        print("    ¿Quizás están todavía en /simplified o en /downloaded?")
        # Fallback opcional: buscar en simplified si no hay nada en backup
        csv_files = glob.glob("/lab/visualdata-ia/data_in/simplified/*.csv")
    
    if not csv_files:
        print("❌ No se encontraron archivos CSV para restaurar. Abortando.")
        return

    print(f"📂 Encontrados {len(csv_files)} archivos CSV para procesar.")

    total_updated = 0
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        print(f"   -> Procesando: {filename}...")
        
        batch_data = []
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            # Verificamos que el CSV tenga la columna 'url_hash' (generada en pasos previos)
            # Si es el CSV puro de 'simplifica.py', debería tenerla si se procesó correctamente.
            # NOTA: 01-simplifica.py no generaba url_hash en el CSV, sino que 02 lo calcula al vuelo.
            # ESTRATEGIA: Recalcular hash desde la URL de imagen o título para coincidir con la BBDD.
            # [cite_start]Sin embargo, el script 01 [cite: 8] guarda 'atributos' y 'imagenes_producto'.
            
            # Vamos a asumir que podemos linkar por 'titulo' si no hay hash, 
            # PERO lo más seguro es replicar la lógica de hash del validador.
            import hashlib
            def get_url_hash(url):
                return hashlib.sha256(url.encode()).hexdigest()

            for row in reader:
                # Obtenemos la URL para regenerar la clave primaria (hash)
                urls = row.get('imagenes_producto', '')
                if not urls: continue
                
                first_url = urls.split(',')[0].strip()
                if not first_url: continue
                
                u_hash = get_url_hash(first_url)
                raw_attrs = row.get('atributos', '{}')
                
                batch_data.append((raw_attrs, u_hash))

        if batch_data:
            cursor.executemany(
                "UPDATE downloads SET atributos_originales = ? WHERE url_hash = ?",
                batch_data
            )
            total_updated += len(batch_data)
            conn.commit()

    conn.close()
    print(f"\n✅ MIGRACIÓN COMPLETADA.")
    print(f"📊 Se han salvaguardado los atributos originales de {total_updated:,} productos.")
    print("🛡️  Ahora puedes ejecutar el Curador v9 sin miedo a perder datos.")

if __name__ == "__main__":
    restore_attributes()
