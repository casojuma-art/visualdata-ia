import sqlite3
import pandas as pd
import os
import time
from datetime import datetime

# Configuración
DB_PATH = "/lab/visualdata-ia/db/registry.db"
IMG_BASE_PATH = "/lab/visualdata-ia/imagenes_in"
OUTPUT_DIR = "/lab/visualdata-ia/data_preparada"
BATCH_SIZE = 1000 # Verás una actualización cada 1000 líneas

def llamar_llm_refinado(titulo, categoria):
    # Aquí irá tu lógica de conexión a la API del LLM (Ollama, GPT, etc.)
    # Por ahora simulamos el retorno limpio
    return f"{titulo} especializado en la categoría {categoria}"

def preparar_entreno():
    start_time = time.time()
    print(f"[{datetime.now()}] 🚀 Iniciando Fase 3-a: Preparación Masiva...")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    conn = sqlite3.connect(DB_PATH)
    # Según readme: url_hash es PK [cite: 16], is_valid filtra [cite: 19]
    query = "SELECT url_hash, titulo, categoria FROM downloads WHERE is_valid = 1"
    
    print("📥 Cargando datos de SQLite (esto puede tardar)...")
    df = pd.read_sql_query(query, conn)
    conn.close()

    total_db = len(df)
    print(f"📊 Registros encontrados: {total_db}")

    registros_preparados = []
    
    print(f"🔍 Verificando imágenes y refinando con LLM (Batch size: {BATCH_SIZE})...")
    
    for i, row in df.iterrows():
        # 1. Verificar ruta física /aa/bb/hash.jpg
        u_hash = row['url_hash']
        path_img = os.path.join(IMG_BASE_PATH, u_hash[:2], u_hash[2:4], f"{u_hash}.jpg")
        
        if os.path.exists(path_img):
            # 2. Refinado con LLM (Opcional: puedes activar esto por lotes)
            # texto_limpio = llamar_llm_refinado(row['titulo'], row['categoria'])
            
            registros_preparados.append({
                'url_hash': u_hash,
                'path_local': path_img,
                'label': row['categoria'],
                'text_input': row['titulo'] # Aquí iría el texto ya refinado
            })

        # Feedback visual cada BATCH_SIZE
        if i % BATCH_SIZE == 0 and i > 0:
            porcentaje = (i / total_db) * 100
            print(f"⏳ Procesado: {i}/{total_db} ({porcentaje:.2f}%) - OK: {len(registros_preparados)}")

    # Guardar en CSV para el entrenamiento
    df_final = pd.DataFrame(registros_preparados)
    output_path = os.path.join(OUTPUT_DIR, "dataset_final_train.csv")
    df_final.to_csv(output_path, index=False, sep=';')

    end_time = time.time()
    duracion = (end_time - start_time) / 60
    print(f"\n✅ PROCESO FINALIZADO")
    print(f"⏱ Tiempo total: {duracion:.2f} minutos")
    print(f"📂 Archivo generado: {output_path} ({len(df_final)} líneas)")

if __name__ == "__main__":
    preparar_entreno()
