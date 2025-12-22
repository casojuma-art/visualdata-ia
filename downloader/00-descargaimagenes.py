import pandas as pd
import sqlite3
import hashlib
import httpx
import asyncio
import os
from pathlib import Path
import shutil

# CONFIGURACIÓN DE RUTAS
BASE_DIR = Path("/lab/visualdata-ia")
INBOX_DIR = BASE_DIR / "data_in/inbox"
PROCESSED_DIR = BASE_DIR / "data_in/downloaded"
DB_PATH = BASE_DIR / "db/registry.db"
IMG_BASE_DIR = BASE_DIR / "imagenes_in"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            url_hash TEXT PRIMARY KEY,
            url TEXT,
            status TEXT
        )
    """)
    conn.commit()
    conn.close()

async def descargar_imagen(client, url, url_hash, semaphore):
    img_path = IMG_BASE_DIR / url_hash[:2] / url_hash[2:4] / f"{url_hash}.jpg"
    
    if img_path.exists():
        return True

    async with semaphore: # Aquí limitamos a 5 simultáneas
        try:
            resp = await client.get(url, timeout=15.0, follow_redirects=True)
            if resp.status_code == 200:
                img_path.parent.mkdir(parents=True, exist_ok=True)
                with open(img_path, "wb") as f:
                    f.write(resp.content)
                return True
        except Exception:
            return False
    return False

async def procesar_csv(file_path):
    print(f"\n>>> PROCESANDO: {file_path.name}")
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()
        sep = ';' if ';' in first_line else ','

    df = pd.read_csv(file_path, sep=sep, low_memory=False)
    col_img = 'imagenes_producto' if 'imagenes_producto' in df.columns else 'URL'
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Límite de 5 descargas simultáneas
    semaphore = asyncio.Semaphore(5)
    
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=5)) as client:
        tareas = []
        for i, row in df.iterrows():
            raw_url = str(row[col_img]).split(',')[0].strip()
            if not raw_url or raw_url == 'nan' or not raw_url.startswith('http'):
                continue

            url_hash = hashlib.sha256(raw_url.encode()).hexdigest()
            
            cursor.execute("SELECT status FROM downloads WHERE url_hash = ?", (url_hash,))
            if cursor.fetchone():
                continue

            # Creamos la tarea de descarga
            tarea = asyncio.create_task(descargar_imagen(client, raw_url, url_hash, semaphore))
            tareas.append((tarea, url_hash, raw_url))

            # Procesamos en bloques para no saturar la RAM si el CSV es gigante
            if len(tareas) >= 20:
                for t, h, u in tareas:
                    exito = await t
                    if exito:
                        cursor.execute("INSERT OR IGNORE INTO downloads (url_hash, url, status) VALUES (?, ?, ?)",
                                     (h, u, 'DOWNLOADED'))
                conn.commit()
                tareas = []
                print(f"[{i}] Registros analizados...")

        # Terminar tareas restantes
        for t, h, u in tareas:
            exito = await t
            if exito:
                cursor.execute("INSERT OR IGNORE INTO downloads (url_hash, url, status) VALUES (?, ?, ?)",
                             (h, u, 'DOWNLOADED'))
        conn.commit()

    conn.close()
    shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))
    print(f"✅ Finalizado y movido: {file_path.name}")

async def main():
    init_db()
    if not INBOX_DIR.exists(): INBOX_DIR.mkdir(parents=True)
    if not PROCESSED_DIR.exists(): PROCESSED_DIR.mkdir(parents=True)
    
    while True:
        csvs = list(INBOX_DIR.glob("*.csv"))
        if not csvs:
            print("Esperando archivos en inbox... (Ctrl+C para salir)")
            await asyncio.sleep(10)
            continue
        
        for csv in csvs:
            await procesar_csv(csv)

if __name__ == "__main__":
    asyncio.run(main())

