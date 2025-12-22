import sqlite3
import os

DB_PATH = "/lab/visualdata-ia/db/registry.db"

def init_db():
    # Asegurarnos de que el directorio existe
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tabla para el control de descargas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS downloads (
            url_hash TEXT PRIMARY KEY,
            url_original TEXT NOT NULL,
            status TEXT DEFAULT 'PENDING',
            file_path TEXT,
            http_code INTEGER,
            attempts INTEGER DEFAULT 0,
            last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Base de datos inicializada en {DB_PATH}")

if __name__ == "__main__":
    init_db()
