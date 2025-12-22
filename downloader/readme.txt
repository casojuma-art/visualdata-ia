===========================================================
PIPELINE SeeStocks: CLASIFICACIÓN, DESCARGA Y VALIDACIÓN (IA)
===========================================================

1. GUÍA DE OPERACIÓN (CÓMO EJECUTAR)
-----------------------
$ cd /lab/developer/docker/visualdata-ia
$ docker compose up -d [cite: 1]

Fase 01 - Ejecutar Clasificación:
$ docker exec -it vi-downloader python 01-simplifica.py [cite: 1]

Fase 00 - Ejecutar Descarga:
$ docker exec -it vi-downloader python 00-descargaimagenes.py [cite: 1]

Fase 02 - Ejecutar Validación Visual (IA):
$ docker exec -it vi-downloader python 02-validador_imagenes.py

===========================================================
2. FLUJO DE DATOS (INPUT / OUTPUT)
===========================================================

-----------------------------------------------------------
SCRIPT: 01-simplifica.py
-----------------------------------------------------------
ENTRADA (Input):
- Carpeta: /lab/visualdata-ia/data_in/raw/ [cite: 2]
- Formato: CSV original del proveedor (sucio). [cite: 2]
SALIDA (Output):
- Carpeta: /lab/visualdata-ia/data_in/simplified/ [cite: 2]
- Formato: CSV estandarizado con separador ';' y nuevas columnas 
           de categoría IA (category_path, category_id). [cite: 2, 3]
- Acción: Genera una fila por cada imagen encontrada. [cite: 4]

-----------------------------------------------------------
SCRIPT: 00-descargaimagenes.py
-----------------------------------------------------------
ENTRADA (Input):
- Carpeta: /lab/visualdata-ia/data_in/inbox/ [cite: 5]
- Acción: Debes MOVER manualmente los archivos de /simplified 
           a /inbox para que el descargador los detecte. [cite: 5]
SALIDA (Output):
- Imágenes: /lab/visualdata-ia/imagenes_in/aa/bb/hash.jpg [cite: 6, 18]
- CSV Final: /lab/visualdata-ia/data_in/downloaded/ [cite: 6]
- Acción: Al terminar, mueve el CSV de /inbox a /downloaded. [cite: 6]

----------------------------------------------------------
SCRIPT: 02-validador_imagenes.py
----------------------------------------------------------
ENTRADA (Input):
- Archivos CSV en: /lab/visualdata-ia/data_in/simplified/
PROCESO:
- Auditoría visual con CLIP (compara imagen vs título/categoría).
- Persistencia: Actualiza scores de IA en DB (Auto-Save cada 100 registros).
SALIDA (Output):
- Acción: Al terminar, mueve el CSV de /simplified a /lab/visualdata-ia/data_in/03indatabase/.

===========================================================
3. INSPECCIÓN DE BASE DE DATOS (Downloads) [cite: 8]
-----------------------
Acceso: $ docker exec -it vi-downloader sqlite3 /lab/visualdata-ia/db/registry.db [cite: 8]

- Control Estado Validación (Resumen): [cite: 8]
SELECT COUNT(*) as total, SUM(CASE WHEN is_valid IS NULL AND status = 'DOWNLOADED' THEN 1 ELSE 0 END) as pendientes, SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as OK, SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as KO FROM downloads; [cite: 8]

- Auditoría de imágenes rechazadas (KO) y motivos:
SELECT url, score_watermark as logo, score_placeholder as no_disponible, score_quality as mala_calidad, confidence FROM downloads WHERE is_valid = 0;

- Ver dominios y volumen de fotos: [cite: 9]
SELECT REPLACE(REPLACE(SUBSTR(url, INSTR(url, '//') + 2), 'www.', ''), SUBSTR(SUBSTR(url, INSTR(url, '//') + 2), INSTR(SUBSTR(url, INSTR(url, '//') + 2), '/')), '') AS dominio, COUNT(*) AS total_fotos FROM downloads GROUP BY dominio ORDER BY total_fotos DESC; [cite: 9]

- Ver resumen de estados: [cite: 10]
SELECT status, COUNT(*) FROM downloads GROUP BY status; [cite: 10]

- Últimos errores descarga: [cite: 11]
SELECT url, timestamp FROM downloads WHERE status = 'FAILED' ORDER BY timestamp DESC LIMIT 10; [cite: 11]

-----------------------------------------------------------
ESTRUCTURA DE LA TABLA: downloads 
-----------------------------------------------------------
url_hash          TEXT (PK)   Clave Primaria. SHA-256 de la URL. 
url               TEXT        La URL original de origen de la imagen. [cite: 13]
status            TEXT        Estado: DOWNLOADED o FAILED. [cite: 13]
is_valid          INTEGER     Resultado IA: 1 (Apta) / 0 (Rechazada).
confidence        FLOAT       Puntuación de confianza general de la IA.
score_category    FLOAT       Similitud entre imagen y categoría GPC. [cite: 20]
score_product     FLOAT       Similitud entre imagen y título de producto.
score_watermark   FLOAT       Probabilidad de logos/texto (>0.6 = KO). [cite: 20]
score_placeholder FLOAT       Detección de "Imagen no disponible". [cite: 20]
score_quality     FLOAT       Detección de fotos borrosas o baja resolución. [cite: 21]
timestamp         DATETIME    Fecha y hora automática del registro. [cite: 14]

-----------------------------------------------------------
ROLES Y CONCURRENCIA
-----------------------------------------------------------
- Usuario root / sudo (Host): Control total sobre el archivo .db. [cite: 15]
- Usuario python (Contenedor): Escritura para registrar resultados. [cite: 16]
- Concurrencia: SQLite Journal Mode; soporta múltiples lectores. [cite: 17]

===========================================================
4. SEGURIDAD Y MANTENIMIENTO
-----------------------
- Sanitización API: api.py elimina '\', '\0' y tags HTML. [cite: 18]
- Estructura Imágenes: Dispersión por hash (/aa/bb/) para evitar límites de archivos. [cite: 19]
- Logs IA: docker logs -f visual_validator_api
- Logs Descarga: docker logs -f vi-downloader
===========================================================
