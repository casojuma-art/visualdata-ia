caso@cs:/lab/developer/docker/visualdata-ia/downloader$ cat readme.txt 
===========================================================
PIPELINE SeeStocks: CLASIFICACIÓN, DESCARGA Y VALIDACIÓN (IA)
===========================================================

1. GUÍA DE OPERACIÓN (CÓMO EJECUTAR)
------------------------------------
$ cd /lab/developer/docker/visualdata-ia
[cite_start]$ docker compose up -d

# --- PASO PREVIO: ARRANCAR SERVIDOR LLM (RTX 5090) ---
$ docker run -d --name vllm-server --gpus all -p 8000:8000 --ipc=host \
  --ulimit memlock=-1 --ulimit stack=67108864 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  -e HF_TOKEN="tu_token_aqui" \
  vllm/vllm-openai:latest \
  hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4 \
  --gpu-memory-utilization 0.95 --max-model-len 32768 --max-num-seqs 512



Fase 00 - Ejecutar Descarga de Imágenes:
docker exec -it vi-downloader python 00-descargaimagenes.py 


Fase 01 - Ejecutar Clasificación y Enriquecimiento:

docker exec -it vi-downloader python 01-simplifica.py 



Fase 02 - Ejecutar Validación Visual (IA):
docker exec -it vi-downloader python 02-validador_imagenes.py 

Fase 03a - Preparación de Ingesta Masiva:
docker exec -it vi-downloader python 03a-procesar_ingesta.py
(Verifica imágenes físicas y genera dataset CSV para entrenamiento)

Fase 03b - Curación de Texto (RTX 5090):
docker exec -it vi-downloader python 03a-text_curator.py
(Limpia publicidad y neutraliza textos para marca blanca usando vLLM)

Fase 03c - La Universidad (Dataset Builder):
$ docker exec -it vi-downloader python 03c-dataset_builder.py
(Genera JSONL vinculando imagen con la Verdad Absoluta: cuerpo_limpio)

# --- FASE 04: LA FÁBRICA (INFERENCIA) ---
Producción masiva de descripciones limpias y neutrales automáticas.



===========================================================
2. FLUJO DE DATOS (INPUT / OUTPUT) ordenado
===========================================================


SCRIPT: 00-descargaimagenes.py
-----------------------------------------------------------
A partir de CVS de 200 ecommerce, descarga las imagenes en disco.
ENTRADA (Input):
- [cite_start]Carpeta: /lab/visualdata-ia/data_in/inbox/ [cite: 5]
- [cite_start]Acción: Mover manualmente archivos de /simplified a /inbox. [cite: 5]
SALIDA (Output):
- [cite_start]Imágenes: /lab/visualdata-ia/imagenes_in/aa/bb/hash.jpg [cite: 6, 19]
- [cite_start]CSV Final: /lab/visualdata-ia/data_in/downloaded/ [cite: 6]
- [cite_start]Acción: Al terminar, mueve el CSV de /inbox a /downloaded. [cite: 6]

-----------------------------------------------------------
SCRIPT: 01-simplifica.py
-----------------------------------------------------------
clasifica el contenido en el arbol de taxonomia de google merchant usando BERTH
COnsolida todos los atributos raw en un json en campo atributos  en BBDD
ENTRADA (Input):
- [cite_start]Carpeta: /lab/visualdata-ia/data_in/raw/ [cite: 2]
- [cite_start]Formato: CSV original del proveedor (sucio). [cite: 2]
SALIDA (Output):
- [cite_start]Carpeta: /lab/visualdata-ia/data_in/simplified/ [cite: 2]
- [cite_start]Formato: CSV estandarizado (separador ';') con mapeo de: [cite: 2, 3]
    * titulo, descripcion, cuerpo_Es, atributos y categoria.
- [cite_start]Acción: Genera una fila por cada imagen encontrada. [cite: 4]

-----------------------------------------------------------

-----------------------------------------------------------
SCRIPT: 02-validador_imagenes.py
-----------------------------------------------------------
- Auditoría visual con CLIP (Imagen vs Título/Categoría).
- Optimización de Red: Redimensión de imágenes a 224x224 en el cliente antes del envío.
- Lógica Smart-Resume: Detecta y salta automáticamente registros ya validados y enriquecidos para evitar reprocesamiento.
- Persistencia: Auto-Save cada 100 registros en registry.db.


ENTRADA (Input):
- Archivos CSV en: /lab/visualdata-ia/data_in/simplified/
PROCESO:
- Auditoría visual con CLIP (Imagen vs Título/Categoría). [cite: 8]
- Persistencia: Auto-Save cada 100 registros en registry.db. [cite: 9]
SALIDA (Output):
- Acción: Mueve CSV a /lab/visualdata-ia/data_in/03indatabase/. [cite: 10]
devuelve:
is_valid: Es un valor booleano (guardado como 1 o 0). Indica si la imagen ha pasado el filtro de calidad y coherencia global definido en la API. Si es 1, la imagen es considerada apta para el catálogo.

confidence: Es el nivel de certeza general que tiene el modelo sobre su predicción (usualmente de 0.0 a 1.0). Representa qué tan "segura" está la IA de que su análisis es correcto.

score_category: Mide la similitud entre la imagen y la categoría proporcionada (ej: si la categoría es "Sillas", qué tanto se parece la imagen a una silla).

score_product: Mide la similitud entre la imagen y el título/nombre específico del producto. Es más preciso que el de categoría porque busca detalles del título.

score_watermark: Indica la probabilidad de que la imagen contenga marcas de agua o texto superpuesto no deseado.

score_placeholder: Indica si la imagen es un "placeholder" (una imagen genérica de "imagen no disponible", un logo de error o una caja vacía).

score_quality: Evalúa la calidad visual. Puntuaciones bajas suelen indicar imágenes borrosas, muy pequeñas o con excesivo ruido visual.

image_suggest_category:Esta es una de las variables más útiles. Si la IA detecta que la imagen no encaja bien con la categoría original, o simplemente para enriquecer el dato, la API propone una categoría alternativa basada únicamente en lo que "ve" en la imagen. image_suggest_category TEXT        Categoría sugerida por Visión (CLIP vs Taxonomía GPC). Puede ser NULL/Vacío si no supera el umbral de certeza.
    * ACTUALIZACIÓN V2: Incluye "Filtro de Ignorancia". Si la similitud visual no supera el umbral de confianza (0.28), este campo se guarda vacío para evitar alucinaciones o categorizaciones erróneas.


SCRIPT: 03a-procesar_ingesta.py
-----------------------------------------------------------
ENTRADA:
- [cite_start]Registros con is_valid = 1 [cite: 19] e imágenes en /aa/bb/.
SALIDA:
- Archivo: /lab/visualdata-ia/data_preparada/dataset_final_train.csv.

SUB-FASE 03-B: EL EDITOR (03b-text_curator.py)
-----------------------------------------------------------
PROCESO:
- Sanitización masiva de 'cuerpo_Es' mediante Llama-3.
- Elimina precios, tiendas, teléfonos y primera persona.
- Mantiene datos técnicos y tono SEO neutral.
SALIDA: Columna 'cuerpo_limpio' en registry.db (Verdad Absoluta).

auditar proceso y fallidos
sqlite> SELECT 
    COUNT(*) AS total_validos,
    COUNT(CASE WHEN categoria_final IS NOT NULL AND categoria_final NOT LIKE '%FAILED%' THEN 1 END) AS curados_con_exito,
    COUNT(CASE WHEN categoria_final LIKE '%FAILED%' THEN 1 END) AS fallidos_definitivos,
    COUNT(CASE WHEN categoria_final IS NULL OR score_ia IS NULL THEN 1 END) AS pendientes_o_reintentos,
    ROUND(100.0 * COUNT(CASE WHEN categoria_final IS NOT NULL AND categoria_final NOT LIKE '%FAILED%' THEN 1 END) / COUNT(*), 2) AS porcentaje_curado_exito
FROM downloads 
WHERE is_valid = 1;
801718|41237|16|760465|5.14



-----------------------------------------------------------
SUB-FASE 03-C: LA UNIVERSIDAD (03c-dataset_builder.py)
-----------------------------------------------------------
PROCESO:
- Mapeo de Imagen + Contexto -> Atributos + Texto Limpio.
SALIDA: Archivo JSONL listo para entrenamiento de VLM.


==========================================================
PIPELINE SeeStocks: FASE 04 - LA FÁBRICA (VLM)
===========================================================

1. OBJETIVO
------------------------------------
Transferir el conocimiento del dataset multimodal (BERT, CLIP, Texto Curado)
a un modelo VLM propio capaz de automatizar el catálogo.

2. GUÍA DE OPERACIÓN
------------------------------------
Fase 04a - Entrenamiento: $ python 04a-train_vlm.py
(Estudio masivo del JSONL generado en 03c. Usa RTX 5090).

Fase 04b - Inferencia:    $ python 04b-factory_inference.py
(Producción automática de descripciones y atributos).

3. FLUJO DE PRODUCCIÓN (MOLDEADO)
------------------------------------
Nuevas imágenes + Metadatos -> [ MODELO VLM ENTRENADO ] -> Verdad Absoluta.
- El modelo actúa como árbitro final resolviendo conflictos de categoría.
- El output cumple estrictamente con el estilo neutral de marca blanca.
===========================================================



===========================================================
3. [cite_start]INSPECCIÓN DE BASE DE DATOS (Downloads) [cite: 8]
-----------------------
[cite_start]Acceso: $ docker exec -it vi-downloader sqlite3 /lab/visualdata-ia/db/registry.db [cite: 8]

- [cite_start]Resumen de Calidad y Validación: [cite: 8]
[cite_start]SELECT COUNT(*) as total, SUM(CASE WHEN is_valid IS NULL THEN 1 ELSE 0 END) as pendientes, SUM(CASE WHEN is_valid = 1 THEN 1 ELSE 0 END) as OK, SUM(CASE WHEN is_valid = 0 THEN 1 ELSE 0 END) as KO FROM downloads; [cite: 8]

- Auditoría de Atributos y Sugerencias IA:
SELECT titulo, categoria, image_suggest_category, confidence FROM downloads WHERE is_valid = 1 LIMIT 20;

- Detección de Conflictos (BERT vs Visión):
SELECT titulo, categoria, image_suggest_category FROM downloads WHERE score_category < 0.2 AND is_valid = 1

- Monitoreo de Curación por LLM:
SELECT titulo, cuerpo_limpio FROM downloads WHERE cuerpo_limpio IS NOT NULL LIMIT 20;

-----------------------------------------------------------
ESTRUCTURA DE LA TABLA: downloads 
-----------------------------------------------------------
[cite_start]url_hash               TEXT (PK)   SHA-256 de la URL. [cite: 16]
[cite_start]url                    TEXT        URL original de la imagen. [cite: 17]
[cite_start]status                 TEXT        Estado: DOWNLOADED / FAILED. [cite: 18]
[cite_start]is_valid               INTEGER     1 (Apta) / 0 (Rechazada). [cite: 19]
[cite_start]confidence             FLOAT       Confianza general de la IA. [cite: 20]
[cite_start]score_category         FLOAT       Similitud Imagen vs Categoría GPC. [cite: 21]
[cite_start]score_product          FLOAT       Similitud Imagen vs Título. [cite: 22]
[cite_start]score_watermark        FLOAT       Probabilidad de logos/texto. [cite: 23]
[cite_start]score_placeholder      FLOAT       Detección de "Imagen no disponible". [cite: 24]
[cite_start]score_quality          FLOAT       Detección de nitidez/resolución. [cite: 25]
titulo                 TEXT        Nombre comercial del producto.
descripcion            TEXT        Descripción corta del producto.
cuerpo_Es              TEXT        Descripción larga / SEO.
cuerpo_limpio          TEXT        Texto técnico limpio generado por LLM.
atributos              TEXT        Especificaciones técnicas (JSON/Texto).
categoria              TEXT        Categoría asignada por BERT (Texto).
image_suggest_category TEXT        Categoría sugerida por Visión (CLIP vs Taxonomía GPC). Puede ser NULL/Vacío si no supera el umbral de certeza.
-----------------------------------------------------------
ROLES Y MANTENIMIENTO
-----------------------------------------------------------
- [cite_start]Estructura Imágenes: Dispersión /aa/bb/ por hash. [cite: 30]
- [cite_start]Sanitización: api.py limpia tags HTML y caracteres nulos. [cite: 29]
- [cite_start]Concurrencia: SQLite Journal Mode (multilectura). [cite: 28]
===========================================================
limpiar bbdd entre pruebas curacion
docker exec -it vi-downloader sqlite3 /lab/visualdata-ia/db/registry.db "
UPDATE downloads 
SET categoria_final = NULL, 
    score_ia = NULL, 
    razonamiento_ia = NULL,
    atributos = NULL,
    cuerpo_limpio = NULL
WHERE is_valid = 1;
VACUUM;



arrancar llm

docker rm -f vllm-server

docker run -d --name vllm-server --gpus all -p 8000:8000 --ipc=host \
  --ulimit memlock=-1 --ulimit stack=67108864 \
  -v ~/.cache/huggingface:/root/.cache/huggingface \
  -e HF_TOKEN="tu_token_aqui" \
  vllm/vllm-openai:latest \
  hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4 \
  --gpu-memory-utilization 0.95 --max-model-len 32768 --max-num-seqs 512

ver el log
docker logs -f vllm-server
