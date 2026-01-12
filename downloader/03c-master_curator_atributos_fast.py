# ==================================================================================
# SCRIPT: 03c-master_curator_atributos_fast.py (V42 - TURBO DEEP SCAN)
# OBJETIVO: Extracción masiva y profunda.
# OPTIMIZACIONES:
#   - Batch Size: 1000 (Velocidad máxima para RTX 5090).
#   - Limite Texto: 3000 caracteres (Para no perder atributos al final).
#   - Prompt: Neutro (Analista Técnico).
# ==================================================================================

import sqlite3
import json
import time
from vllm import LLM, SamplingParams

# --- CONFIGURACIÓN DE ALTO RENDIMIENTO ---
MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
BATCH_SIZE = 1000  # Procesamiento masivo por bloques
TEXT_LIMIT = 3000  # Límite ampliado para leer todo el detalle técnico

# =================================================================
# PROMPT V40: ANALISTA TÉCNICO (NEUTRO)
# =================================================================
SYSTEM_PROMPT = """
Eres un ANALISTA DE DATOS TÉCNICOS y bibliotecario experto.
Tu única función es leer la información del producto y ESTRUCTURARLA en formato JSON estandarizado.

DIRECTRICES DE EXTRACCIÓN (NEUTRALIDAD):
1. NO inventes información. Solo extrae lo que esté explícito o sea obvio por contexto técnico.
2. Si la categoría del producto parece errónea, IGNÓRALA y céntrate en lo que es el producto realmente.
3. LIMPIEZA: No uses "No especificado", "N/A". Si no hay dato, omite la clave.
4. ESTRUCTURA: Normaliza unidades (ej: "10m" -> "10 metros").

REGLAS DE CAMPOS:
- Sexo: Solo "Hombre", "Mujer", "Unisex".
- Talla: Úsalo para ropa. Para objetos, usa Dimensiones (Longitud, Anchura, Altura).
- detalles_producto: Usa claves técnicas precisas (ej: "Potencia", "Voltaje", "Caudal").

RESUMEN TÉCNICO (aspectos_destacados):
Extrae 2-5 características funcionales. Usa tono descriptivo y neutral.

Responde EXCLUSIVAMENTE con este JSON válido:

{
  "atributos_estandar": {
      "Color": null, "Sexo": null, "Grupo de edad": null, "Material": null,
      "Talla": null, "Diseño": null, "Longitud del producto": null,
      "Anchura del producto": null, "Altura del producto": null,
      "Peso del producto": null, "Nivel de eficiencia energética": null
  },
  "detalles_producto": { "Nombre_Atributo_Tecnico": "Valor" },
  "aspectos_destacados": ["Característica 1", "Característica 2"]
}
"""

def get_pending_rows(limit):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    # Seleccionamos lotes grandes de productos pendientes
    cursor.execute("""
        SELECT url_hash, titulo, descripcion, cuerpo_limpio, atributos_originales, categoria_final 
        FROM downloads 
        WHERE is_valid = 1 
          AND categoria_final IS NOT NULL 
          AND cuerpo_limpio IS NOT NULL
          AND (atributos IS NULL OR length(atributos) < 10)
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def save_batch(updates):
    if not updates: return
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany("UPDATE downloads SET atributos = ? WHERE url_hash = ?", updates)
        conn.commit()
        conn.close()
        print(f"💾 Lote guardado: {len(updates)} registros.")
    except Exception as e:
        print(f"❌ Error crítico guardando en BBDD: {e}")

def run_production():
    print(f"🚀 Iniciando MODO TURBO DEEP V42 (Batch: {BATCH_SIZE}, Limit: {TEXT_LIMIT})...")
    llm = LLM(
        model=MODEL_PATH, 
        gpu_memory_utilization=0.90, 
        quantization="awq_marlin", 
        dtype="float16", 
        enforce_eager=True
    )
    sampling_params = SamplingParams(temperature=0.1, max_tokens=1024)

    while True:
        rows = get_pending_rows(BATCH_SIZE)
        if not rows:
            print("✅ No quedan registros pendientes. ¡Trabajo terminado!")
            break

        prompts = []
        hashes = []

        print(f"📥 Cargando lote de {len(rows)} productos...")

        for row in rows:
            # TEXTOS AMPLIADOS A 3000 CARACTERES
            desc_txt = row['descripcion'][:TEXT_LIMIT] if row['descripcion'] else ""
            cuerpo_txt = row['cuerpo_limpio'][:TEXT_LIMIT] if row['cuerpo_limpio'] else ""
            
            # Input técnico
            raw_tech = row['atributos_originales']
            tech_data = raw_tech if (raw_tech and len(raw_tech) > 2 and raw_tech != '{}') else "No disponible"

            user_content = (
                f"PRODUCTO: {row['titulo']}\n"
                f"DESCRIPCIÓN: {desc_txt}\n"
                f"CATEGORÍA: {row['categoria_final']}\n"
                f"CUERPO TÉCNICO: {cuerpo_txt}\n"
                f"DATOS TÉCNICOS: {tech_data}"
            )
            
            prompt = (f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n{SYSTEM_PROMPT}<|eot_id|>"
                      f"<|start_header_id|>user<|end_header_id|>\n\n{user_content}<|eot_id|>"
                      f"<|start_header_id|>assistant<|end_header_id|>\n\n")
            
            prompts.append(prompt)
            hashes.append(row['url_hash'])

        # Inferencia Masiva
        start_time = time.time()
        outputs = llm.generate(prompts, sampling_params)
        
        updates = []
        
        for i, output in enumerate(outputs):
            json_str = output.outputs[0].text.strip()
            current_hash = hashes[i]
            
            # Limpieza Markdown por si acaso
            if "```" in json_str:
                json_str = json_str.replace("```json", "").replace("```", "").strip()

            try:
                json.loads(json_str) 
                updates.append((json_str, current_hash))
            except:
                # Marcamos error con motivo V42
                error_marker = '{"status": "error_json", "motivo": "invalid_syntax_v42"}'
                updates.append((error_marker, current_hash))
        
        save_batch(updates)
        
        # Estadísticas de velocidad
        elapsed = time.time() - start_time
        items_per_sec = len(rows) / elapsed
        print(f"⚡ Velocidad: {items_per_sec:.2f} items/seg (Lote en {elapsed:.2f}s)")

if __name__ == "__main__":
    run_production()
