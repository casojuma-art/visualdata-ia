# ==================================================================================
# SCRIPT: 03c-master_curator_atributos_fast.py (V30)
# OBJETIVO: Extracción REAL OFFLINE (GPU) con Prompt Optimizado SEO/Merchant.
# MEJORAS V30:
#   - Limpieza estricta: Prohibido "No especificado".
#   - Claves dinámicas: Prohibido "Característica_Tecnica".
#   - Copywriting: Frases orientadas a beneficio.
# ==================================================================================

import sqlite3
import json
import time
from vllm import LLM, SamplingParams

# --- CONFIGURACIÓN TÉCNICA ---
MODEL_PATH = "hugging-quants/Meta-Llama-3.1-8B-Instruct-AWQ-INT4"
DB_PATH = "/lab/visualdata-ia/db/registry.db"
DEBUG_MODE = True  
BATCH_SIZE = 10 

# =================================================================
# PROMPT V30: EXPERTO SEO Y MERCHANT CENTER
# =================================================================
SYSTEM_PROMPT = """
Eres un experto en optimización de feeds para Google Merchant Center y SEO técnico.
Tu objetivo es enriquecer la ficha del producto extrayendo datos estructurados de alta calidad del texto proporcionado.

INSTRUCCIONES CRÍTICAS DE CALIDAD:
1. VALORES REALES: Jamás inventes datos. Si no encuentras el dato exacto o una inferencia lógica fuerte, usa null.
2. LIMPIEZA EXTREMA: Prohibido usar textos como "No especificado", "N/A", "Varios", "Ver detalles", "No aplica". Si no hay dato, NO incluyas la clave en 'detalles_producto'.
3. NO DUPLICIDAD: Si un dato ya está relleno en 'atributos_estandar' (ej: Material, Color), NO lo repitas en 'detalles_producto'.
4. CLAVES DINÁMICAS: En 'detalles_producto', GENERA nombres de claves específicos (ej: "Voltaje", "Tipo de Cierre", "Ingredientes"). NUNCA uses "Característica_Tecnica" como clave literal.

REGLAS DE NORMALIZACIÓN GOOGLE MERCHANT:
- Sexo: Úsalo solo para moda/accesorios. Valores: "Hombre", "Mujer", "Unisex".
- Grupo de edad: "recién nacido", "bebé", "infante", "niños", "adulto".
- Talla: Úsalo para ropa/calzado. Para dimensiones de objetos (muebles, cajas), intenta usar Longitud/Anchura/Altura.

MARKETING (Aspectos Destacados):
Genera 2-5 "Key Selling Points" (puntos de venta clave) optimizados para conversión.
- Estructura: [Verbo de Acción/Beneficio] + [Característica Técnica].
- Ejemplo: "Ahorra energía gracias a su certificación A+".
- Evita frases vacías como "Alta calidad" o "Diseño exclusivo" si no van acompañadas del porqué.

Responde EXCLUSIVAMENTE con JSON válido y nada más:

{
  "atributos_estandar": {
      "Color": null,
      "Sexo": null,
      "Grupo de edad": null,
      "Material": null,
      "Talla": null,
      "Diseño": null,
      "Longitud del producto": null,
      "Anchura del producto": null,
      "Altura del producto": null,
      "Peso del producto": null,
      "Nivel de eficiencia energética": null
  },
  "detalles_producto": {
      "Nombre_Atributo_Especifico": "Valor_Real"
  },
  "aspectos_destacados": [
      "Beneficio 1",
      "Beneficio 2"
  ]
}
"""

def get_random_rows(limit):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT url_hash, titulo, cuerpo_limpio, atributos as atributos_raw, categoria_final 
        FROM downloads 
        WHERE is_valid = 1 
          AND categoria_final IS NOT NULL 
          AND cuerpo_limpio IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def run_curator():
    print(f"🚀 Cargando vLLM (Prompt V30 SEO/Merchant)...")
    llm = LLM(
        model=MODEL_PATH, 
        gpu_memory_utilization=0.90, 
        quantization="awq_marlin", 
        dtype="float16", 
        enforce_eager=True
    )
    sampling_params = SamplingParams(temperature=0.1, max_tokens=1024)

    rows = get_random_rows(BATCH_SIZE)
    prompts = []

    for row in rows:
        user_content = f"PRODUCTO: {row['titulo']}\nCATEGORÍA: {row['categoria_final']}\nCUERPO: {row['cuerpo_limpio']}\nATRIBUTOS: {row['atributos_raw']}"
        prompt = (f"<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n\n{SYSTEM_PROMPT}<|eot_id|>"
                  f"<|start_header_id|>user<|end_header_id|>\n\n{user_content}<|eot_id|>"
                  f"<|start_header_id|>assistant<|end_header_id|>\n\n")
        prompts.append(prompt)

    print(f"⚡ Procesando Auditoría V30...")
    outputs = llm.generate(prompts, sampling_params)

    for i, output in enumerate(outputs):
        print("\n" + "═"*100)
        print(f" 🔍 AUDITANDO HASH: {rows[i]['url_hash']}")
        print("═" + "═"*98)
        print(f" 📦 [ENTRADA REAL]")
        print(f"    • Título: {rows[i]['titulo']}")
        print(f"    • Atributos Raw: {rows[i]['atributos_raw']}")
        print(f"    • Cuerpo Completo:\n{rows[i]['cuerpo_limpio']}")
        
        print(f"\n 🧠 [RESULTADO IA V30]")
        print("-" * 60)
        print(output.outputs[0].text.strip())
        print("-" * 60)
        print("═"*100 + "\n")

if __name__ == "__main__":
    run_curator()
