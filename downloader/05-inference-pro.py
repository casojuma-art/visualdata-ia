import torch
import sys
import os
from PIL import Image
# CORRECCIÓN: Quitamos PeftModel de aquí porque daba error
from transformers import AutoProcessor, PaliGemmaForConditionalGeneration

# --- CONFIGURACIÓN ---
BASE_MODEL = "google/paligemma-3b-pt-224"
ADAPTER_PATH = "/lab/visualdata-ia/modelos/seestocks-vlm-v1" 
DEFAULT_IMAGE = "prueba.jpg"

print("🏭 Iniciando Factory Inference (Fixed Version)...")

# 1. CARGA DEL MODELO
# Usamos la clase nativa que sabe gestionar adaptadores sin imports raros
model = PaliGemmaForConditionalGeneration.from_pretrained(
    BASE_MODEL,
    torch_dtype=torch.bfloat16,
    device_map="auto",
).eval()

# 2. CARGAMOS EL ADAPTADOR (LoRA)
# Esta función interna carga lo necesario sin que tengas que importar 'peft' manualmente
model.load_adapter(ADAPTER_PATH)

processor = AutoProcessor.from_pretrained(BASE_MODEL)
print("✅ Modelo cargado correctamente.")

def analizar_producto(ruta_imagen):
    if not os.path.exists(ruta_imagen):
        return f"❌ Error: No existe la imagen {ruta_imagen}"

    # --- TRUCO ANTI-BUCLE ---
    # 1. <image>: Token obligatorio
    # 2. \n: Fin de pregunta
    # 3. {: Forzamos el inicio del JSON
    prompt = "<image>Analiza este producto: Título: ¿?\n{"

    try:
        image = Image.open(ruta_imagen).convert("RGB")
        inputs = processor(text=prompt, images=image, return_tensors="pt").to(model.device)

        with torch.inference_mode():
            generated_ids = model.generate(
                **inputs,
                max_new_tokens=512,
                do_sample=False,
                repetition_penalty=1.1
            )

        result = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
        
        # Limpieza
        parte_generada = result.split("\n")[-1].strip()
        
        # Reconstruimos el JSON
        if not parte_generada.startswith("{"):
            json_final = "{" + parte_generada
        else:
            json_final = parte_generada

        return json_final

    except Exception as e:
        return f"❌ Error: {str(e)}"

if __name__ == "__main__":
    target_image = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_IMAGE
    print(f"\n📸 Procesando: {target_image}...")
    print(analizar_producto(target_image))
    print("\n")
