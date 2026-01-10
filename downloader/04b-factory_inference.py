import torch
import sys
import os
from PIL import Image
from transformers import AutoProcessor, PaliGemmaForConditionalGeneration

# --- CONFIGURACIÓN ---
BASE_MODEL = "google/paligemma-3b-pt-224"
# AHORA APUNTAMOS AL MODELO DE PRODUCCIÓN (El de 2 horas)
ADAPTER_PATH = "/lab/visualdata-ia/modelos/seestocks-vlm-v13" 
DEFAULT_IMAGE = "prueba.jpg"

print(f"🏭 Iniciando Inferencia (Script 04b) usando: {ADAPTER_PATH}...")

try:
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        BASE_MODEL,
        torch_dtype=torch.bfloat16,
        device_map="auto",
    ).eval()

    # Intentamos cargar el adaptador. Si no existe, avisamos.
    if os.path.exists(ADAPTER_PATH):
        model.load_adapter(ADAPTER_PATH)
        print("✅ Adaptador LoRA cargado correctamente.")
    else:
        print(f"⚠️ OJO: No encuentro el adaptador en {ADAPTER_PATH}")
        print("   ¿Seguro que el entrenamiento de 2 horas ha terminado?")
        sys.exit(1)

    processor = AutoProcessor.from_pretrained(BASE_MODEL)

except Exception as e:
    print(f"❌ Error cargando modelo: {e}")
    sys.exit(1)

def analizar_producto(ruta_imagen):
    if not os.path.exists(ruta_imagen):
        return f"❌ Error: No existe la imagen {ruta_imagen}"

    # Prompt: Le forzamos a abrir el JSON
    prompt = '<image>Analiza este producto: Título: ¿?\n{"titulo": "'

    try:
        image = Image.open(ruta_imagen).convert("RGB")
        inputs = processor(text=prompt, images=image, return_tensors="pt").to(model.device)
        input_len = inputs.input_ids.shape[-1]

        with torch.inference_mode():
            generated_ids = model.generate(
                **inputs,
                max_new_tokens=200,
                do_sample=False,       
                repetition_penalty=1.2
            )

        output_ids = generated_ids[0][input_len:]
        decoded = processor.decode(output_ids, skip_special_tokens=True)
        
        # Reconstruimos el JSON visualmente
        return '{"titulo": "' + decoded

    except Exception as e:
        return f"❌ Error inferencia: {str(e)}"

if __name__ == "__main__":
    target_image = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_IMAGE
    print(f"\n📸 Procesando: {target_image}...")
    resultado = analizar_producto(target_image)
    
    print("\n" + "="*40)
    print(resultado)
    print("="*40 + "\n")
