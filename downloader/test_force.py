import torch
import sys
import os
from transformers import PaliGemmaForConditionalGeneration, PaliGemmaProcessor
from PIL import Image

# --- CONFIGURACIÓN ---
BASE_MODEL_ID = "google/paligemma-3b-pt-224"
TRAIN_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v16-robust/"

def get_latest_checkpoint(base_path):
    if not os.path.exists(base_path): return None
    checkpoints = [d for d in os.listdir(base_path) if d.startswith("checkpoint")]
    if not checkpoints: return None
    latest = sorted(checkpoints, key=lambda x: int(x.split("-")[1]))[-1]
    return os.path.join(base_path, latest)

def main():
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
    else:
        print("❌ Indica la imagen.")
        return

    # --- PROMPT DE FUERZA BRUTA ---
    # Le damos la lista de claves que QUEREMOS que rellene.
    # Esto a veces "desbloquea" al modelo.
    prompt_text = """<image>
Analiza este producto visualmente.
Tu tarea es rellenar obligatoriamente el JSON con atributos visibles.
NO DEJES EL JSON VACIO.
Extrae: "Color", "Material", "Tipo", "Componentes".
Dictamina la categoría y limpia la descripción."""

    print(f"🔥 PROMPT AGRESIVO:\n{prompt_text}\n")

    checkpoint_path = get_latest_checkpoint(TRAIN_DIR)
    try:
        processor = PaliGemmaProcessor.from_pretrained(BASE_MODEL_ID)
        model = PaliGemmaForConditionalGeneration.from_pretrained(
            checkpoint_path,
            device_map="cpu",
            torch_dtype=torch.float32
        )
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    print("🚀 Generando respuesta forzada...")
    image = Image.open(image_path).convert("RGB")
    inputs = processor(text=prompt_text, images=image, return_tensors="pt")
    inputs = {k: v.to("cpu") for k, v in inputs.items()}

    with torch.no_grad():
        generation = model.generate(**inputs, max_new_tokens=512, do_sample=False)
        decoded = processor.decode(generation[0][inputs["input_ids"].shape[-1]:], skip_special_tokens=True)

    print("\n" + "="*20 + " RESULTADO " + "="*20)
    print(decoded)
    print("="*51 + "\n")

if __name__ == "__main__":
    main()
