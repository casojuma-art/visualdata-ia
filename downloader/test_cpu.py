import torch
import sys
import os
from transformers import PaliGemmaForConditionalGeneration, PaliGemmaProcessor
from PIL import Image

# --- CONFIGURACIÓN ---
BASE_MODEL_ID = "google/paligemma-3b-pt-224"
#TRAIN_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v15-1epoch"
TRAIN_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v16-robust/"

def get_latest_checkpoint(base_path):
    if not os.path.exists(base_path): return None
    checkpoints = [d for d in os.listdir(base_path) if d.startswith("checkpoint")]
    if not checkpoints: return None
    latest = sorted(checkpoints, key=lambda x: int(x.split("-")[1]))[-1]
    return os.path.join(base_path, latest)

def main():
    # 1. Gestionar la imagen desde el argumento
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
    else:
        print("❌ Error: Indica la imagen. Ej: python test_cpu.py ./foto.jpg")
        return

    if not os.path.exists(image_path):
        print(f"❌ La imagen no existe: {image_path}")
        return

    # 2. INTERACCIÓN: Pedir datos para simular un caso real
    print("\n" + "="*50)
    print("🤖 TEST INTERACTIVO (MODO CPU)")
    print("="*50)
    print(f"📸 Imagen: {image_path}")
    print("Introduce datos (o pulsa Enter para dejar en blanco/desconocido):")
    
    titulo = input("\n📝 Título del producto: ").strip()
    if not titulo: titulo = "Producto desconocido"
    
    sugerencia = input("🗂️  Sugerencia Categoría: ").strip()
    if not sugerencia: sugerencia = "Sin categoría"

    # 3. Construir el Prompt exacto del entrenamiento
    prompt_text = f"""<image>
Analiza este producto:
- Título: {titulo}
- Sugerencia Texto: {sugerencia}
Extrae obligatoriamente atributos visuales en JSON (Color, Material, Tipo, Cantidad).
Limpia la descripción."""

    print(f"\n⏳ Cargando modelo y procesador...")

    # 4. Cargar modelo (Seguro para CPU)
    checkpoint_path = get_latest_checkpoint(TRAIN_DIR)
    if not checkpoint_path:
        print("❌ No hay checkpoints.")
        return
        
    try:
        # Processor de Google (para evitar error de config faltante)
        processor = PaliGemmaProcessor.from_pretrained(BASE_MODEL_ID)
        # Modelo de tu checkpoint
        model = PaliGemmaForConditionalGeneration.from_pretrained(
            checkpoint_path,
            device_map="cpu",
            torch_dtype=torch.float32
        )
    except Exception as e:
        print(f"❌ Error cargando: {e}")
        return

    # 5. Generar
    print("🚀 Pensando respuesta...")
    image = Image.open(image_path).convert("RGB")
    inputs = processor(text=prompt_text, images=image, return_tensors="pt")
    inputs = {k: v.to("cpu") for k, v in inputs.items()}

    with torch.no_grad():
        generation = model.generate(**inputs, max_new_tokens=512, do_sample=False)
        input_len = inputs["input_ids"].shape[-1]
        decoded = processor.decode(generation[0][input_len:], skip_special_tokens=True)

    print("\n" + "="*20 + " RESULTADO " + "="*20)
    print(decoded)
    print("="*51 + "\n")

if __name__ == "__main__":
    main()
