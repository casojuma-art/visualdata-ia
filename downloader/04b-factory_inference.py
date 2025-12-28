import torch
from PIL import Image
from transformers import AutoProcessor, PeftModel, AutoModelForVision2Seq

# CONFIGURACIÓN
BASE_MODEL = "google/paligemma-3b-pt-224"
ADAPTER_PATH = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

def ejecutar_fabrica(image_path, prompt):
    """Genera la ficha técnica y SEO automáticamente."""
    processor = AutoProcessor.from_pretrained(BASE_MODEL)
    model = AutoModelForVision2Seq.from_pretrained(BASE_MODEL, torch_dtype=torch.bfloat16).to(DEVICE)
    model = PeftModel.from_pretrained(model, ADAPTER_PATH)

    image = Image.open(image_path).convert("RGB")
    inputs = processor(text=prompt, images=image, return_tensors="pt").to(DEVICE)

    with torch.no_grad():
        output = model.generate(**inputs, max_new_tokens=500)
    
    return processor.decode(output[0], skip_special_tokens=True)

# Ejemplo de uso:
# resultado = ejecutar_fabrica("ruta/a/nueva_imagen.jpg", "Genera ficha técnica y SEO.")
