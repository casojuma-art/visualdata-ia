import os
import torch
from datasets import load_dataset
from transformers import AutoProcessor, AutoModelForVision2Seq, TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model

# CONFIGURACIÓN (Node 211 - RTX 5090)
MODEL_ID = "google/paligemma-3b-pt-224" # Ejemplo de VLM ligero y potente
DATASET_PATH = "/lab/visualdata-ia/data_preparada/dataset_vlm_train.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"

def train():
    print("🚀 [04a] Iniciando Entrenamiento en la Fábrica...")
    
    # 1. Carga de Dataset
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")
    
    # 2. Configuración de Modelo y Procesador
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    model = AutoModelForVision2Seq.from_pretrained(
        MODEL_ID, 
        torch_dtype=torch.bfloat16, 
        device_map="auto"
    )

    # 3. Configuración LoRA (Entrenamiento Eficiente)
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules=["q_proj", "v_proj"],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)

    # 4. Argumentos de Entrenamiento
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,
        learning_rate=2e-4,
        num_train_epochs=3,
        save_steps=500,
        logging_steps=100,
        bf16=True, # Optimizado para la serie RTX 50
        push_to_hub=False,
        report_to="none"
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        # Aquí se incluiría la lógica de pre-procesamiento de imágenes y textos
    )

    print("🎓 Estudiando el dataset...")
    trainer.train()
    
    # Guardar el modelo final (La Verdad Absoluta)
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print(f"✅ Modelo entrenado y guardado en: {OUTPUT_DIR}")

if __name__ == "__main__":
    train()
