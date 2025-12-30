import os
import torch
from PIL import Image
from datasets import load_dataset
from transformers import AutoProcessor, AutoModelForImageTextToText, TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model

# CONFIGURACIÓN (Node 211 - RTX 5090)
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"

def train():
    print("🚀 [04a] Iniciando Entrenamiento en la Fábrica...")
    
    # 1. Carga de Dataset y Procesador
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

    # 2. Función de Colación (Crítica para 800k imágenes)
    def collate_fn(examples):
        texts = [example["conversations"][0]["value"] for example in examples]
        labels = [example["conversations"][1]["value"] for example in examples]
        images = [Image.open(example["image"]).convert("RGB") for example in examples]
        
        # Tokenización y procesamiento de imagen para PaliGemma
        inputs = processor(text=texts, images=images, return_tensors="pt", padding=True)
        # Preparamos las etiquetas (targets)
        with processor.tokenizer.as_target_tokenizer():
            labels_tokens = processor.tokenizer(labels, return_tensors="pt", padding=True).input_ids
            
        inputs["labels"] = labels_tokens
        return inputs

    # 3. Configuración de Modelo (Optimizada para RTX 50)
    model = AutoModelForImageTextToText.from_pretrained(
        MODEL_ID, 
        dtype=torch.bfloat16, 
        device_map="auto"
    )

    lora_config = LoraConfig(
        r=16, lora_alpha=32,
        target_modules=["q_proj", "v_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)

    # 4. Argumentos de Entrenamiento para Gran Volumen
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=8,
        gradient_accumulation_steps=4,
        learning_rate=2e-4,
        num_train_epochs=1, # Con 800k, 1 época suele ser suficiente
        save_steps=1000,    # Guardar cada 1000 pasos por seguridad
        logging_steps=100,
        bf16=True,          # Uso de núcleos Tensor de la 5090
        remove_unused_columns=False,
        report_to="none"
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        data_collator=collate_fn
    )

    print(f"🎓 Estudiando {len(dataset)} registros...")
    trainer.train()
    
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print(f"✅ La Verdad Absoluta guardada en: {OUTPUT_DIR}")

if __name__ == "__main__":
    train()
