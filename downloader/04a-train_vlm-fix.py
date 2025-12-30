import os
import torch
from PIL import Image
from datasets import load_dataset
from transformers import AutoProcessor, AutoModelForImageTextToText, TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model

# CONFIGURACIÓN
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"

def train():
    print("🚀 [04a-FIX] Iniciando Entrenamiento (Estrategia Causal)...")
    
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

    # --- CORRECCIÓN: PREPARAR DATOS PARA MODELO CAUSAL ---
    def collate_fn(examples):
        texts = [ex["conversations"][0]["value"] for ex in examples] # Pregunta
        labels = [ex["conversations"][1]["value"] for ex in examples] # Respuesta
        images = [Image.open(ex["image"]).convert("RGB") for ex in examples]

        # 1. Fusionamos Pregunta + \n + Respuesta
        # El modelo ve todo junto y aprende a completar la parte final
        full_texts = [t + "\n" + l for t, l in zip(texts, labels)]
        
        # 2. Tokenizamos todo junto
        inputs = processor(text=full_texts, images=images, return_tensors="pt", padding="longest")
        
        # 3. Clonamos para crear las etiquetas (targets)
        input_ids = inputs["input_ids"]
        labels_tensor = input_ids.clone()
        
        # 4. ENMASCARADO (Masking):
        # Ponemos -100 en la parte de la IMAGEN y la PREGUNTA.
        # Así el modelo solo "puntúa" si acierta la RESPUESTA.
        tokens_pad = processor.tokenizer.pad_token_id
        
        for i in range(len(input_ids)):
            # Averiguamos cuánto mide la pregunta+imagen sola
            prompt_only = texts[i] + "\n"
            prompt_tokens = processor(text=prompt_only, images=images[i], return_tensors="pt")["input_ids"][0]
            prompt_len = len(prompt_tokens)
            
            # Ocultamos la pregunta (ponemos -100)
            labels_tensor[i, :prompt_len] = -100
            # Ocultamos el relleno (padding)
            labels_tensor[i][input_ids[i] == tokens_pad] = -100

        inputs["labels"] = labels_tensor
        return inputs

    # Modelo y Configuración
    model = AutoModelForImageTextToText.from_pretrained(
        MODEL_ID, 
        dtype=torch.bfloat16, 
        device_map="auto"
    )

    # Configuración LoRA Potenciada
    lora_config = LoraConfig(
        r=16, lora_alpha=32,
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # Argumentos de Entrenamiento (Batch seguro)
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=4, 
        gradient_accumulation_steps=8, 
        learning_rate=2e-4,
        num_train_epochs=1,
        save_steps=500,
        logging_steps=10,
        bf16=True,
        remove_unused_columns=False,
        report_to="none"
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=dataset,
        data_collator=collate_fn
    )

    trainer.train()
    
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print(f"✅ Entrenamiento Finalizado. Guardado en: {OUTPUT_DIR}")

if __name__ == "__main__":
    train()
