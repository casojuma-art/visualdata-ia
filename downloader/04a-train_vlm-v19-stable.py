import os
import torch
import random
from PIL import Image
from datasets import load_dataset
from transformers import (
    PaliGemmaProcessor, PaliGemmaForConditionalGeneration, 
    TrainingArguments, Trainer, BitsAndBytesConfig
)
from peft import LoraConfig, get_peft_model

# --- CONFIGURACIÓN V19b (SMART & SAFE - FIXED) ---
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v19-stable"

def train():
    print(f"🛡️ [MODO V19b] Estrategia Blindada: Rank 32 + Batch 2 (Sin Grouping).")
    
    # 1. Configuración del Modelo
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True, 
        bnb_4bit_quant_type="nf4", 
        bnb_4bit_compute_dtype=torch.bfloat16
    )
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        MODEL_ID, quantization_config=bnb_config, device_map="auto"
    )
    processor = PaliGemmaProcessor.from_pretrained(MODEL_ID)
    
    # 2. LoRA (Alta capacidad)
    lora_config = LoraConfig(
        r=32,                
        lora_alpha=64, 
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, 
        bias="none", 
        task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # 3. Carga de Datos
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")
    dataset = dataset.shuffle(seed=42) 

    # --- ROBUSTEZ ---
    def create_robust_prompt(example):
        raw_prompt = example["conversations"][0]["value"]
        titulo_real = ""
        try:
            lines = raw_prompt.split('\n')
            for line in lines:
                if "- Título:" in line:
                    titulo_real = line.split("Título:")[1].strip()
                    break
        except: pass

        scenario = random.random() 
        instruccion_fija = "Dictamina la categoría oficial completa, extrae todos los atributos visuales en JSON y limpia la descripción."

        if scenario < 0.35: 
            new_prompt = f"<image>\nAnaliza este producto visualmente.\n{instruccion_fija}"
        elif scenario < 0.70:
            t = titulo_real if titulo_real else "Producto"
            new_prompt = f"<image>\nAnaliza este producto:\n- Título: {t}\n{instruccion_fija}"
        else:
            new_prompt = raw_prompt
        return new_prompt

    def collate_fn(examples):
        texts, images, labels = [], [], []
        for ex in examples:
            prompt_input = create_robust_prompt(ex)
            answer = ex["conversations"][1]["value"]
            full_prompt = (prompt_input + "\n") if "<image>" in prompt_input else ("<image>" + prompt_input + "\n")
            texts.append(full_prompt)
            labels.append(answer)
            images.append(Image.open(ex["image"]).convert("RGB"))

        inputs = processor(
            text=texts, images=images, suffix=labels, return_tensors="pt", 
            padding="longest", truncation=True, max_length=1024
        )
        return inputs.to(torch.bfloat16)

    # 4. Argumentos
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        
        # --- SEGURIDAD TOTAL ---
        per_device_train_batch_size=2,   # 2 imágenes por paso (Muy seguro)
        gradient_accumulation_steps=32,  # Acumulamos 32 veces (Equivale a Batch 64)
        
        # group_by_length=False,         # ELIMINADO para evitar el error
        
        num_train_epochs=1,            
        save_strategy="steps",         
        save_steps=1000,
        save_total_limit=2,
        learning_rate=2e-5,
        weight_decay=0.01,
        warmup_steps=100,
        logging_steps=20, 
        bf16=True,
        optim="paged_adamw_8bit",
        report_to="none",
        remove_unused_columns=False,
        dataloader_pin_memory=True
    )

    trainer = Trainer(model=model, args=training_args, train_dataset=dataset, data_collator=collate_fn)
    
    print("🔥 ARRANCANDO V19b (Modo Seguro Corregido)...")
    trainer.train()
    
    print(f"💾 Guardando modelo final en {OUTPUT_DIR}...")
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print("✅ FIN.")

if __name__ == "__main__":
    train()
