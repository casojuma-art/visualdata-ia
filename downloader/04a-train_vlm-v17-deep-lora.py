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

# --- CONFIGURACIÓN V18 (BASE V16 + POTENCIA 5090) ---
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v18-final"

def train():
    print(f"🚀 [MODO V18 - FINAL] Estabilidad del V16 + Potencia 5090.")
    
    # 1. Configuración del Modelo (Igual que V16)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True, 
        bnb_4bit_quant_type="nf4", 
        bnb_4bit_compute_dtype=torch.bfloat16
    )
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        MODEL_ID, quantization_config=bnb_config, device_map="auto"
    )
    processor = PaliGemmaProcessor.from_pretrained(MODEL_ID)
    
    # 2. LoRA (Aquí aplicamos la mejora de inteligencia de la V17)
    lora_config = LoraConfig(
        r=32,                # Más capacidad que el v16 (era 8)
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

    # --- LÓGICA DE ROBUSTEZ (PROMPT DROPOUT - IDÉNTICA A V16) ---
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

        if scenario < 0.35: # Solo Visión
            new_prompt = f"<image>\nAnaliza este producto visualmente.\n{instruccion_fija}"
        elif scenario < 0.70: # Título + Visión
            t = titulo_real if titulo_real else "Producto"
            new_prompt = f"<image>\nAnaliza este producto:\n- Título: {t}\n{instruccion_fija}"
        else: # Full Context
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

    # 4. Argumentos (Equilibrio VRAM/Velocidad)
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        
        # AJUSTE SEGURO PARA 32GB VRAM (Sin Gradient Checkpointing)
        per_device_train_batch_size=4,   # Doble que en V16
        gradient_accumulation_steps=16,  # Acumulamos para un batch efectivo de 64
        
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
    
    print("🔥 ARRANCANDO V18 (Codebase V16 + Power 5090)...")
    trainer.train()
    
    print(f"💾 Guardando modelo final en {OUTPUT_DIR}...")
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print("✅ FIN.")

if __name__ == "__main__":
    train()
