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

# --- CONFIGURACIÓN ---
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v16-robust"

def train():
    print(f"🚀 [MODO ROBUST v16 - TURBO 5090] Iniciando Entrenamiento...")
    
    # 1. Configuración del Modelo (OPTIMIZADA PARA RTX 5090)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True, 
        bnb_4bit_quant_type="nf4", 
        bnb_4bit_compute_dtype=torch.bfloat16
    )
    
    # ACTIVAMOS FLASH ATTENTION 2 (Aceleración nativa para RTX 40/50 series)
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        MODEL_ID, 
        quantization_config=bnb_config, 
        device_map="auto",
        attn_implementation="flash_attention_2" 
    )
    processor = PaliGemmaProcessor.from_pretrained(MODEL_ID)
    
    # 2. LoRA (Configuración estándar robusta)
    lora_config = LoraConfig(
        r=16,               # Subimos un poco el rango para mejor aprendizaje
        lora_alpha=32, 
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, 
        bias="none", 
        task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)
    model.print_trainable_parameters()

    # 3. Carga de Datos
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")
    print(f"📚 Dataset cargado: {len(dataset)} imágenes.")
    dataset = dataset.shuffle(seed=42) 

    # --- LÓGICA DE ROBUSTEZ (INTACTA) ---
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

        # Escenario 1: Visión Pura (35%)
        if scenario < 0.35:
            new_prompt = f"<image>\nAnaliza este producto visualmente.\n{instruccion_fija}"
        # Escenario 2: Título + Visión (35%)
        elif scenario < 0.70:
            t = titulo_real if titulo_real else "Producto"
            new_prompt = f"<image>\nAnaliza este producto:\n- Título: {t}\n{instruccion_fija}"
        # Escenario 3: Full Context (30%)
        else:
            new_prompt = raw_prompt
        return new_prompt

    def collate_fn(examples):
        texts, images, labels = [], [], []
        for ex in examples:
            prompt_input = create_robust_prompt(ex)
            answer = ex["conversations"][1]["value"]
            
            # Aseguramos token de imagen
            full_prompt = (prompt_input + "\n") if "<image>" in prompt_input else ("<image>" + prompt_input + "\n")
            
            texts.append(full_prompt)
            labels.append(answer)
            images.append(Image.open(ex["image"]).convert("RGB"))

        inputs = processor(
            text=texts, images=images, suffix=labels, return_tensors="pt", 
            padding="longest", truncation=True, max_length=1024
        )
        return inputs.to(torch.bfloat16)

    # 4. Argumentos (AJUSTADOS A VRAM DE 5090)
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        
        # BATCH SIZE: Subimos de 2 a 8.
        # PaliGemma 3B es pequeño. En una 5090 (32GB), 8 o 16 caben de sobra.
        per_device_train_batch_size=8, 
        
        # GRADIENT ACCUMULATION: Ajustamos para un Batch Efectivo de 32 o 64.
        # 8 (batch) * 8 (accum) = 64 muestras por paso de optimizador. Muy estable.
        gradient_accumulation_steps=8, 
        
        num_train_epochs=1,            
        save_strategy="steps",         
        save_steps=1000,               # Guardamos más a menudo porque iremos más rápido
        save_total_limit=3,
        learning_rate=2e-5,
        weight_decay=0.01,
        warmup_steps=100,
        logging_steps=10,              # Feedback más frecuente
        bf16=True,                     # CRÍTICO para serie 50
        optim="paged_adamw_8bit",
        report_to="none",
        remove_unused_columns=False,
        dataloader_pin_memory=True,    # Aceleramos carga de datos
        dataloader_num_workers=4       # Usamos CPUs para pre-cargar imágenes
    )

    trainer = Trainer(model=model, args=training_args, train_dataset=dataset, data_collator=collate_fn)
    
    print("🔥 INICIANDO ENTRENAMIENTO ROBUSTO (TURBO 5090)...")
    trainer.train()
    
    print(f"💾 Guardando modelo final en {OUTPUT_DIR}...")
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print("✅ FIN.")

if __name__ == "__main__":
    train()
