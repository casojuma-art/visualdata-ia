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
    print(f"🚀 [MODO ROBUST v16] Entrenando con 'Prompt Dropout' (Ceguera de texto selectiva).")
    print(f"   Objetivo: Forzar al modelo a USAR LA VISIÓN para sacar Atributos y Categorías.")
    
    # 1. Configuración del Modelo (4-bit)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16
    )
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        MODEL_ID, quantization_config=bnb_config, device_map="auto"
    )
    processor = PaliGemmaProcessor.from_pretrained(MODEL_ID)
    
    # 2. LoRA
    lora_config = LoraConfig(
        r=8, lora_alpha=16, 
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)

    # 3. Carga de Datos
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")
    
    print("🎲 Mezclando datos...")
    dataset = dataset.shuffle(seed=42) 

    # --- FUNCIÓN DE ROBUSTEZ (La clave del éxito) ---
    def create_robust_prompt(example):
        raw_prompt = example["conversations"][0]["value"]
        
        # Intentamos extraer el título real del prompt original para reusarlo a veces
        titulo_real = ""
        try:
            lines = raw_prompt.split('\n')
            for line in lines:
                if "- Título:" in line:
                    titulo_real = line.split("Título:")[1].strip()
                    break
        except:
            pass

        # GENERA UN NÚMERO ALEATORIO (0.0 a 1.0)
        scenario = random.random() 
        
        instruccion_fija = "Dictamina la categoría oficial completa, extrae todos los atributos visuales en JSON y limpia la descripción."

        # ESCENARIO 1: SOLO VISIÓN (35% de las veces)
        # Simulamos lo que acabas de hacer en el test: Sin título, sin sugerencia.
        # El modelo aprenderá que AQUÍ también tiene que responder bien.
        if scenario < 0.35:
            new_prompt = f"<image>\nAnaliza este producto visualmente.\n{instruccion_fija}"
            
        # ESCENARIO 2: FOTO + TÍTULO (35% de las veces)
        # Simulamos que solo tenemos el nombre del archivo o un título corto.
        elif scenario < 0.70:
            t = titulo_real if titulo_real else "Producto"
            new_prompt = f"<image>\nAnaliza este producto:\n- Título: {t}\n{instruccion_fija}"
            
        # ESCENARIO 3: FULL CONTEXTO (30% de las veces)
        # El prompt original perfecto (para que no olvide cómo usar la ayuda si la hay)
        else:
            new_prompt = raw_prompt

        return new_prompt

    def collate_fn(examples):
        texts, images, labels = [], [], []
        for ex in examples:
            # APLICAMOS LA ROBUSTEZ AQUÍ
            prompt_input = create_robust_prompt(ex)
            
            # LA RESPUESTA DESEADA SIEMPRE ES LA MISMA (COMPLETA)
            # Esto enseña al modelo: "Tenga yo el texto o no, tú dame la categoría completa y los atributos".
            answer = ex["conversations"][1]["value"]
            
            # Formateo técnico de imagen
            full_prompt = (prompt_input + "\n") if "<image>" in prompt_input else ("<image>" + prompt_input + "\n")
            
            texts.append(full_prompt)
            labels.append(answer)
            images.append(Image.open(ex["image"]).convert("RGB"))

        inputs = processor(
            text=texts, images=images, suffix=labels, return_tensors="pt", 
            padding="longest", truncation=True, max_length=1024
        )
        return inputs.to(torch.bfloat16)

    # 4. Argumentos de Entrenamiento
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=2,
        gradient_accumulation_steps=8, # Batch efectivo = 16
        
        # 1 ÉPOCA es suficiente. 
        # Al ver 600k fotos, 200k serán sin texto, 200k con título, 200k completas.
        num_train_epochs=1,            
        
        save_strategy="steps",         
        save_steps=2000,               # Guarda cada ~45 mins
        save_total_limit=3,
        
        learning_rate=2e-5,
        weight_decay=0.01,
        warmup_steps=100,
        logging_steps=50,
        bf16=True,
        optim="paged_adamw_8bit",
        report_to="none",
        remove_unused_columns=False,
        dataloader_pin_memory=False
    )

    trainer = Trainer(model=model, args=training_args, train_dataset=dataset, data_collator=collate_fn)
    
    print("🔥 INICIANDO ENTRENAMIENTO ROBUSTO (v16)...")
    print("   El modelo aprenderá a no depender del texto.")
    trainer.train()
    
    print(f"💾 Guardando modelo final en {OUTPUT_DIR}...")
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print("✅ FIN.")

if __name__ == "__main__":
    train()
