import os
import torch
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
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v14-full"  # <--- Versión v14 Full

def train():
    print(f"🚀 [MODO COMPLETO] Entrenando por 5 ÉPOCAS (Sin límite de tiempo)...")
    print(f"   Esto exprimirá al máximo el dataset para bajar ese loss de 0.6")
    
    # 1. Carga Optimizada (4-bit)
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16
    )
    model = PaliGemmaForConditionalGeneration.from_pretrained(
        MODEL_ID, quantization_config=bnb_config, device_map="auto"
    )
    processor = PaliGemmaProcessor.from_pretrained(MODEL_ID)
    
    # 2. LoRA (Configuración segura)
    lora_config = LoraConfig(
        r=8, lora_alpha=16, 
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)

    # 3. Datos
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train")

    def collate_fn(examples):
        texts, images, labels = [], [], []
        for ex in examples:
            q = ex["conversations"][0]["value"]
            a = ex["conversations"][1]["value"]
            # Anti-duplicados: Evita el error de visión doble
            full_prompt = (q + "\n") if "<image>" in q else ("<image>" + q + "\n")
            texts.append(full_prompt)
            labels.append(a)
            images.append(Image.open(ex["image"]).convert("RGB"))

        inputs = processor(
            text=texts, images=images, suffix=labels, return_tensors="pt", 
            padding="longest", truncation=True, max_length=1024
        )
        return inputs.to(torch.bfloat16)

    # 4. Argumentos para ENTRENAMIENTO COMPLETO
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=2,
        gradient_accumulation_steps=8,
        
        # --- ESTRATEGIA DE TIEMPO: ÉPOCAS ---
        # En lugar de pasos fijos, usamos épocas.
        # 5 épocas es el "Gold Standard" para que aprenda bien sin memorizar demasiado (overfitting).
        num_train_epochs=5,            
        
        # Guardado
        save_strategy="epoch",         # Guarda un checkpoint al final de cada época
        save_total_limit=2,            # Solo mantiene los 2 mejores/últimos para no llenar el disco
        # -------------------------
        
        learning_rate=2e-5,            # LR Suave
        weight_decay=0.01,
        warmup_steps=100,
        logging_steps=50,              # Reporta menos frecuente para no llenar la pantalla
        bf16=True,
        optim="paged_adamw_8bit",
        report_to="none",
        remove_unused_columns=False,
        dataloader_pin_memory=False
    )

    trainer = Trainer(model=model, args=training_args, train_dataset=dataset, data_collator=collate_fn)
    
    print("🔥 INICIANDO ENTRENAMIENTO COMPLETO (5 ÉPOCAS)...")
    print("   Nota: La duración dependerá del tamaño de tu dataset.")
    trainer.train()
    
    print(f"💾 Guardando modelo final en {OUTPUT_DIR}...")
    model.save_pretrained(OUTPUT_DIR)
    processor.save_pretrained(OUTPUT_DIR)
    print("✅ FIN. El modelo ha completado su ciclo de aprendizaje.")

if __name__ == "__main__":
    train()
