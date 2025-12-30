import os
import torch
from PIL import Image
from datasets import load_dataset
from transformers import AutoProcessor, AutoModelForImageTextToText, TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model

# --- CONFIGURACIÓN ---
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"

# LÍMITE DE SEGURIDAD:
# Cualquier registro que supere esto será IGNORADO (no entrenado).
# 1536 es un buen equilibrio (cabe de sobra en 32GB con Batch 1).
MAX_ALLOWED_TOKENS = 1536 

def train():
    print("🚀 [04a-AUTO-FILTER] Iniciando: Filtrado al vuelo + Entrenamiento Blindado...")
    
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    tokenizer = processor.tokenizer
    
    # 1. Cargar Dataset en modo Streaming (sin llenar RAM)
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train", streaming=True)

    # --- EL FILTRO INTEGRADO ---
    def filter_fn(example):
        # Reconstruimos el texto tal cual lo verá el modelo
        q = example["conversations"][0]["value"]
        a = example["conversations"][1]["value"]
        full_text = q + "\n" + a
        
        # Medimos longitud (rápido, solo ids)
        # No hace falta ser exacto al milímetro, solo detectar los gigantes
        length = len(tokenizer(full_text, add_special_tokens=False)["input_ids"])
        
        # Devuelve True si es seguro (lo mantiene), False si es gigante (lo tira)
        return length <= MAX_ALLOWED_TOKENS

    print(f"🛡️ Aplicando filtro: Descartando registros > {MAX_ALLOWED_TOKENS} tokens...")
    dataset = dataset.filter(filter_fn)
    # ---------------------------

    def collate_fn(examples):
        texts = [ex["conversations"][0]["value"] for ex in examples] 
        labels = [ex["conversations"][1]["value"] for ex in examples] 
        images = [Image.open(ex["image"]).convert("RGB") for ex in examples]

        full_texts = [t + "\n" + l for t, l in zip(texts, labels)]
        
        # Ya hemos filtrado los gigantes, pero mantenemos truncation=True 
        # como "doble seguridad" por si acaso se cuela algo raro.
        inputs = processor(
            text=full_texts, 
            images=images, 
            return_tensors="pt", 
            padding="longest",
            truncation=True,
            max_length=MAX_ALLOWED_TOKENS
        )
        
        input_ids = inputs["input_ids"]
        labels_tensor = input_ids.clone()
        tokens_pad = processor.tokenizer.pad_token_id
        
        for i in range(len(input_ids)):
            prompt_only = texts[i] + "\n"
            prompt_tokens = processor(text=prompt_only, images=images[i], return_tensors="pt")["input_ids"][0]
            prompt_len = len(prompt_tokens)
            
            if prompt_len > len(labels_tensor[i]):
                prompt_len = len(labels_tensor[i])

            labels_tensor[i, :prompt_len] = -100
            labels_tensor[i][input_ids[i] == tokens_pad] = -100

        inputs["labels"] = labels_tensor
        return inputs

    # Configuración del Modelo
    model = AutoModelForImageTextToText.from_pretrained(
        MODEL_ID, 
        dtype=torch.bfloat16, 
        device_map="auto"
    )
    model.gradient_checkpointing_enable() 

    lora_config = LoraConfig(
        r=16, lora_alpha=32,
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)
    
    def make_inputs_require_grad(module, input, output):
        output.requires_grad_(True)
    model.get_input_embeddings().register_forward_hook(make_inputs_require_grad)
    
    # Argumentos de Entrenamiento (Modo Tanque)
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=1,      # Seguridad VRAM
        gradient_accumulation_steps=32,     # Eficiencia
        learning_rate=2e-4,
        max_steps=19800,                    # Ajustado ligeramente por si filtramos algunos
        save_steps=1000,
        logging_steps=50,
        bf16=True,
        gradient_checkpointing=True,
        remove_unused_columns=False,
        report_to="none",
        dataloader_pin_memory=True,
        dataloader_num_workers=2            # 2 workers es suficiente con el filtro activo
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
