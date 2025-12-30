import os
import torch
from PIL import Image
from datasets import load_dataset
from transformers import AutoProcessor, AutoModelForImageTextToText, TrainingArguments, Trainer
from peft import LoraConfig, get_peft_model

# --- CONFIGURACIÓN MAESTRA ---
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"
OUTPUT_DIR = "/lab/visualdata-ia/modelos/seestocks-vlm-v1"

# 1. LÍMITE FÍSICO (Lo que cabe en la RTX 5090)
# Si algo pasa de aquí, corta. Pero gracias al filtro, nada llegará a este punto.
MAX_TOTAL_TOKENS = 1800 

# 2. EL FILTRO (La protección)
# Solo dejamos pasar textos que, sumados a la imagen, quepan HOLGADAMENTE.
# 1500 texto + 256 imagen = 1756 (Menor que 1800 -> PERFECTO)
MAX_TEXT_TOKENS_FILTER = 1500

def train():
    print(f"🚀 [04a-V9] Iniciando: Smart Batching + Filtro Seguro ({MAX_TEXT_TOKENS_FILTER})...")
    
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    tokenizer = processor.tokenizer
    
    # Carga en modo Streaming (RAM Plana)
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train", streaming=True)

    # --- FILTRO DE SEGURIDAD ---
    def filter_fn(example):
        q = example["conversations"][0]["value"]
        a = example["conversations"][1]["value"]
        full_text = q + "\n" + a
        length = len(tokenizer(full_text, add_special_tokens=False)["input_ids"])
        
        # Si mide más de 1500, lo tiramos. Así aseguramos que NUNCA se corte la respuesta.
        return length <= MAX_TEXT_TOKENS_FILTER

    print(f"🛡️ Filtro Activo: Ignorando textos > {MAX_TEXT_TOKENS_FILTER} tokens.")
    dataset = dataset.filter(filter_fn)

    def collate_fn(examples):
        texts = [ex["conversations"][0]["value"] for ex in examples] 
        labels = [ex["conversations"][1]["value"] for ex in examples] 
        images = [Image.open(ex["image"]).convert("RGB") for ex in examples]

        full_texts = [t + "\n" + l for t, l in zip(texts, labels)]
        
        inputs = processor(
            text=full_texts, 
            images=images, 
            return_tensors="pt", 
            padding="longest",
            truncation=True,
            max_length=MAX_TOTAL_TOKENS # Cinturón de seguridad final
        )
        
        input_ids = inputs["input_ids"]
        labels_tensor = input_ids.clone()
        tokens_pad = processor.tokenizer.pad_token_id
        
        for i in range(len(input_ids)):
            prompt_only = texts[i] + "\n"
            prompt_tokens = processor(text=prompt_only, images=images[i], return_tensors="pt")["input_ids"][0]
            prompt_len = len(prompt_tokens)
            
            # Protección extra por si el prompt midiera más que el total (improbable con el filtro)
            if prompt_len > len(labels_tensor[i]):
                prompt_len = len(labels_tensor[i])

            labels_tensor[i, :prompt_len] = -100
            labels_tensor[i][input_ids[i] == tokens_pad] = -100

        inputs["labels"] = labels_tensor
        return inputs

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
    
    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=1,
        gradient_accumulation_steps=32,
        learning_rate=2e-4,
        max_steps=19800,
        save_steps=1000,
        logging_steps=50,
        bf16=True,
        gradient_checkpointing=True,
        remove_unused_columns=False,
        report_to="none",
        dataloader_pin_memory=True,
        dataloader_num_workers=2,
        group_by_length=True        # <--- LA MEJORA PRO: Acelera agrupando por tamaño
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
