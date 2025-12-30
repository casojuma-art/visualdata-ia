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
    print("🚀 [04a-FINAL] Iniciando Entrenamiento (Modo Tanque: Batch 1 + Checkpointing)...")
    
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    dataset = load_dataset("json", data_files=DATASET_PATH, split="train", streaming=True)

    def collate_fn(examples):
        texts = [ex["conversations"][0]["value"] for ex in examples] 
        labels = [ex["conversations"][1]["value"] for ex in examples] 
        images = [Image.open(ex["image"]).convert("RGB") for ex in examples]

        full_texts = [t + "\n" + l for t, l in zip(texts, labels)]
        inputs = processor(text=full_texts, images=images, return_tensors="pt", padding="longest")
        
        input_ids = inputs["input_ids"]
        labels_tensor = input_ids.clone()
        tokens_pad = processor.tokenizer.pad_token_id
        
        for i in range(len(input_ids)):
            prompt_only = texts[i] + "\n"
            prompt_tokens = processor(text=prompt_only, images=images[i], return_tensors="pt")["input_ids"][0]
            prompt_len = len(prompt_tokens)
            labels_tensor[i, :prompt_len] = -100
            labels_tensor[i][input_ids[i] == tokens_pad] = -100

        inputs["labels"] = labels_tensor
        return inputs

    model = AutoModelForImageTextToText.from_pretrained(
        MODEL_ID, 
        dtype=torch.bfloat16, 
        device_map="auto"
    )

    # Habilitamos Gradient Checkpointing en el modelo base para ahorrar VRAM masiva
    model.gradient_checkpointing_enable() 

    lora_config = LoraConfig(
        r=16, lora_alpha=32,
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_dropout=0.05, bias="none", task_type="CAUSAL_LM"
    )
    model = get_peft_model(model, lora_config)
    
    # Aseguramos que los gradientes fluyan bien con checkpointing
    def make_inputs_require_grad(module, input, output):
        output.requires_grad_(True)
    model.get_input_embeddings().register_forward_hook(make_inputs_require_grad)

    model.print_trainable_parameters()

    training_args = TrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=1,      # <--- MÍNIMO ABSOLUTO (Seguridad VRAM)
        gradient_accumulation_steps=32,     # <--- MÁXIMA ACUMULACIÓN (Equivalente a Batch 32)
        learning_rate=2e-4,
        max_steps=20000,
        save_steps=1000,
        logging_steps=50,
        bf16=True,
        gradient_checkpointing=True,        # <--- LA CLAVE: Reduce consumo VRAM a la mitad
        remove_unused_columns=False,
        report_to="none",
        dataloader_pin_memory=True,
        dataloader_num_workers=4
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
