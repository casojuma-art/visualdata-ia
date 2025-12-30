import json
import sys
from transformers import AutoProcessor

# CONFIGURACIÓN
MODEL_ID = "google/paligemma-3b-pt-224"
DATASET_PATH = "/lab/visualdata-ia/metadata/dataset_vlm_final.jsonl"

def audit():
    print(f"🕵️‍♂️ [03e] Iniciando auditoría de longitud (Streaming)...")
    print(f"📚 Modelo de referencia: {MODEL_ID}")
    
    # Cargamos solo el tokenizador (rápido y ligero)
    processor = AutoProcessor.from_pretrained(MODEL_ID)
    tokenizer = processor.tokenizer
    
    lengths = []
    over_1024 = 0
    over_2048 = 0
    max_len = 0
    longest_record = ""
    longest_id = -1
    
    try:
        with open(DATASET_PATH, 'r') as f:
            print(f"📊 Leyendo {DATASET_PATH}...")
            
            i = 0
            for line in f:
                i += 1
                if not line.strip(): continue # Saltar líneas vacías
                
                try:
                    data = json.loads(line)
                    # Extraemos pregunta y respuesta
                    q = data["conversations"][0]["value"]
                    a = data["conversations"][1]["value"]
                    
                    # Simulamos el texto completo que ve el modelo (Prompt + Respuesta)
                    full_text = q + "\n" + a
                    
                    # Contamos tokens (Sin contar los 256 de la imagen, solo texto puro)
                    text_ids = tokenizer(full_text, add_special_tokens=False)["input_ids"]
                    token_count = len(text_ids)
                    
                    lengths.append(token_count)
                    
                    # Estadísticas
                    if token_count > 1024:
                        over_1024 += 1
                    if token_count > 2048:
                        over_2048 += 1
                        
                    # Guardamos al campeón
                    if token_count > max_len:
                        max_len = token_count
                        longest_id = i
                        # Guardamos un extracto seguro
                        excerpt = full_text[:300] + "..." if len(full_text) > 300 else full_text
                        longest_record = f"ID (Línea): {i}\nLONGITUD: {token_count} tokens\nCONTENIDO:\n{excerpt}"
                        
                    # Progreso visual cada 5000 registros
                    if i % 5000 == 0:
                        print(f"   -> Escaneados: {i} | Máx actual: {max_len} tokens", end='\r')
                        
                except Exception as e:
                    print(f"\n⚠️ Error en línea {i}: {e}")
                    
    except FileNotFoundError:
        print("❌ No encuentro el archivo. Revisa la ruta.")
        return

    # --- INFORME FINAL ---
    if len(lengths) == 0:
        print("❌ El archivo estaba vacío.")
        return

    avg_len = sum(lengths) / len(lengths)
    
    print("\n\n" + "="*50)
    print("🏁 RESULTADOS DE LA AUDITORÍA DE DATOS")
    print("="*50)
    print(f"Total registros: {len(lengths)}")
    print(f"Longitud Media:  {avg_len:.2f} tokens")
    print(f"Longitud Máxima: {max_len} tokens 🚨")
    print("-" * 50)
    print(f"⚠️  > 1024 tokens: {over_1024} registros ({(over_1024/len(lengths))*100:.4f}%)")
    print(f"⛔️ > 2048 tokens: {over_2048} registros ({(over_2048/len(lengths))*100:.4f}%)")
    print("="*50)
    print("\n🏆 EL CAMPEÓN DE PESO PESADO (Culpable potencial de OOM):")
    print("-" * 50)
    print(longest_record)
    print("="*50)
    
    # RECOMENDACIÓN AUTOMÁTICA
    print("\n💡 RECOMENDACIÓN:")
    if max_len < 1000:
        print("✅ Tus datos son ligeros. Batch Size=4 u 8 es seguro.")
    elif over_1024 < (len(lengths) * 0.01): # Menos del 1%
        print("✂️ Solo unos pocos casos extremos. Usa 'truncation=True' y Batch Size=2 o 4.")
    else:
        print("⚠️ Tienes muchos textos largos. Usa Batch Size=1 y Gradient Checkpointing obligatoriamente.")

if __name__ == "__main__":
    audit()
