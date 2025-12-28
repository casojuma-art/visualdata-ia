import requests
import os

# --- CONFIGURACIÓN ---
# Ajusta la URL si tu API corre en otro puerto o IP
API_URL = "http://localhost:8005/verify" 
API_KEY = "seestocks_secret_key_wwRT"

# Ruta de la imagen de la "Bomba de Calor Ariston" (ID 3391e5...)
IMG_PATH = "/lab/visualdata-ia/imagenes_in/33/91/3391e51c3061b391e910f4266b42322deac7b76f01941ec07a2d719d24030b1c.jpg"

def probar():
    if not os.path.exists(IMG_PATH):
        print(f"❌ Error: No encuentro la imagen en {IMG_PATH}")
        return

    print(f"📸 Enviando imagen: {os.path.basename(IMG_PATH)}")
    print(f"📡 Destino API: {API_URL}")

    try:
        with open(IMG_PATH, 'rb') as f:
            files = {'file': ('test.jpg', f, 'image/jpeg')}
            # Enviamos título y categoría genéricos para ver solo qué opina la VISIÓN
            data = {
                'title': 'Bomba de calor Ariston Nuos', 
                'category': 'Suministros eléctricos'
            }
            headers = {"X-API-Key": API_KEY}
            
            response = requests.post(API_URL, files=files, data=data, headers=headers)
            
            if response.status_code == 200:
                res = response.json()
                print("\n--- RESULTADOS V2 ---")
                print(f"✅ Sugerencia Visual (Categoría): '{res.get('image_suggest_category')}'")
                print(f"📊 Confianza Técnica (Score):      {res.get('image_suggest_confidence')}")
                
                # INTERPRETACIÓN
                cat = res.get('image_suggest_category')
                conf = res.get('image_suggest_confidence')
                
                if cat is None and conf < 0.22:
                    print("\n🎉 ¡ÉXITO! El sistema se ha 'callado' correctamente.")
                    print("   Como la confianza es baja (< 0.22), no ha alucinado que es una Tablet.")
                elif cat:
                    print(f"\n⚠️ CUIDADO: El sistema sigue sugiriendo '{cat}'.")
                    print("   Si la categoría es errónea, necesitas subir el TAXONOMY_THRESHOLD en api.py.")
            else:
                print(f"❌ Error API: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Error de conexión: {e}")

if __name__ == "__main__":
    probar()
