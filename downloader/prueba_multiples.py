import requests
import os

# --- CONFIGURACIÓN ---
API_URL = "http://localhost:8005/verify"
API_KEY = "seestocks_secret_key_wwRT"
BASE_IMG_DIR = "/lab/visualdata-ia/imagenes_in"

# LISTA DE "SOSPECHOSOS" A PROBAR
TEST_CASES = [
    {
        "nombre": "Bomba Calor (El 'Champú')",
        "hash": "3391e51c3061b391e910f4266b42322deac7b76f01941ec07a2d719d24030b1c"
    },
    {
        "nombre": "Downlight LED (El 'Tenedor')",
        "hash": "ba99d75eb4327686dec80b4cea7c73f6f20b370b031b44c2d759e1b518bfb665"
    },
    {
        "nombre": "Campana UFO (La 'Luz de Coche')",
        "hash": "92f444a900cf4f8829e05e1f74ae7f0012b399b27da3229352c9cb2a477c2316"
    }
]

def get_image_path(h):
    # Reconstruye la ruta: /ab/cd/hash.jpg
    return os.path.join(BASE_IMG_DIR, h[:2], h[2:4], f"{h}.jpg")

def probar():
    print(f"🚀 Iniciando batería de pruebas contra {API_URL}...\n")
    
    headers = {"X-API-Key": API_KEY}

    for caso in TEST_CASES:
        h = caso['hash']
        nombre = caso['nombre']
        img_path = get_image_path(h)

        print(f"🔹 Probando: {nombre}")
        
        if not os.path.exists(img_path):
            print(f"   ❌ NO ENCONTRADA: {img_path}")
            continue

        try:
            with open(img_path, 'rb') as f:
                files = {'file': ('test.jpg', f, 'image/jpeg')}
                data = {'title': 'Test', 'category': 'Test'}
                
                response = requests.post(API_URL, files=files, data=data, headers=headers)
                
                if response.status_code == 200:
                    res = response.json()
                    cat = res.get('image_suggest_category')
                    score = res.get('image_suggest_confidence', 0.0)
                    
                    print(f"   📊 Score: {score}")
                    if cat is None:
                        print(f"   ✅ RESULTADO: SILENCIO (Correcto si dudaba).")
                    else:
                        print(f"   ⚠️ RESULTADO: SUGIERE '{cat}'")
                        if score < 0.28:
                            print("      [!] OJO: Tiene score bajo pero devolvió categoría. ¿Revisaste el umbral?")
                else:
                    print(f"   ❌ Error API: {response.status_code}")

        except Exception as e:
            print(f"   ❌ Error conexión: {e}")
        
        print("-" * 40)

if __name__ == "__main__":
    probar()
