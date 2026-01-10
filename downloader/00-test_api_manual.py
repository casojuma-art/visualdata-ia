import requests
import io
from PIL import Image

# Configuración
API_URL = "http://visual_validator_api:8000/verify"
API_KEY = "seestocks_secret_key_wwRT"

def test_api():
    print(f"📡 Probando conexión a: {API_URL} ...")
    
    # 1. Crear una imagen dummy (negra) o intentar leer una real si sabes la ruta
    # Para ser más realistas, creamos una imagen simple en memoria
    img = Image.new('RGB', (224, 224), color = 'red')
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    img_bytes = img_byte_arr.getvalue()

    # 2. Enviar petición
    try:
        files = {'file': ('test.jpg', img_bytes, 'image/jpeg')}
        data = {'title': 'Prueba Técnica', 'category': 'Test'}
        headers = {'X-API-Key': API_KEY}
        
        r = requests.post(API_URL, files=files, data=data, headers=headers, timeout=10)
        
        print(f"🔙 Código HTTP: {r.status_code}")
        
        if r.status_code == 200:
            res = r.json()
            print("\n📦 RESPUESTA JSON COMPLETA:")
            print("-" * 40)
            print(res)
            print("-" * 40)
            
            # Análisis
            conf = res.get('image_suggest_confidence', 0)
            cat = res.get('image_suggest_category')
            print(f"\n🧐 ANÁLISIS:")
            print(f"   - Confianza numérica (Raw): {conf}")
            print(f"   - Categoría sugerida:       {cat}")
            
            if cat is None:
                print("   ⚠️ RESULTADO: NULL. (La confianza es menor al umbral de 0.28 o los embeddings fallaron)")
            else:
                print(f"   ✅ RESULTADO: ÉXITO. ('{cat}')")
        else:
            print(f"❌ Error en API: {r.text}")

    except Exception as e:
        print(f"💥 Excepción conectando: {e}")

if __name__ == "__main__":
    test_api()
