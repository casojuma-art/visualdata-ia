import sqlite3
import json
import re

# ================= CONFIGURACIÓN =================
DB_PATH = "/lab/visualdata-ia/db/registry.db"
LIMIT_EXAMPLES = 50  # Cuántos ejemplos quieres ver

# ================= DICCIONARIOS Y PATRONES =================
COLORES = [
    "rojo", "azul", "verde", "amarillo", "negro", "blanco", "gris", "plata", 
    "dorado", "naranja", "rosa", "violeta", "marrón", "beige", "cromado", 
    "inox", "transparente", "multicolor", "bronce", "cobre"
]
MATERIALES = [
    "madera", "metal", "plástico", "acero", "aluminio", "cuero", "tela", 
    "algodón", "vidrio", "cristal", "polipropileno", "resina", "goma", 
    "silicona", "cerámica", "latón", "piel", "poliéster", "bambú"
]

def simulate_extraction(text):
    """
    Simula la lógica de extracción basada en tu lista de ecommerce.
    Retorna un diccionario con lo encontrado.
    """
    found = {}
    text_lower = text.lower()
    
    # 1. COLOR
    for c in COLORES:
        if re.search(rf"\b{c}\b", text_lower):
            found["Color"] = c.capitalize()
            break

    # 2. MATERIAL
    for m in MATERIALES:
        if re.search(rf"\b{m}\b", text_lower):
            found["Material"] = m.capitalize()
            break

    # 3. EFICIENCIA ENERGÉTICA (Ej: Clase A++, A+, F)
    # Buscamos "Clase A", "Eficiencia A", o simplemente "A++" aislado si contexto ayuda
    energy = re.search(r'\b(?:clase|eficiencia|energ[eé]tica)\s+([a-g](?:\+{1,3})?)\b', text_lower)
    if energy:
        found["Eficiencia energética"] = energy.group(1).upper()

    # 4. DIMENSIONES (Largo, Ancho, Alto, Talla)
    # Patrón común: 50x50 cm, 100 x 200 mm
    dims = re.search(r'\b(\d+(?:[.,]\d+)?)\s*[xX]\s*(\d+(?:[.,]\d+)?)(?:\s*[xX]\s*(\d+(?:[.,]\d+)?))?\s*(cm|mm|m)\b', text_lower)
    if dims:
        # Si encuentra formato AxB o AxBxC
        full_match = dims.group(0)
        found["Dimensiones"] = full_match
    
    # Patrón longitud/altura explícita
    # Ej: "cable 5m", "altura 10cm"
    longitud = re.search(r'\b(?:largo|longitud|cable)\s*de\s*(\d+(?:[.,]\d+)?\s*(?:cm|m|mm))', text_lower)
    if longitud:
        found["Longitud"] = longitud.group(1)

    # 5. VOLTAJE / POTENCIA (Técnico)
    volt = re.search(r'\b(\d{1,3})\s*[vV](?:oltios)?\b', text)
    if volt: found["Voltaje"] = f"{volt.group(1)}V"
    
    watts = re.search(r'\b(\d+(?:[.,]\d+)?)\s*([kK]?[wW])(?:atts)?\b', text)
    if watts: found["Potencia"] = f"{watts.group(1)}{watts.group(2)}"

    # 6. CANTIDAD (Pack, Set)
    pack = re.search(r'\b(?:set|pack|juego|lote|kit|caja)\s+(?:de\s+)?(\d+)', text_lower)
    if pack: found["Cantidad"] = pack.group(1)

    return found

def run_simulation():
    print(f"🕵️‍♂️ Iniciando SIMULACRO de enriquecimiento...")
    print(f"   Buscando registros VACÍOS que se puedan arreglar con el texto.")
    print("-" * 120)
    print(f"{'TÍTULO (Recortado)':<50} | {'ANTES (JSON)':<15} | {'DESPUÉS (Simulado)':<40}")
    print("-" * 120)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Seleccionamos registros con JSON vacío o sospechoso
    # Y que tengan texto suficiente para buscar
    cursor.execute("""
        SELECT titulo, atributos, cuerpo_limpio 
        FROM downloads 
        WHERE is_valid = 1 
          AND (atributos IS NULL OR atributos = '{}' OR atributos LIKE '%clave%')
          AND length(titulo) > 5
    """)
    
    count_shown = 0
    
    # Iteramos (Streaming) para no cargar todo en memoria
    while count_shown < LIMIT_EXAMPLES:
        row = cursor.fetchone()
        if not row: break
        
        titulo, json_raw, descripcion = row
        contexto = f"{titulo} {descripcion or ''}"
        
        # 1. Simular Extracción
        nuevos_datos = simulate_extraction(contexto)
        
        # 2. Si encontramos algo interesante, lo mostramos
        if nuevos_datos:
            tit_short = (titulo[:47] + '...') if len(titulo) > 47 else titulo
            json_old = "{}"
            json_new = json.dumps(nuevos_datos, ensure_ascii=False)
            
            # Colorear output para verlo mejor
            print(f"{tit_short:<50} | {json_old:<15} | \033[92m{json_new:<40}\033[0m")
            count_shown += 1

    conn.close()
    print("-" * 120)
    print(f"✅ Simulacro finalizado. Se mostraron {count_shown} ejemplos potenciales.")

if __name__ == "__main__":
    run_simulation()
