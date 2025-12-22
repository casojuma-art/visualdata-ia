#!/usr/bin/env python
import os
import glob
import shutil
import pandas as pd
import json
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# --- CONFIGURACIÓN ---
BASE_DIR = "/lab/visualdata-ia/data_in"
INPUT_DIR = os.path.join(BASE_DIR, "downloaded")
OUTPUT_DIR = os.path.join(BASE_DIR, "simplified")
RAW_DIR = os.path.join(BASE_DIR, "raw")

# CONFIGURACIÓN API INTERNA
API_URL = "http://product_classifier_api:8000/classify" 
API_KEY = "seestocks_secret_key_SGvRT" 
MAX_WORKERS = 4 

# Asegurar directorios
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

def clean_html(text):
    if not isinstance(text, str) or not text.strip():
        return ""
    try:
        soup = BeautifulSoup(text, "html.parser")
        return soup.get_text(separator=" ").strip()
    except Exception:
        return text

def get_category_from_api(data):
    """
    Consulta la API local.
    """
    title = data.get('title', '')
    description = data.get('description', '')
    body_snippet = data.get('body_snippet', '')
    
    full_desc = f"{description or ''} {body_snippet or ''}".strip()
    full_desc = full_desc[:900] 

    payload = {
        "title": title,
        "description": full_desc
    }
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": API_KEY
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=60)
        if response.status_code == 200:
            resp_json = response.json()
            # Intenta obtener el path, si falla, el nombre
            return resp_json.get("category_path") or resp_json.get("category_name", "")
        else:
            return ""
    except Exception:
        return ""

def extract_attributes_to_dict(row, attr_pairs):
    attrs = {}
    for col_name, col_val in attr_pairs:
        k = str(row.get(col_name, "")).strip()
        v = str(row.get(col_val, "")).strip()
        if k and k.lower() not in ['nan', 'none', '']:
            attrs[k] = v
    return attrs

def merge_attributes(parent_attrs, variants_attrs_list):
    final_attrs = parent_attrs.copy()
    collected_values = {}
    
    for k, v in parent_attrs.items():
        if k not in collected_values: collected_values[k] = set()
        if v: collected_values[k].add(v)
        
    for v_attr in variants_attrs_list:
        for k, v in v_attr.items():
            if k not in collected_values: collected_values[k] = set()
            if v: collected_values[k].add(v)
            
    for k, values_set in collected_values.items():
        val_list = sorted(list(values_set))
        if len(val_list) == 0: final_attrs[k] = ""
        elif len(val_list) == 1: final_attrs[k] = val_list[0]
        else: final_attrs[k] = val_list
            
    return final_attrs

def process_csv(filepath):
    filename = os.path.basename(filepath)
    print(f"\n--- Procesando: {filename} ---")

    try:
        df = pd.read_csv(filepath, sep=';', dtype=str, encoding='utf-8', on_bad_lines='skip')
    except Exception as e:
        print(f"Error leyendo {filename}: {e}")
        return

    df.columns = df.columns.str.strip()

    # Identificar columnas de atributos
    attr_pairs = []
    for col in df.columns:
        if col.startswith("nombre_atributo_"):
            suffix = col.split("_")[-1]
            val_col = f"valor_atributo_{suffix}"
            if val_col in df.columns:
                attr_pairs.append((col, val_col))

    print("Pre-procesando atributos y HTML...")
    df['temp_attrs'] = df.apply(lambda row: extract_attributes_to_dict(row, attr_pairs), axis=1)
    
    if 'cuerpo_es' in df.columns:
        df['cuerpo_es_clean'] = df['cuerpo_es'].apply(clean_html)
    else:
        df['cuerpo_es_clean'] = ""

    if 'tipo' not in df.columns: df['tipo'] = 'P'
    else: df['tipo'] = df['tipo'].fillna('P').str.upper()

    df_p = df[df['tipo'] == 'P'].copy()
    df_m = df[df['tipo'] == 'M'].copy()
    df_v = df[df['tipo'] == 'V'].copy()

    df_p['atributos_json'] = df_p['temp_attrs']

    processed_m_rows = []
    if not df_m.empty:
        join_col_v = 'padre' if 'padre' in df.columns else 'id_merchant'
        join_col_m = 'referencia'
        
        if join_col_v in df_v.columns and join_col_m in df_m.columns:
            grouped_vars = df_v.groupby(join_col_v)
            for _, row_m in df_m.iterrows():
                ref_id = row_m.get(join_col_m)
                child_attrs_list = []
                if ref_id and ref_id in grouped_vars.groups:
                    children = grouped_vars.get_group(ref_id)
                    child_attrs_list = children['temp_attrs'].tolist()
                
                merged_attrs = merge_attributes(row_m['temp_attrs'], child_attrs_list)
                row_m_copy = row_m.copy()
                row_m_copy['atributos_json'] = merged_attrs
                processed_m_rows.append(row_m_copy)
        else:
            df_m['atributos_json'] = df_m['temp_attrs']
            processed_m_rows = [row for _, row in df_m.iterrows()]

    df_m_final = pd.DataFrame(processed_m_rows) if processed_m_rows else pd.DataFrame()
    result_df = pd.concat([df_p, df_m_final], ignore_index=True)

    # Filtrar por imágenes
    if 'imagenes_producto' not in result_df.columns:
        print("Advertencia: No existe columna imagenes_producto")
        return

    len_before = len(result_df)
    result_df = result_df[result_df['imagenes_producto'].notna() & (result_df['imagenes_producto'].str.strip() != '')].copy()
    print(f"Filas a clasificar: {len(result_df)} (de {len_before})")

    if result_df.empty:
        print("No hay productos válidos con imágenes.")
        return

    # ---------------------------------------------------------
    # API CLASIFICACIÓN (LOCAL)
    # ---------------------------------------------------------
    print(f"Consultando API Local...")
    
    if 'nombre_es' not in result_df.columns: result_df['nombre_es'] = ""
    if 'descripcion_es' not in result_df.columns: result_df['descripcion_es'] = ""
    
    rows_data = []
    for _, row in result_df.iterrows():
        rows_data.append({
            'title': row.get('nombre_es', ''),
            'description': row.get('descripcion_es', ''),
            'body_snippet': row.get('cuerpo_es_clean', '')[:200]
        })

    # DEBUG: Probar solo el primer producto para ver si funciona
    if rows_data:
        print("DEBUG: Probando conexión API con el primer producto...")
        test_cat = get_category_from_api(rows_data[0])
        print(f"DEBUG: Respuesta API para primer producto: '{test_cat}'")

    categories = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        categories = list(tqdm(executor.map(get_category_from_api, rows_data), total=len(rows_data), unit="prod"))
    
    result_df['categoria'] = categories

    # ---------------------------------------------------------
    # EXPLOSIÓN Y SALIDA
    # ---------------------------------------------------------
    print("Generando CSV final (Explosión de imágenes)...")
    
    # CORRECCIÓN 1: Detectar separador (coma o punto y coma)
    # Algunos CSVs usan ; y otros , para separar las URLs.
    def split_images(val):
        val = str(val)
        if ';' in val:
            return [u.strip() for u in val.split(';') if u.strip()]
        else:
            return [u.strip() for u in val.split(',') if u.strip()]

    result_df['lista_imagenes'] = result_df['imagenes_producto'].apply(split_images)
    
    # CORRECCIÓN 2: Borrar la columna original antes de renombrar para evitar duplicados
    result_df = result_df.drop(columns=['imagenes_producto'])
    
    exploded_df = result_df.explode('lista_imagenes')
    
    # Renombrar la lista explotada a 'imagenes_producto'
    exploded_df = exploded_df.rename(columns={'lista_imagenes': 'imagenes_producto'})
    exploded_df = exploded_df.dropna(subset=['imagenes_producto'])

    exploded_df['atributos'] = exploded_df['atributos_json'].apply(lambda x: json.dumps(x, ensure_ascii=False))

    exploded_df = exploded_df.rename(columns={
        'nombre_es': 'titulo',
        'descripcion_es': 'descripcion',
        'cuerpo_es_clean': 'cuerpo_Es'
    })

    cols_to_keep = ['titulo', 'descripcion', 'cuerpo_Es', 'atributos', 'imagenes_producto', 'categoria']
    for col in cols_to_keep:
        if col not in exploded_df.columns: exploded_df[col] = ""

    final_output = exploded_df[cols_to_keep]

    output_filename = filename.replace('.csv', '-simplificado.csv')
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    
    try:
        final_output.to_csv(output_path, sep=';', index=False, encoding='utf-8')
        print(f"Guardado: {output_path} ({len(final_output)} filas)")
        
        # Mover original a RAW
        raw_dest = os.path.join(RAW_DIR, filename)
        shutil.move(filepath, raw_dest)
        
    except Exception as e:
        print(f"Error guardando {filename}: {e}")

def main():
    print("Iniciando proceso corregido (Explosión por comas y limpieza de columnas)...")
    csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    
    if not csv_files:
        print("No hay archivos CSV en data_in/downloaded")
        # Como los movimos a RAW, vamos a buscarlos allí para reintentar si no hay en downloaded
        # Esto es solo para facilitarte la prueba ahora mismo.
        print("Buscando en RAW para reintentar corrección...")
        csv_files = glob.glob(os.path.join(RAW_DIR, "*.csv"))

    for filepath in csv_files:
        process_csv(filepath)

if __name__ == "__main__":
    main()
