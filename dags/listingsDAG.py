from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import requests
import os

# --- RUTAS DE ARCHIVOS (SIMULANDO TU SISTEMA DE ARCHIVOS EN LA VM) ---
DATA_DIR = "/home/usuario/data_airbnb/"
RAW_FILE = os.path.join(DATA_DIR, "listings_raw.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "listings_cleaned.csv")
FINAL_FILE = os.path.join(DATA_DIR, "listings_final_malaga.csv")

# 1. FUNCIÓN DE EXTRACCIÓN (EXTRACT)
def extract_data():
    """Descarga los datos de Málaga y los guarda en local."""
    url = "https://data.insideairbnb.com/spain/andaluc%C3%ADa/malaga/2025-09-30/data/listings.csv.gzz"
    print(f"Descargando desde {url}...")
    
    # En un entorno real usarías requests + gzip como hicimos antes
    # Aquí simulamos la descarga y guardado del CSV
    response = requests.get(url)
    with open(RAW_FILE, 'wb') as f:
        f.write(response.content)
    print("Extracción completada.")

# 2. FUNCIÓN DE LIMPIEZA (TASK 10 - TRANSFORM)
def clean_data():
    """Elimina variables innecesarias y gestiona nulos básicos."""
    df = pd.read_csv(RAW_FILE, compression='gzip') # Leemos el .gz directamente
    
    cols_to_drop = ['scrape_id', 'picture_url', 'host_thumbnail_url', 'license', 'calendar_updated']
    df_cleaned = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    # Ejemplo de limpieza: convertir precio a numérico
    if 'price' in df_cleaned.columns:
        df_cleaned['price_num'] = df_cleaned['price'].replace('[\$,]', '', regex=True).astype(float)
    
    df_cleaned.to_csv(CLEAN_FILE, index=False)
    print(f"Limpieza completada. Columnas finales: {df_cleaned.shape[1]}")

# 3. FUNCIÓN DE ENRIQUECIMIENTO (TASK 11 - ENRICH)
def enrich_data():
    """Añade geolocalización y métricas de confianza."""
    df = pd.read_csv(CLEAN_FILE)
    
    # Coordenadas Málaga (Calle Larios)
    LAT_MGA, LON_MGA = 36.7213, -4.4214
    
    # Cálculo de distancia (simplificado para el ejemplo)
    df['dist_center_km'] = np.sqrt((df['latitude'] - LAT_MGA)**2 + (df['longitude'] - LON_MGA)**2) * 111
    
    # Flag de "Superhost Profesional"
    df['is_pro_host'] = (df['calculated_host_listings_count'] > 1).astype(int)
    
    df.to_csv(FINAL_FILE, index=False)
    print("Enriquecimiento completado. Archivo final generado.")

# --- DEFINICIÓN DEL DAG ---
default_args = {
    'owner': 'tu_usuario',
    'start_date': datetime(2026, 3, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_airbnb_malaga_v1',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
) as dag:

    # Definición de los nodos (Tasks)
    t1 = PythonOperator(task_id='extract', python_callable=extract_data)
    t2 = PythonOperator(task_id='clean_variables', python_callable=clean_data)
    t3 = PythonOperator(task_id='enrich_data', python_callable=enrich_data)

    # El flujo (Pipeline)
    t1 >> t2 >> t3