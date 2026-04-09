import pandas as pd
import numpy as np
import io
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# --- CONFIGURACIÓN DE RUTAS ---
# Asegúrate de que el archivo 'listings.csv' esté en esta ruta de tu VM
BASE_DIR = "/home/vboxuser/Documentos/proy_SSDD_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "listings.csv")
OUTPUT_FILE = "/home/vboxuser/Documentos/resultados_listings_final.csv"

# --- DEFINICIÓN DEL DAG ---
@dag(
    dag_id='airbnb_listings_taskflow_v1',
    schedule=None,              # Se ejecuta manualmente desde la UI
    start_date=datetime(2026, 3, 27),
    catchup=False,
    tags=['malaga', 'listings', 'taskflow'],
    default_args={
        'owner': 'vboxuser',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def airbnb_listings_pipeline():

    @task()
    def extract():
        """1. EXTRACT: Carga el archivo CSV desde el disco local."""
        print(f"Buscando archivo en: {PATH_LOCAL_CSV}")
        
        if not os.path.exists(PATH_LOCAL_CSV):
            raise FileNotFoundError(f"No se encuentra el archivo en {PATH_LOCAL_CSV}. "
                                    f"Verifica que el nombre sea 'listings.csv'.")
        
        # Leemos el CSV (Ajustamos encoding por si tiene tildes de Málaga)
        df = pd.read_csv(PATH_LOCAL_CSV, encoding='utf-8')
        print(f"ÉXITO: {len(df)} filas cargadas en memoria.")
        
        # Pasamos el DataFrame como JSON a la siguiente tarea
        return df.to_json()

    @task()
    def transform(json_data):
        """2. TRANSFORM: Limpieza y Enriquecimiento en RAM."""
        # Convertimos el JSON de vuelta a DataFrame
        df = pd.read_json(io.StringIO(json_data))
        print("Iniciando transformaciones...")

        # A. Limpieza de columnas innecesarias
        cols_drop = ['neighbourhood_group_cleansed', 'batrooms_text', 'scrape_id', 'license']
        df = df.drop(columns=[c for c in cols_drop if c in df.columns])

        # B. Formateo de precios
        if 'price' in df.columns:
            # Quitamos '$' y ',' para poder operar matemáticamente
            df['price_num'] = df['price'].replace('[\$,]', '', regex=True).astype(float)

        # C. Enriquecimiento Geográfico (Málaga Centro)
        # Coordenadas: 36.7213, -4.4214
        LAT_MGA, LON_MGA = 36.7213, -4.4214
        
        # Cálculo de distancia euclídea aproximada (Haversine simplificado)
        df['dist_center_km'] = np.sqrt((df['latitude'] - LAT_MGA)**2 + 
                                       (df['longitude'] - LON_MGA)**2) * 111
        
        # Flag de cercanía (menos de 1.5 km del centro)
        df['is_prime_location'] = (df['dist_center_km'] < 1.5).astype(int)

        print(f"Transformación completada. Columnas: {len(df.columns)}")
        return df.to_json()

    @task()
    def load(json_final):
        """3. LOAD: Guardado del CSV final enriquecido."""
        df = pd.read_json(io.StringIO(json_final))
        
        # Aseguramos que la carpeta de salida existe
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"PROCESO FINALIZADO. Archivo guardado en: {OUTPUT_FILE}")
        print(f"Registros finales: {len(df)}")

    # --- FLUJO DE TRABAJO ---
    # La TaskFlow API conecta las tareas automáticamente mediante los argumentos
    raw_data = extract()
    transformed_data = transform(raw_data)
    load(transformed_data)

# Instanciamos el DAG para que Airflow lo registre
dag_instance = airbnb_listings_pipeline()