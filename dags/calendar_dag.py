import os
import pandas as pd
import numpy as np
from airflow.decorators import dag, task
import logging

BASE_DIR = "/home/vboxuser/SDPD2/proy_SSDD_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "calendar.csv")
OUTPUT_DIR= "/home/vboxuser/SDPD2/"

@dag(
    dag_id='airbnb_calendar_taskflow',
)

def calendar_pipeline():
    
    @task()
    def extract() -> str:
        dtype_dict = {
            'listing_id': 'int64',
            'date': 'str',
            'available': 'str',
            'price': 'str',
            'adjusted_price': 'str',
            'minimum_nights': 'int64',
            'maximum_nights': 'int64',
        }

        df = pd.read_csv(PATH_LOCAL_CSV, dtype=dtype_dict) 

        # Guardamoe el DataFrame en formato Parquet
        processed_path = os.path.join(OUTPUT_DIR, "processed_calendar.parquet")
        df.to_parquet(processed_path, index=False)

        return processed_path

    log = logging.getLogger(__name__)

    def handle_missing_values(df, variable, threshold=0.8):
        na_count = df[variable].isna().sum()
        if na_count > threshold*len(df):
            log.info(f"Dropping variable {variable} due to high missing percentage.")
            df.drop(columns=[variable], inplace=True)
        else:
            log.info(f"Keeping variable {variable} with missing percentage below threshold.")
            df[variable] = df[variable].str.replace(r'[\$,]', '', regex=True).astype(float)

        return df

    @task()
    def clean(processed_path: str) -> str:
        # Load dataset
        df = pd.read_parquet(processed_path)
        log.info(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

        # Date conversion
        df['date'] = pd.to_datetime(df['date'], errors='coerce') # coerce para datos faltantes que pasen a ser NA
        date_na = df['date'].isna().sum()
        if date_na > 0:
            log.warning(f"Found {date_na} missing values in 'date' column after conversion.")

        # Available convesion to boolean
        df['available'] = df['available'].map({'t': True, 'f': False}) # Convertir available a booleano
        log.info('Available variable converted to boolean.')

        # Dinamic gestión for price an adjusted_price variables
        for col in ['price', 'adjusted_price']:
            if col in df.columns:
                df = handle_missing_values(df, col)

        log.info(f'Dataset cleaned: {df.shape[0]} rows, {df.shape[1]} columns')

        # Guardar el DataFrame limpio en formato Parquet
        cleaned_path = os.path.join(OUTPUT_DIR, "cleaned_calendar.parquet")
        df.to_parquet(cleaned_path, index=False)
        log.info(f'Cleaned dataset saved to {cleaned_path}')

        return cleaned_path

    @task()
    def tranform(pd.DataFrame) -> pd.DataFrame:

    @task()
    def EDA(pd.DataFrame) -> str:
    
    @task()
    def load(pd.DataFrame) -> str: