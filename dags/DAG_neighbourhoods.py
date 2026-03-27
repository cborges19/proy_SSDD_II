import pandas as pd
import geopandas as gpd
from airflow.decorators import task, dag
from datetime import datetime
import json

# Rutas de archivos
PATH_CSV = "../data/neighbourhoods.csv"
PATH_GEOJSON = "../data/neighbourhoods.geojson"
PATH_LISTINGS = "../data/listings.csv"

@dag(
    schedule=None, 
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["malaga", "barrios", "enrichment"],
)
def dag_neighbourhoods_etl():

    @task()
    def extract():
        """1. Extracción: Carga de archivos base"""
        df_neigh = pd.read_csv(PATH_CSV)
        gdf = gpd.read_file(PATH_GEOJSON)
        # Cargamos listings solo con las columnas necesarias para enriquecer
        df_list = pd.read_csv(PATH_LISTINGS, usecols=["id", "neighbourhood"])

        return {
            "df_neighbourhoods": df_neigh.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": df_list.to_json()
        }

    @task()
    def transform_cleaning(data):
        """2. Limpieza: Eliminación de columnas irrelevantes"""
        df_neighbourhoods = pd.read_json(data["df_neighbourhoods"])
        gdf = gpd.read_json(data["gdf"])

        # Eliminamos 'neighbourhood_group' si existe (limpieza básica)
        cols_to_drop = ["neighbourhood_group"]
        df_neighbourhoods = df_neighbourhoods.drop(columns=[c for c in cols_to_drop if c in df_neighbourhoods.columns])
        gdf = gdf.drop(columns=[c for c in cols_to_drop if c in gdf.columns])

        return {
            "df_neighbourhoods": df_neighbourhoods.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": data["df_listings"] 
        }

    @task()
    def transform_enrichment(cleaned_data):
        """3. Enriquecimiento: Cruce con listings y cálculo de KPIs"""
        df_neighbourhoods = pd.read_json(cleaned_data["df_neighbourhoods"])
        gdf = gpd.read_json(cleaned_data["gdf"])
        df_listings = pd.read_json(cleaned_data["df_listings"])

        # --- CÁLCULO DE MÉTRICAS (Enrichment) ---
        # Contamos cuántos anuncios hay por barrio
        stats_barrios = df_listings.groupby('neighbourhood').agg(
            num_anuncios=('id', 'count')
        ).reset_index()

        # Calculamos el % que representa cada barrio sobre el total de Málaga
        total_anuncios = stats_barrios['num_anuncios'].sum()
        stats_barrios['pct_total_anuncios'] = (stats_barrios['num_anuncios'] / total_anuncios) * 100

        # --- MERGE / UNIÓN ---
        # Unimos las métricas al DataFrame de barrios
        df_final = df_neighbourhoods.merge(stats_barrios, on='neighbourhood', how='left')
        df_final[['num_anuncios', 'pct_total_anuncios']] = df_final[['num_anuncios', 'pct_total_anuncios']].fillna(0)

        # Unimos las métricas al GeoJSON para que el mapa tenga datos de densidad
        gdf_final = gdf.merge(stats_barrios, on='neighbourhood', how='left')
        gdf_final[['num_anuncios', 'pct_total_anuncios']] = gdf_final[['num_anuncios', 'pct_total_anuncios']].fillna(0)

        return {
            "df_final": df_final.to_json(orient='records'),
            "gdf_final": gdf_final.to_json()
        }
    
    @task()
    def load(final_data):
        """4. Carga: Persistencia y preparación para Kafka"""
        # Recuperamos el DataFrame enriquecido
        df = pd.read_json(final_data["df_final"])
        
        # Guardamos el CSV enriquecido
        output_path = "../data/enriched_neighbourhoods.csv"
        df.to_csv(output_path, index=False)
        
        # Simulación de envío a Kafka (formato JSON línea a línea)
        kafka_payload = df.to_json(orient='records')
        
        print(f"ÉXITO: {len(df)} barrios enriquecidos guardados en {output_path}.")
        print("Payload listo para Kafka.")

    # Flujo de ejecución
    raw = extract()
    cleaned = transform_cleaning(raw)
    enriched = transform_enrichment(cleaned)
    load(enriched)

# Ejecutar el DAG
dag_instance = dag_neighbourhoods_etl()
