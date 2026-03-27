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
    tags=["malaga", "barrios"],
)
def dag_neighbourhoods():

    @task()
    def extract():
        #1. Extracción: Carga de archivos base
        df_neigh = pd.read_csv(PATH_CSV)
        gdf = gpd.read_file(PATH_GEOJSON)
        # Cargamos listings solo con las columnas necesarias para enriquecer
        df_list = pd.read_csv(PATH_LISTINGS, usecols=["id", "neighbourhood", "price", "availability_365", "number_of_reviews", "room_type"])

        extract_data = {
            "df_neighbourhoods": df_neigh.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": df_list.to_json()
        }

        return extract_data

    @task()
    def transform(extract_data):
        # 2. Limpieza: Eliminación de columnas irrelevantes
        df_neighbourhoods = pd.read_json(extract_data["df_neighbourhoods"])
        gdf = gpd.read_json(extract_data["gdf"])

        # Definimos la variable que tenemos con valores nulos (neighbourhood_group) basándola en proximidad geográfica dentro de Málaga
        GRUPOS_BARRIOS = {
            "Centro"   : ["Centro"],
            "Este"     : ["Este"],
            "Oeste"    : ["Carretera de Cadiz", "Cruz De Humilladero", "Teatinos-Universidad"],
            "Norte"    : ["Bailen-Miraflores", "Palma-Palmilla", "Ciudad Jardin"],
            "Periferia": ["Churriana", "Campanillas", "Puerto de la Torre"],
        }

        # Construir mapa (barrio: grupo)
        mapa_barrio_grupo = {
            barrio: grupo
            for grupo, barrios in GRUPOS_BARRIOS.items()
            for barrio in barrios
        }

        # 4.2 Asignar neighbourhood_group al CSV y al GeoJSON 
        df_neighbourhoods["neighbourhood_group"] = (
            df_neighbourhoods["neighbourhood"].map(mapa_barrio_grupo)
        )

        gdf["neighbourhood_group"] = (
            gdf["neighbourhood"].map(mapa_barrio_grupo)
        )

        data_cleaned = {
            "df_neighbourhoods": df_neighbourhoods.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": extract_data["df_listings"] 
        }

        return data_cleaned

    @task()
    def enrichment(cleaned_data):
        # Data enrichment
        df_neighbourhoods = pd.read_json(cleaned_data["df_neighbourhoods"])
        gdf = gpd.read_json(cleaned_data["gdf"])
        df_listings = pd.read_json(cleaned_data["df_listings"])

        # 1. Lista de columnas que estamos añadiendo (para poder limpiar)
        cols_enrichment = ['num_anuncios', 'precio_medio', 'disponibilidad_media', 'reviews_totales']

        # 3. Calculamos las estadísticas de listings
        stats_barrios = df_listings.groupby('neighbourhood').agg(
            num_anuncios=('id', 'count'),
            precio_medio=('price', 'median'),
            disponibilidad_media=('availability_365', 'mean'),
            reviews_totales=('number_of_reviews', 'sum')
        ).reset_index()

        # 4. Realizamos la unión (MERGE)
        df_neighbourhoods_final = df_neighbourhoods.merge(stats_barrios, on='neighbourhood', how='left')
        gdf_final = gdf.merge(stats_barrios, on='neighbourhood', how='left')

        data_enriched = {
            "df_final": df_neighbourhoods_final.to_json(orient='records'),
            "gdf_final": gdf_final.to_json()
        }

        return data_enriched
    
    @task()
    def load(final_data):
        # 4. Carga: Persistencia y preparación para Kafka
        # Recuperamos el DataFrame ya enriquecido
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
    cleaned = transform(raw)
    enriched = enrichment(cleaned)
    load(enriched)

# Ejecutar el DAG
dag_instance = dag_neighbourhoods()
