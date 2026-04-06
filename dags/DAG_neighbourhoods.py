import pandas as pd
import geopandas as gpd
from airflow.decorators import task, dag
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import os
import io 

# Rutas de archivos
PATH_CSV      = "/opt/airflow/data/neighbourhoods.csv"
PATH_GEOJSON  = "/opt/airflow/data/neighbourhoods.geojson"
PATH_LISTINGS = "/opt/airflow/data/listings.csv"

@dag(
    schedule=None, 
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["malaga", "barrios"],
)
def dag_neighbourhoods():

    @task()
    def extract():
        df_neigh = pd.read_csv(PATH_CSV)
        gdf = gpd.read_file(PATH_GEOJSON)
        df_list = pd.read_csv(PATH_LISTINGS, usecols=["id", "neighbourhood", "price", "availability_365", "number_of_reviews", "room_type"])

        return {
            "df_neighbourhoods": df_neigh.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": df_list.to_json()
        }

    @task()
    def transform(extract_data):
        df_neighbourhoods = pd.read_json(io.StringIO(extract_data["df_neighbourhoods"]))
        gdf = gpd.read_file(io.StringIO(extract_data["gdf"]), driver='GeoJSON')

        GRUPOS_BARRIOS = {
            "Centro"   : ["Centro"],
            "Este"     : ["Este"],
            "Oeste"    : ["Carretera de Cadiz", "Cruz De Humilladero", "Teatinos-Universidad", "Churriana", "Campanillas", "Puerto de la Torre"],
            "Norte"    : ["Bailen-Miraflores", "Palma-Palmilla", "Ciudad Jardin"],
        }

        mapa_barrio_grupo = {barrio: grupo for grupo, barrios in GRUPOS_BARRIOS.items() for barrio in barrios}

        df_neighbourhoods["neighbourhood_group"] = df_neighbourhoods["neighbourhood"].map(mapa_barrio_grupo)
        gdf["neighbourhood_group"] = gdf["neighbourhood"].map(mapa_barrio_grupo)

        return {
            "df_neighbourhoods": df_neighbourhoods.to_json(),
            "gdf": gdf.to_json(),
            "df_listings": extract_data["df_listings"] 
        }

    @task()
    def enrichment(cleaned_data):
        df_neighbourhoods = pd.read_json(io.StringIO(cleaned_data["df_neighbourhoods"]))
        gdf = gpd.read_file(io.StringIO(cleaned_data["gdf"]), driver='GeoJSON')
        df_listings = pd.read_json(io.StringIO(cleaned_data["df_listings"]))

        # Limpieza de precios
        df_listings['price'] = df_listings['price'].replace(r'[\$,]', '', regex=True).astype(float)

        # Agregación
        stats_barrios = df_listings.groupby('neighbourhood').agg(
            num_anuncios=('id', 'count'),
            precio_medio=('price', 'mean'),
            disponibilidad_media=('availability_365', 'mean'),
            reviews_totales=('number_of_reviews', 'sum')
        ).reset_index()

        # Merges con limpieza de espacios
        df_neighbourhoods['neighbourhood'] = df_neighbourhoods['neighbourhood'].str.strip()
        gdf['neighbourhood'] = gdf['neighbourhood'].str.strip()
        stats_barrios['neighbourhood'] = stats_barrios['neighbourhood'].str.strip()

        df_final = df_neighbourhoods.merge(stats_barrios, on='neighbourhood', how='left').fillna(0)
        gdf_final = gdf.merge(stats_barrios, on='neighbourhood', how='left').fillna(0)

        # Reordenar
        columnas = ['neighbourhood', 'neighbourhood_group', 'num_anuncios', 'precio_medio', 'disponibilidad_media', 'reviews_totales']
        df_final = df_final[columnas].round(2)
        gdf_final = gdf_final[columnas + ['geometry']].round(2)

        return {
            "df_final": df_final.to_json(orient='records'),
            "gdf_final": gdf_final.to_json()
        }
    
    @task()
    def eda(data_enriched):
        PLOT_DIR = "/opt/airflow/data/eda_plots"
        os.makedirs(PLOT_DIR, exist_ok=True)

        df = pd.read_json(io.StringIO(data_enriched["df_final"]))
        gdf = gpd.read_file(io.StringIO(data_enriched["gdf_final"]), driver='GeoJSON')

        # 1. Mapa de barrios
        COLORES = {"Centro": "#e63946", "Este": "#2a9d8f", "Oeste": "#e9c46a", "Norte": "#457b9d"}
        gdf["color"] = gdf["neighbourhood_group"].map(COLORES)
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf.plot(ax=ax, color=gdf["color"], edgecolor="white")
        plt.savefig(os.path.join(PLOT_DIR, "1_mapa_grupos.png"))
        plt.close(fig)

        # 2. Top Caros
        top = df.sort_values('precio_medio', ascending=False).head(15)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(top['neighbourhood'], top['precio_medio'], color='skyblue')
        ax.invert_yaxis()
        plt.savefig(os.path.join(PLOT_DIR, "2_top_caros.png"))
        plt.close(fig)

        # 3. Disponibilidad
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf.plot(column='disponibilidad_media', cmap='RdYlGn_r', legend=True, ax=ax)
        plt.savefig(os.path.join(PLOT_DIR, "3_disponibilidad.png"))
        plt.close(fig)

        # 4. Densidad
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf.plot(column='num_anuncios', cmap='YlOrRd', legend=True, ax=ax)
        plt.savefig(os.path.join(PLOT_DIR, "4_densidad.png"))
        plt.close(fig)

    @task()
    def load(final_data):
        df = pd.read_json(io.StringIO(final_data["df_final"]))
        output_path = "../data/enriched_neighbourhoods.csv"
        df.to_csv(output_path, index=False)
        print(f"ÉXITO: Guardado en {output_path}")

    # Ejecución del Pipeline
    data_raw = extract()
    data_cleaned = transform(data_raw)
    data_enriched = enrichment(data_cleaned)
    eda(data_enriched)
    load(data_enriched)

dag_instance = dag_neighbourhoods()