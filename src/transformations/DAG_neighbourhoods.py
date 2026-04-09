import pandas as pd
import geopandas as gpd
from airflow.decorators import task, dag
from datetime import datetime
import matplotlib.pyplot as plt
import os
import io 

# Rutas de entrada y salida
DATA_DIR      = "/opt/airflow/data"
PATH_CSV      = f"{DATA_DIR}/neighbourhoods.csv"
PATH_GEOJSON  = f"{DATA_DIR}/neighbourhoods.geojson"
PATH_LISTINGS = f"{DATA_DIR}/listings.csv"
# Carpeta para archivos intermedios Parquet
TMP_DIR       = f"{DATA_DIR}/tmp_parquet"

@dag(
    schedule=None, 
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["malaga", "barrios"],
)
def dag_neighbourhoods():

    @task()
    def extract():
        os.makedirs(TMP_DIR, exist_ok=True)
        
        df_neigh = pd.read_csv(PATH_CSV)
        gdf = gpd.read_file(PATH_GEOJSON)
        df_list = pd.read_csv(PATH_LISTINGS, usecols=["id", "neighbourhood", "price", "availability_365", "number_of_reviews", "room_type"])

        # Guardar como Parquet
        p1 = f"{TMP_DIR}/neigh.parquet"
        p2 = f"{TMP_DIR}/gdf.parquet"
        p3 = f"{TMP_DIR}/list.parquet"

        df_neigh.to_parquet(p1)
        gdf.to_parquet(p2) # GeoPandas soporta geoparquet nativamente
        df_list.to_parquet(p3)

        return {"p_neigh": p1, "p_gdf": p2, "p_list": p3}

    @task()
    def transform(paths):
        df_neighbourhoods = pd.read_parquet(paths["p_neigh"])
        gdf = gpd.read_parquet(paths["p_gdf"])

        GRUPOS_BARRIOS = {
            "Centro"   : ["Centro"],
            "Este"     : ["Este"],
            "Oeste"    : ["Carretera de Cadiz", "Cruz De Humilladero", "Teatinos-Universidad", "Churriana", "Campanillas", "Puerto de la Torre"],
            "Norte"    : ["Bailen-Miraflores", "Palma-Palmilla", "Ciudad Jardin"],
        }

        mapa_barrio_grupo = {barrio: grupo for grupo, barrios in GRUPOS_BARRIOS.items() for barrio in barrios}

        df_neighbourhoods["neighbourhood_group"] = df_neighbourhoods["neighbourhood"].map(mapa_barrio_grupo)
        gdf["neighbourhood_group"] = gdf["neighbourhood"].map(mapa_barrio_grupo)

        # Sobrescribir archivos intermedios
        df_neighbourhoods.to_parquet(paths["p_neigh"])
        gdf.to_parquet(paths["p_gdf"])

        return paths 


    @task()
    def enrichment(paths):
        df_neighbourhoods = pd.read_parquet(paths["p_neigh"])
        gdf = gpd.read_parquet(paths["p_gdf"])
        df_listings = pd.read_parquet(paths["p_list"])

        df_listings['price'] = df_listings['price'].replace(r'[\$,]', '', regex=True).astype(float)

        stats_barrios = df_listings.groupby('neighbourhood').agg(
            num_anuncios=('id', 'count'),
            precio_medio=('price', 'mean'),
            disponibilidad_media=('availability_365', 'mean'),
            reviews_totales=('number_of_reviews', 'sum')
        ).reset_index()

        df_neighbourhoods['neighbourhood'] = df_neighbourhoods['neighbourhood'].str.strip()
        gdf['neighbourhood'] = gdf['neighbourhood'].str.strip()
        stats_barrios['neighbourhood'] = stats_barrios['neighbourhood'].str.strip()

        df_final = df_neighbourhoods.merge(stats_barrios, on='neighbourhood', how='left').fillna(0)
        gdf_final = gdf.merge(stats_barrios, on='neighbourhood', how='left').fillna(0)

        columnas = ['neighbourhood', 'neighbourhood_group', 'num_anuncios', 'precio_medio', 'disponibilidad_media', 'reviews_totales']
        df_final = df_final[columnas].round(2)
        gdf_final = gdf_final[columnas + ['geometry']].round(2)

        # Guardar resultados finales en Parquet
        p_final_df = f"{TMP_DIR}/df_final.parquet"
        p_final_gdf = f"{TMP_DIR}/gdf_final.parquet"
        
        df_final.to_parquet(p_final_df)
        gdf_final.to_parquet(p_final_gdf)

        return {"df_final": p_final_df, "gdf_final": p_final_gdf}
    
    @task()
    def eda(paths_enriched):
        PLOT_DIR = f"{DATA_DIR}/eda_plots"
        os.makedirs(PLOT_DIR, exist_ok=True)

        df = pd.read_parquet(paths_enriched["df_final"])
        gdf = gpd.read_parquet(paths_enriched["gdf_final"])

        # Visualizaciones (mantengo tu lógica original)
        COLORES = {"Centro": "#e63946", "Este": "#2a9d8f", "Oeste": "#e9c46a", "Norte": "#457b9d"}
        gdf["color"] = gdf["neighbourhood_group"].map(COLORES)
        
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf.plot(ax=ax, color=gdf["color"], edgecolor="white")
        plt.savefig(os.path.join(PLOT_DIR, "1_mapa_grupos.png"))
        plt.close(fig)

        # ... (puedes añadir el resto de plots aquí igual que antes)

    @task()
    def load(paths_enriched):
        df = pd.read_parquet(paths_enriched["df_final"])
        output_path = f"{DATA_DIR}/enriched_neighbourhoods.csv"
        df.to_csv(output_path, index=False)
        print(f"ÉXITO: Guardado en {output_path}")

    # Pipeline
    data_raw_paths = extract()
    data_cleaned_paths = transform(data_raw_paths)
    data_enriched_paths = enrichment(data_cleaned_paths)
    eda(data_enriched_paths)
    load(data_enriched_paths)

dag_instance = dag_neighbourhoods()
