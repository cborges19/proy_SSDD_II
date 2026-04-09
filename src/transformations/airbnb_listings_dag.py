import pandas as pd
import numpy as np
import io
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

import re
import pyarrow

BASE_DIR = "/home/vboxuser/Documentos/proy_SSDD_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "listings.csv")
OUTPUT_FILE = "/home/vboxuser/Documentos/resultados_listings_final.csv"
EDA_OUTPUT_DIR = "/home/vboxuser/Documentos/outputs/"
SCRAPE_DATE = pd.Timestamp('2025-09-30')


# --- DEFINICIÓN DEL DAG ---
@dag(
    dag_id='airbnb_listings_taskflow_v1',
    schedule=None,             
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
        df = pd.read_csv(PATH_LOCAL_CSV, encoding='utf-8')
        output_path = "/tmp/listings_processed.parquet"
        df.to_parquet(output_path, engine='pyarrow', index=False)
        
        print(f"Archivo guardado en {output_path}")
        return output_path 
    @task()
    def transform(path_archivo):
        """2. TRANSFORM: Limpieza y Enriquecimiento en RAM."""
        # Convertimos el JSON de vuelta a DataFrame
        df = pd.read_parquet(path_archivo)
        print("Iniciando transformaciones...")
        ################################ TRANSFORMACION PRECIO ############################3
        # El precio viene con un simbolo de dolar ($), lo eliminamos.
        df['price_num'] = (
            df['price']
            .str.replace(r'[\$,]', '', regex=True)
            .astype(float)
        )

        n_zero = (df['price_num'] == 0).sum()
        df.loc[df['price_num'] == 0, 'price_num'] = np.nan
        print(f'Precios igual a 0 anulados: {n_zero}')
        print('clean_price OK ')

        ###########################3 TRANSFORMACION BOOLEANOS #######################333
        # Homogeneizamos columnas con valores t/f a True/False.

        BOOL_COLS = [
            'host_is_superhost',
            'host_has_profile_pic',
            'host_identity_verified',
            'instant_bookable',
            'has_availability'
        ]
        bool_cols_present = [c for c in BOOL_COLS if c in df.columns]

        for col in bool_cols_present:
            original_nulls = df[col].isnull().sum()
            df[col] = df[col].map({'t': True, 'f': False})
            new_nulls = df[col].isnull().sum()
            if new_nulls > original_nulls:
                print(f'  {col}: {new_nulls - original_nulls} valores inesperados → NaN')
            else:
                print(f'{col}: OK')

        print('clean_booleans OK ')

        ##################### TRANSFORMACION RATIOS ####################################
        # Transformamos ratios (%) a formato decimal 0-1 en la misma variable
        RATE_COLS = ['host_response_rate', 'host_acceptance_rate']

        for col in RATE_COLS:
            if col not in df.columns:
                print(f'  {col} no encontrada, se omite')
                continue

            # 1. Limpieza y conversión a float en la misma columna
            # Usamos errors='coerce' por seguridad si hay valores no numéricos
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace('%', '', regex=False), 
                    errors='coerce'
                ) / 100
            )

            # 2. Validación de rango (ahora sobre la columna original modificada)
            # Filtramos valores que no estén entre 0 y 1 (ej. si alguien puso 150%)
            out_of_range = (
                ~df[col].between(0, 1, inclusive='both') &
                df[col].notna()
            ).sum()

            if out_of_range:
                print(f'  {col}: {out_of_range} valores fuera de [0,1] → NaN')
                df.loc[~df[col].between(0, 1, inclusive='both'), col] = np.nan
            else:
                print(f'  {col}: rango OK (formato 0-1)')

        print('clean_rates OK (Variables originales actualizadas)')
        ###################### TRANSFROMACION FECHAS ######################################
        # Parseamos las fechas y detectamos posibles valores invalidos
        DATE_COLS = ['host_since', 'first_review', 'last_review']

        for col in DATE_COLS:
            if col not in df.columns:
                print(f'  {col} no encontrada, se omite')
                continue

            # Convertir a datetime
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Fechas futuras respecto al scraping - inválidas
            invalid_future = (df[col] > SCRAPE_DATE).sum()
            if invalid_future:
                print(f'  {col}: {invalid_future} fechas futuras detectadas → NaT')
                df.loc[df[col] > SCRAPE_DATE, col] = pd.NaT
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!! VALIDACION #####################################################################333
        # Inconsistencia lógica: first_review posterior a last_review
        if 'first_review' in df.columns and 'last_review' in df.columns:
            invalid_order = (
                df['first_review'].notna() &
                df['last_review'].notna() &
                (df['first_review'] > df['last_review'])
            ).sum()
            
            if invalid_order:
                print(f'  Inconsistencia: first_review > last_review en {invalid_order} filas → Reseteando a NaT')
                mask = df['first_review'] > df['last_review']
                df.loc[mask, ['first_review', 'last_review']] = pd.NaT
            else:
                print('  Orden first_review / last_review: OK')
        print('clean_dates OK')
        #####################################################################################3
        
        ######################### TRANSFORMACIÓN BATHROOMS .################################3
        #  la cruzamos con bathroom_text para rellenar valores.

        df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')

        # 2. Validación cruzada con bathrooms_text
        if 'bathrooms_text' in df.columns:
            def parse_bathrooms_text(text):
                if pd.isna(text): return np.nan
                text = text.lower().strip()
                if 'half' in text or 'medio' in text: 
                    return 0.5
                match = re.search(r'[\d\.]+', text)
                return float(match.group()) if match else np.nan

            # Creamos columna temporal para comparar
            df['bathrooms_text_num'] = df['bathrooms_text'].apply(parse_bathrooms_text)
            
        #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! VALIDACION ###############################################33
            # Calcular discrepancias
            mask_mismatch = (
                df['bathrooms'].notna() & 
                df['bathrooms_text_num'].notna() & 
                (df['bathrooms'] != df['bathrooms_text_num'])
            )
            mismatch = mask_mismatch.sum()
            
            print(f"  -> Discrepancias encontradas: {mismatch}")
            
            recovered = (df['bathrooms'].isna() & df['bathrooms_text_num'].notna()).sum()
            if recovered:
                print(f"  -> Recuperando {recovered} valores de 'bathrooms' desde el texto...")
                df['bathrooms'] = df['bathrooms'].fillna(df['bathrooms_text_num'])

        print('clean_bathrooms OK')
        print(f"Nulos finales en 'bathrooms': {df['bathrooms'].isnull().sum()}")
        ##########################################################################################################

        ################################33 IS_SUPERHOST? ################################################
        # Imputar valores nulos de la variable host_is_superhost usando las variables number_of_reviews,
        #  host_response_rate, host_acceptance_rate y reviews_scores_rating (agrupado por host_id).

        # Rellenar nulos dentro del mismo host usando el valor más frecuente (moda) o el primero disponible
        # Rellenar nulos usando el valor más común del mismo host_id
        df['host_is_superhost'] = df.groupby('host_id')['host_is_superhost'].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else pd.NA)
        )
        # Segun AirBNB un superhost debe
        # review_scores_rating >= 4.8
        # host_response_rate >= 90%
        # host_acceptance_rate >= 90%
        # number_of_reviews > 0
        condicion_superhost = (
            (df['review_scores_rating'] >= 4.8) & 
            (df['host_response_rate'] >= 0.9) & 
            (df['host_acceptance_rate'] >= 0.9) &
            (df['number_of_reviews'] >= 5)
        )

        # Solo aplicamos a los que siguen siendo nulos
        df.loc[df['host_is_superhost'].isna() & condicion_superhost, 'host_is_superhost'] = True

        # El resto de nulos que no cumplieron la condición, los marcamos como False
        df['host_is_superhost'] = df['host_is_superhost'].fillna(False)


       


        ################################# VALORES NULOS HOST_RESPONSE_TIME ######################
        # Lo hacemos mediante la moda agrupando por si el host es superhost o no
        df['host_response_time'] = df.groupby('host_is_superhost')['host_response_time'].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "unknown")
        )



        ################################## VALORES NULOS HOST_ACCEPTANCE_RATE HOST_RESPONSE_RATE ##################
        def imputar_mediana_con_ruido(serie):
            # Si el grupo está totalmente vacío, lo devolvemos tal cual
            if serie.isnull().all():
                return serie
            
            mediana = serie.median()
            desviacion = serie.std()
            
            # Si no hay desviación (solo un dato), ponemos un ruido mínimo (1%)
            if pd.isna(desviacion) or desviacion == 0:
                desviacion = 0.01
            
            nulos = serie.isna()
            total_nulos = nulos.sum()
            
            if total_nulos > 0:
                # Generar ruido
                ruido = np.random.normal(loc=0, scale=desviacion * 0.1, size=total_nulos)
                # Calcular nuevos valores y asegurar rango [0, 1]
                valores_uevos = np.clip(mediana + ruido, 0, 1)
                
                # REEMPLAZO SEGURO:
                # Creamos una serie con los nuevos valores y el índice correcto
                reemplazos = pd.Series(valores_uevos, index=serie.index[nulos])
                serie = serie.fillna(reemplazos)
                
            return serie

        df['host_acceptance_rate'] = df.groupby('host_is_superhost')['host_acceptance_rate'].transform(imputar_mediana_con_ruido)

        df['host_response_rate'] = df.groupby(
            ['host_is_superhost', 'host_response_time'], 
            group_keys=False
        )['host_response_rate'].apply(imputar_mediana_con_ruido)



         ############################### ELIMINACION DE VARIABLES ################################
        cols_to_drop = [
            'scrape_id', 'host_thumbnail_url', 'host_picture_url', 'price', 
            'minimum_minimum_nights', 'maximum_minimum_nights', 
            'minimum_maximum_nights', 'maximum_maximum_nights',
            'host_listings_count', 'host_total_listings_count',
            'host_about', 'host_url', 'host_neighbourhood',
            'neighborhood_overview', 'neighbourhood', 'neighbourhood_group_cleansed','calendar_updated'
        ]

        # Eliminamos las columnas. errors='ignore' evita que falle si una ya no existe
        df_cleaned = df.drop(columns=cols_to_drop, errors='ignore')

        ################################## RENOMBRACIONES #####################################
        renames = {
            'calculated_host_listings_count': 'host_listings_count',
            'calculated_host_listings_count_entire_homes': 'host_listings_count_eh',
            'calculated_host_listings_count_private_rooms': 'host_listings_count_pr',
            'calculated_host_listings_count_shared_rooms': 'host_listings_count_sr',
            'calculated_host_listings_count_hotel_rooms': 'host_listings_count_hr'
        }

        # Aplicamos los nuevos nombres
        df_cleaned.rename(columns=renames, inplace=True)

        print(f"Limpieza y renombrado OK. Columnas actuales: {len(df_cleaned.columns)}")


        # --- REPORTE DE LIMPIEZA ---
        print(f"Columnas eliminadas: {len(cols_to_drop)}")
        print(f"Columnas restantes: {df_cleaned.shape[1]}")

        # 1. Definimos una ruta para el archivo transformado
        path_transformado = "/tmp/listings_transformed.parquet"
        
        # 2. Guardamos el archivo físicamente en el disco
        df_cleaned.to_parquet(path_transformado, index=False)
        
        # 3. DEVOLVEMOS LA RUTA (un string), no los bytes
        return path_transformado
    
    @task()
    def enrichment(path_archivo):


        df = pd.read_parquet(path_archivo)
        df['host_since'] = pd.to_datetime(df['host_since'])

        # 2. ANTIGÜEDAD 
        df['host_tenure_days'] = (SCRAPE_DATE - df['host_since']).dt.days

        # 3. IMPUTACIÓN DE REVIEWS
        if 'reviews_per_month' in df.columns:
            no_reviews = df['number_of_reviews'] == 0
            df.loc[no_reviews, 'reviews_per_month'] = 0
            print(f'  reviews_per_month imputados a 0: {no_reviews.sum()}')

        # 4. SEGMENTACIÓN DEL ANFITRIÓN
        def segment_host(count):
            if count == 1: return 'Individual'
            if count <= 5: return 'Small_Investor'
            return 'Agency'

        df['host_segment'] = df['host_listings_count'].apply(segment_host)

        # 5. AMENITIES ESTRATÉGICAS
        # Usamos .fillna('') para evitar errores de tipo si amenities es NaN
        amenities_series = df['amenities'].fillna('').str
        df['has_ac'] = amenities_series.contains('Air conditioning|Central air conditioning', case=False).astype(int)
        df['has_workspace'] = amenities_series.contains('Dedicated workspace', case=False).astype(int)
        df['has_parking'] = amenities_series.contains('Free parking|Paid parking', case=False).astype(int)

        # Conteo de amenities
        df['amenities_count'] = df['amenities'].apply(lambda x: len(str(x).split(',')) if pd.notnull(x) else 0)

        # 6. RATIO DE HABITABILIDAD. 1= 1 cama por persona, 0.5 lo habitual y menor a 0.5 mas de 2 personas por cama
        df['ratio_beds_accommodates'] = np.where(df['accommodates'] > 0, 
                                                df['beds'] / df['accommodates'], 
                                                0)

        # 7. DISTANCIA AL CENTRO (Málaga)
        def haversine_distance(lat1, lon1, lat2, lon2):
            R = 6371.0
            lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
            c = 2 * np.arcsin(np.sqrt(a))
            return R * c

        LAT_CENTRO, LON_CENTRO = 36.7213, -4.4214
        df['dist_center_km'] = haversine_distance(df['latitude'], df['longitude'], LAT_CENTRO, LON_CENTRO)

        # 8. TRUST SCORE
        # Llenamos nulos en rating con 0 para evitar que el score sea NaN
        df['trust_score'] = df['review_scores_rating'].fillna(0) * np.log1p(df['number_of_reviews'])

        print(f"Registros procesados: {len(df)}")
        
        path_enriquecido = "/tmp/listings_enriched.parquet"
        
        df.to_parquet(path_enriquecido, index=False)
        
        return path_enriquecido


    @task()
    def eda_report(path_archivo: str) -> str:
        """EDA: Genera dashboard HTML interactivo con análisis exploratorio."""
        import json, math
        from collections import Counter

        df = pd.read_parquet(path_archivo)
        os.makedirs(EDA_OUTPUT_DIR, exist_ok=True)

        # ── helpers ──────────────────────────────────────────────────────────────
        def to_json(obj):
            if isinstance(obj, (np.integer,)):  return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.ndarray,)):  return obj.tolist()
            if isinstance(obj, pd.Timestamp):   return str(obj)
            return obj

        def safe(val):
            if val is None or (isinstance(val, float) and math.isnan(val)):
                return None
            return to_json(val)

        # ── 1. Stats generales ───────────────────────────────────────────────────
        total_rows   = len(df)
        total_cols   = len(df.columns)
        num_cols     = df.select_dtypes(include='number').columns.tolist()
        cat_cols     = df.select_dtypes(include='object').columns.tolist()
        bool_cols    = df.select_dtypes(include='bool').columns.tolist()
        null_pct     = (df.isnull().mean() * 100).round(2).to_dict()

        # ── 2. Precio ────────────────────────────────────────────────────────────
        price_series = df['price_num'].dropna()
        price_hist   = np.histogram(price_series.clip(upper=500), bins=40)
        price_data   = {
            "bins":   [round(float(x), 1) for x in price_hist[1][:-1]],
            "counts": price_hist[0].tolist(),
            "mean":   round(float(price_series.mean()), 2),
            "median": round(float(price_series.median()), 2),
            "p95":    round(float(price_series.quantile(0.95)), 2),
        }

        # ── 3. Room type ─────────────────────────────────────────────────────────
        room_counts = df['room_type'].value_counts().to_dict() if 'room_type' in df.columns else {}

        # ── 4. Host segment ──────────────────────────────────────────────────────
        seg_counts = df['host_segment'].value_counts().to_dict() if 'host_segment' in df.columns else {}

        # ── 5. Scores rating ─────────────────────────────────────────────────────
        rating_series = df['review_scores_rating'].dropna() if 'review_scores_rating' in df.columns else pd.Series([], dtype=float)
        rating_hist   = np.histogram(rating_series, bins=20)
        rating_data   = {
            "bins":   [round(float(x), 2) for x in rating_hist[1][:-1]],
            "counts": rating_hist[0].tolist(),
            "mean":   round(float(rating_series.mean()), 3) if len(rating_series) else None,
        }

        # ── 6. Disponibilidad 365 ────────────────────────────────────────────────
        avail_series = df['availability_365'].dropna() if 'availability_365' in df.columns else pd.Series([], dtype=float)
        avail_hist   = np.histogram(avail_series, bins=24)
        avail_data   = {
            "bins":   [int(x) for x in avail_hist[1][:-1]],
            "counts": avail_hist[0].tolist(),
        }

        # ── 7. Distancia al centro ───────────────────────────────────────────────
        dist_series = df['dist_center_km'].dropna() if 'dist_center_km' in df.columns else pd.Series([], dtype=float)
        dist_hist   = np.histogram(dist_series.clip(upper=20), bins=30)
        dist_data   = {
            "bins":   [round(float(x), 2) for x in dist_hist[1][:-1]],
            "counts": dist_hist[0].tolist(),
        }

        # ── 8. Top barrios por precio ─────────────────────────────────────────────
        neigh_price = {}
        if 'neighbourhood_cleansed' in df.columns and 'price_num' in df.columns:
            neigh_price = (
                df.groupby('neighbourhood_cleansed')['price_num']
                .agg(['median', 'count'])
                .query('count >= 10')
                .sort_values('median', ascending=False)
                .head(15)
                .rename(columns={'median': 'median_price', 'count': 'n'})
                .reset_index()
                .to_dict(orient='list')
            )

        # ── 9. Nulos por columna (top 20) ────────────────────────────────────────
        null_sorted = sorted(null_pct.items(), key=lambda x: -x[1])[:20]
        null_data   = {"cols": [k for k, _ in null_sorted], "pct": [v for _, v in null_sorted]}

        # ── 10. Correlaciones numéricas ──────────────────────────────────────────
        corr_cols   = ['price_num', 'review_scores_rating', 'number_of_reviews',
                    'accommodates', 'dist_center_km', 'amenities_count',
                    'host_tenure_days', 'trust_score']
        corr_cols   = [c for c in corr_cols if c in df.columns]
        corr_matrix = df[corr_cols].corr().round(3)
        corr_data   = {
            "labels": corr_cols,
            "matrix": corr_matrix.values.tolist(),
        }

        # ── 11. Superhost vs precio ───────────────────────────────────────────────
        superhost_data = {}
        if 'host_is_superhost' in df.columns:
            for val, label in [(True, 'Superhost'), (False, 'Regular')]:
                sub = df[df['host_is_superhost'] == val]['price_num'].dropna()
                superhost_data[label] = {
                    "median": round(float(sub.median()), 2) if len(sub) else None,
                    "mean":   round(float(sub.mean()), 2)   if len(sub) else None,
                    "n":      int(len(sub)),
                }

        # ── 12. Amenities populares ───────────────────────────────────────────────
        amenity_counts = Counter()
        if 'amenities' in df.columns:
            for row in df['amenities'].dropna():
                for a in str(row).split(','):
                    a = a.strip().strip('[]"\'')
                    if a:
                        amenity_counts[a] += 1
        top_amenities = amenity_counts.most_common(20)
        amenity_data  = {
            "names":  [a for a, _ in top_amenities],
            "counts": [int(c) for _, c in top_amenities],
        }

        # ── Build JSON payload ───────────────────────────────────────────────────
        payload = json.dumps({
            "meta":          {"rows": total_rows, "cols": total_cols, "num": len(num_cols), "cat": len(cat_cols)},
            "price":         price_data,
            "room_type":     {str(k): int(v) for k, v in room_counts.items()},
            "host_segment":  {str(k): int(v) for k, v in seg_counts.items()},
            "rating":        rating_data,
            "availability":  avail_data,
            "distance":      dist_data,
            "neigh_price":   neigh_price,
            "null_data":     null_data,
            "corr":          corr_data,
            "superhost":     superhost_data,
            "amenities":     amenity_data,
        }, default=str)

        # ── HTML ──────────────────────────────────────────────────────────────────
        html = f"""<!DOCTYPE html>
    <html lang="es">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Airbnb Málaga — EDA Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
    :root {{
        --bg: #0f1117; --card: #1a1d27; --border: #2a2d3a;
        --text: #e8eaf0; --muted: #8b8fa8; --accent: #ff5a5f;
        --blue: #4e9af1; --green: #43c78a; --amber: #f5a623;
        --purple: #9b7ee8; --teal: #2eccc4;
        --font: 'Segoe UI', system-ui, sans-serif;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ background: var(--bg); color: var(--text); font-family: var(--font); padding: 0; }}
    header {{
        background: linear-gradient(135deg, #1a1d27 0%, #12151f 100%);
        border-bottom: 1px solid var(--border);
        padding: 28px 40px 22px;
        display: flex; align-items: flex-end; gap: 24px; flex-wrap: wrap;
    }}
    header h1 {{ font-size: 1.7rem; font-weight: 700; letter-spacing: -.5px; }}
    header h1 span {{ color: var(--accent); }}
    .badge {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: 20px; padding: 4px 14px; font-size: .78rem;
        color: var(--muted); white-space: nowrap;
    }}
    .badge b {{ color: var(--text); }}
    main {{ padding: 32px 40px; max-width: 1400px; margin: 0 auto; }}
    .section-title {{ font-size: .7rem; font-weight: 600; letter-spacing: 1.5px;
        text-transform: uppercase; color: var(--muted); margin: 32px 0 14px; }}
    .grid {{ display: grid; gap: 16px; }}
    .grid-4 {{ grid-template-columns: repeat(4, 1fr); }}
    .grid-3 {{ grid-template-columns: repeat(3, 1fr); }}
    .grid-2 {{ grid-template-columns: repeat(2, 1fr); }}
    .grid-1 {{ grid-template-columns: 1fr; }}
    @media(max-width: 1100px) {{ .grid-4 {{ grid-template-columns: repeat(2,1fr); }} }}
    @media(max-width: 700px)  {{ .grid-4,.grid-3,.grid-2 {{ grid-template-columns: 1fr; }} }}
    .card {{
        background: var(--card); border: 1px solid var(--border);
        border-radius: 14px; padding: 20px 22px; position: relative; overflow: hidden;
    }}
    .card::before {{
        content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
        background: var(--card-accent, transparent); border-radius: 14px 14px 0 0;
    }}
    .card.accent-red   {{ --card-accent: var(--accent); }}
    .card.accent-blue  {{ --card-accent: var(--blue);   }}
    .card.accent-green {{ --card-accent: var(--green);  }}
    .card.accent-amber {{ --card-accent: var(--amber);  }}
    .card.accent-purple{{ --card-accent: var(--purple); }}
    .card.accent-teal  {{ --card-accent: var(--teal);   }}
    .kpi-label {{ font-size: .72rem; color: var(--muted); text-transform: uppercase;
        letter-spacing: .8px; margin-bottom: 6px; }}
    .kpi-value {{ font-size: 2.1rem; font-weight: 700; line-height: 1; }}
    .kpi-sub   {{ font-size: .8rem; color: var(--muted); margin-top: 5px; }}
    .chart-title {{ font-size: .82rem; font-weight: 600; color: var(--muted);
        margin-bottom: 16px; letter-spacing: .3px; }}
    canvas {{ display: block; }}
    .corr-grid {{
        display: grid; gap: 2px;
        grid-template-columns: repeat(var(--cols), 1fr);
        font-size: .6rem; text-align: center;
    }}
    .corr-cell {{
        aspect-ratio: 1; display: flex; align-items: center; justify-content: center;
        border-radius: 3px; font-size: .6rem; font-weight: 500;
    }}
    .corr-label {{ padding: 4px 2px; color: var(--muted); font-size: .62rem;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .corr-header {{ color: var(--muted); font-size: .6rem; padding: 2px;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-align: center; }}
    .null-bar-wrap {{ display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }}
    .null-col-name  {{ font-size: .73rem; color: var(--text); width: 190px;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex-shrink: 0; }}
    .null-bar-bg  {{ flex: 1; height: 8px; background: var(--border); border-radius: 4px; overflow: hidden; }}
    .null-bar-fill{{ height: 100%; border-radius: 4px; background: var(--accent); }}
    .null-pct  {{ font-size: .72rem; color: var(--muted); width: 38px; text-align: right; }}
    .superhost-row {{ display: flex; gap: 16px; flex-wrap: wrap; }}
    .superhost-card {{
        flex: 1; min-width: 160px; background: var(--bg); border: 1px solid var(--border);
        border-radius: 10px; padding: 16px; text-align: center;
    }}
    .sh-label {{ font-size: .73rem; color: var(--muted); margin-bottom: 6px; }}
    .sh-price {{ font-size: 1.5rem; font-weight: 700; }}
    .sh-n {{ font-size: .72rem; color: var(--muted); margin-top: 4px; }}
    footer {{
        text-align: center; padding: 28px; font-size: .75rem; color: var(--muted);
        border-top: 1px solid var(--border); margin-top: 40px;
    }}
    </style>
    </head>
    <body>
    <header>
    <div>
        <h1>Airbnb <span>Málaga</span> — EDA Report</h1>
        <div style="margin-top:8px;font-size:.82rem;color:var(--muted)">
        Datos scrapeados al 30 sep 2025 · Generado por Apache Airflow
        </div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-left:auto" id="badges"></div>
    </header>
    <main>
    <div class="section-title">Resumen del dataset</div>
    <div class="grid grid-4" id="kpis"></div>

    <div class="section-title">Distribución de precios</div>
    <div class="grid grid-2">
        <div class="card accent-red">
        <div class="chart-title">Precio por noche (€) — clip &lt;500 | histograma</div>
        <canvas id="chartPrice" height="160"></canvas>
        </div>
        <div class="card accent-amber">
        <div class="chart-title">Precio mediano por barrio (top 15, mín 10 listings)</div>
        <canvas id="chartNeigh" height="160"></canvas>
        </div>
    </div>

    <div class="section-title">Tipo de alojamiento y anfitriones</div>
    <div class="grid grid-3">
        <div class="card accent-blue">
        <div class="chart-title">Tipo de habitación</div>
        <canvas id="chartRoom" height="180"></canvas>
        </div>
        <div class="card accent-purple">
        <div class="chart-title">Segmento de anfitrión</div>
        <canvas id="chartSeg" height="180"></canvas>
        </div>
        <div class="card accent-green">
        <div class="chart-title">Superhost vs Regular — precio mediano (€)</div>
        <div class="superhost-row" id="superhostRow"></div>
        </div>
    </div>

    <div class="section-title">Reviews y disponibilidad</div>
    <div class="grid grid-2">
        <div class="card accent-teal">
        <div class="chart-title">Distribución puntuación media (review_scores_rating)</div>
        <canvas id="chartRating" height="160"></canvas>
        </div>
        <div class="card accent-blue">
        <div class="chart-title">Disponibilidad anual (días / año)</div>
        <canvas id="chartAvail" height="160"></canvas>
        </div>
    </div>

    <div class="section-title">Geografía y comodidades</div>
    <div class="grid grid-2">
        <div class="card accent-amber">
        <div class="chart-title">Distancia al centro de Málaga (km)</div>
        <canvas id="chartDist" height="160"></canvas>
        </div>
        <div class="card accent-purple">
        <div class="chart-title">Top 20 amenities más frecuentes</div>
        <canvas id="chartAmen" height="160"></canvas>
        </div>
    </div>

    <div class="section-title">Calidad de datos — nulos (%)</div>
    <div class="grid grid-1">
        <div class="card">
        <div class="chart-title">Top 20 columnas con más valores nulos</div>
        <div id="nullBars"></div>
        </div>
    </div>

    <div class="section-title">Correlaciones entre variables numéricas</div>
    <div class="grid grid-1">
        <div class="card accent-teal">
        <div class="chart-title">Matriz de correlación de Pearson</div>
        <div id="corrMatrix" style="overflow-x:auto"></div>
        </div>
    </div>
    </main>
    <footer>Pipeline: extract → transform → enrichment → <strong>eda_report</strong> → load</footer>

    <script>
    const RAW = {payload};
    const D = RAW;

    // ── Palette ────────────────────────────────────────────────────────────────
    const C = {{
    red:    '#ff5a5f', blue:   '#4e9af1', green:  '#43c78a',
    amber:  '#f5a623', purple: '#9b7ee8', teal:   '#2eccc4',
    muted:  '#8b8fa8', grid:   '#2a2d3a', text:   '#e8eaf0',
    }};

    Chart.defaults.color = C.muted;
    Chart.defaults.borderColor = C.grid;
    Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

    function rgba(hex, a) {{
    const r = parseInt(hex.slice(1,3),16), g = parseInt(hex.slice(3,5),16), b = parseInt(hex.slice(5,7),16);
    return `rgba(${{r}},${{g}},${{b}},${{a}})`;
    }}

    // ── KPIs ───────────────────────────────────────────────────────────────────
    const kpiData = [
    {{ label:'Listings',      value: D.meta.rows.toLocaleString('es'), sub:'registros totales', accent:'red'   }},
    {{ label:'Columnas',      value: D.meta.cols,                      sub:`${{D.meta.num}} numéricas · ${{D.meta.cat}} categóricas`, accent:'blue'   }},
    {{ label:'Precio medio',  value:`€${{D.price.mean}}`,              sub:`mediana €${{D.price.median}} · p95 €${{D.price.p95}}`, accent:'amber'  }},
    {{ label:'Rating medio',  value: D.rating.mean ?? '—',             sub:'review_scores_rating', accent:'green'  }},
    ];
    const badges = document.getElementById('badges');
    kpiData.forEach(k => {{
    badges.insertAdjacentHTML('beforeend',
        `<span class="badge"><b>${{k.value}}</b> ${{k.label}}</span>`);
    }});
    const kpis = document.getElementById('kpis');
    kpiData.forEach(k => {{
    kpis.insertAdjacentHTML('beforeend', `
        <div class="card accent-${{k.accent}}">
        <div class="kpi-label">${{k.label}}</div>
        <div class="kpi-value">${{k.value}}</div>
        <div class="kpi-sub">${{k.sub}}</div>
        </div>`);
    }});

    // ── Helpers chart ──────────────────────────────────────────────────────────
    function barChart(id, labels, data, color, opts={{}} ) {{
    return new Chart(document.getElementById(id), {{
        type:'bar',
        data:{{ labels, datasets:[{{data, backgroundColor:rgba(color,.7), borderColor:color,
        borderWidth:1, borderRadius:3, borderSkipped:false }}] }},
        options:{{ plugins:{{legend:{{display:false}}}}, scales:{{
        x:{{grid:{{color:C.grid}}, ticks:{{maxRotation:45, font:{{size:9}}}}}},
        y:{{grid:{{color:C.grid}}, ticks:{{font:{{size:9}}}}}}
        }}, ...opts }}
    }});
    }}
    function hbarChart(id, labels, data, color) {{
    return new Chart(document.getElementById(id), {{
        type:'bar',
        data:{{ labels, datasets:[{{data, backgroundColor:rgba(color,.7), borderColor:color,
        borderWidth:1, borderRadius:3, borderSkipped:false }}] }},
        options:{{
        indexAxis:'y',
        plugins:{{legend:{{display:false}}}},
        scales:{{
            x:{{grid:{{color:C.grid}}, ticks:{{font:{{size:9}}}}}},
            y:{{grid:{{color:'transparent'}}, ticks:{{font:{{size:9}}}}}}
        }}
        }}
    }});
    }}
    function doughnut(id, labels, data, colors) {{
    return new Chart(document.getElementById(id), {{
        type:'doughnut',
        data:{{ labels, datasets:[{{data, backgroundColor:colors, borderColor:'#1a1d27', borderWidth:2}}] }},
        options:{{ plugins:{{legend:{{position:'right', labels:{{font:{{size:10}}, padding:8}}}}}},
        cutout:'60%' }}
    }});
    }}

    // ── Charts ─────────────────────────────────────────────────────────────────
    barChart('chartPrice',
    D.price.bins.map(v=>v.toFixed(0)),
    D.price.counts, C.red);

    if (D.neigh_price && D.neigh_price.neighbourhood_cleansed) {{
    hbarChart('chartNeigh',
        D.neigh_price.neighbourhood_cleansed,
        D.neigh_price.median_price, C.amber);
    }} else {{
    document.getElementById('chartNeigh').parentElement.innerHTML =
        '<div class="chart-title">Sin datos de barrio disponibles</div>';
    }}

    const roomKeys = Object.keys(D.room_type);
    doughnut('chartRoom', roomKeys, roomKeys.map(k=>D.room_type[k]),
    [C.blue, C.teal, C.purple, C.green]);

    const segKeys = Object.keys(D.host_segment);
    doughnut('chartSeg', segKeys, segKeys.map(k=>D.host_segment[k]),
    [C.purple, C.amber, C.red]);

    barChart('chartRating',
    D.rating.bins.map(v=>v.toFixed(2)),
    D.rating.counts, C.teal);

    barChart('chartAvail',
    D.availability.bins.map(v=>v.toString()),
    D.availability.counts, C.blue);

    barChart('chartDist',
    D.distance.bins.map(v=>v.toFixed(1)),
    D.distance.counts, C.amber);

    hbarChart('chartAmen',
    D.amenities.names, D.amenities.counts, C.purple);

    // ── Superhost ──────────────────────────────────────────────────────────────
    const shRow = document.getElementById('superhostRow');
    [['Superhost', C.green], ['Regular', C.muted]].forEach(([key, col]) => {{
    const d = D.superhost[key] ?? {{}};
    shRow.insertAdjacentHTML('beforeend', `
        <div class="superhost-card">
        <div class="sh-label">${{key}}</div>
        <div class="sh-price" style="color:${{col}}">€${{d.median ?? '—'}}</div>
        <div class="sh-n">n=${{(d.n??0).toLocaleString('es')}} · media €${{d.mean??'—'}}</div>
        </div>`);
    }});

    // ── Null bars ──────────────────────────────────────────────────────────────
    const nullWrap = document.getElementById('nullBars');
    D.null_data.cols.forEach((col, i) => {{
    const pct = D.null_data.pct[i];
    const color = pct > 50 ? C.red : pct > 20 ? C.amber : C.teal;
    nullWrap.insertAdjacentHTML('beforeend', `
        <div class="null-bar-wrap">
        <div class="null-col-name">${{col}}</div>
        <div class="null-bar-bg"><div class="null-bar-fill" style="width:${{pct}}%;background:${{color}}"></div></div>
        <div class="null-pct">${{pct}}%</div>
        </div>`);
    }});

    // ── Correlation heatmap ────────────────────────────────────────────────────
    (function() {{
    const {{ labels, matrix }} = D.corr;
    const n = labels.length;
    const wrap = document.getElementById('corrMatrix');
    wrap.style.setProperty('--cols', n + 1);

    let html = `<div class="corr-grid" style="--cols:${{n+1}}">`;
    html += `<div></div>`;
    labels.forEach(l => html += `<div class="corr-header">${{l.replace(/_/g,' ')}}</div>`);
    labels.forEach((row, i) => {{
        html += `<div class="corr-label">${{row.replace(/_/g,' ')}}</div>`;
        matrix[i].forEach((val, j) => {{
        const v  = val === null ? 0 : val;
        const abs = Math.abs(v);
        let bg, fg;
        if (i === j) {{ bg = '#2a2d3a'; fg = C.muted; }}
        else if (v > 0) {{
            const t = abs;
            bg = `rgba(78,154,241,${{0.1 + t*0.75}})`;
            fg = t > 0.5 ? '#fff' : C.muted;
        }} else {{
            const t = abs;
            bg = `rgba(255,90,95,${{0.1 + t*0.75}})`;
            fg = t > 0.5 ? '#fff' : C.muted;
        }}
        const disp = val === null ? '' : v.toFixed(2);
        html += `<div class="corr-cell" style="background:${{bg}};color:${{fg}}">${{disp}}</div>`;
        }});
    }});
    html += '</div>';
    wrap.innerHTML = html;
    }})();
    </script>
    </body>
    </html>"""

        out_path = os.path.join(EDA_OUTPUT_DIR, "eda_dashboard.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        print(f"EDA dashboard guardado en: {out_path}")
        return path_archivo  # pasa los datos sin modificar a la siguiente tarea

    @task()
    def load(path_final): # Recibe la ruta que viene de enrichment o eda
        """3. LOAD: Guardado del CSV final enriquecido."""
        
        df = pd.read_parquet(path_final)
        
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"PROCESO FINALIZADO. Archivo guardado en: {OUTPUT_FILE}")
        print(f"Registros finales: {len(df)}")

    # --- FLUJO RECOMENDADO ---
    raw_path       = extract()      
    transformed_path = transform(raw_path) 
    enriched_path  = enrichment(transformed_path) 


    eda_report(enriched_path) 

    load(enriched_path)
dag_instance = airbnb_listings_pipeline()