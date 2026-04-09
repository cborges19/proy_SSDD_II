import pandas as pd
import numpy as np
import io
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import re
import pyarrow
import json
import math
from collections import Counter
from jinja2 import Template  
BASE_DIR = "/home/vboxuser/Documentos/proy_SSDD_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "listings.csv")
OUTPUT_FILE = "/home/vboxuser/Documentos/resultados_listings_final.csv"
EDA_OUTPUT_DIR = "/home/vboxuser/Documentos/outputs/"
SCRAPE_DATE = pd.Timestamp('2025-09-30')


# ---------- DAG DEFINITION ----------
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
        """
         DATA EXTRACTION: Ingests raw data from the source file.
        - Loads the raw Airbnb listings from a  CSV source.
        - Validates basic file existence and integrity.
        - Persists a raw copy in the temporary directory for lineage tracking.
        """
        df = pd.read_csv(PATH_LOCAL_CSV, encoding='utf-8')
        output_path = "/tmp/listings_processed.parquet"
        df.to_parquet(output_path, engine='pyarrow', index=False)
        
        print(f"Archivo guardado en {output_path}")
        return output_path 
    @task()
    def transform(path_archivo):
        """
        DATA TRANSFORMATION: Standardizes formats and handles missing values.
        """
        df = pd.read_parquet(path_archivo)
        print("Iniciando transformaciones...")
        # ---------- PRICE CLEANING ----------
        # Regex-based stripping: Remove currency symbols and commas ($ and ,)
        # Parsing: Cast the cleaned strings to float for numeric analysis
        df['price_num'] = (
            df['price']
            .str.replace(r'[\$,]', '', regex=True)
            .astype(float)
        )

        # Zero-value handling: Convert prices equal to 0 to NaN
        # Airbnb price '0' is usually a listing error or an uninitialized value.
        n_zero = (df['price_num'] == 0).sum()
        df.loc[df['price_num'] == 0, 'price_num'] = np.nan
        
        print(f'Zero-priced listings invalidated (set to NaN): {n_zero}')
        print('Price cleaning: OK')

       # ---------- BOOLEAN PARSING & MAPPING ----------
        # Convert string indicators ('t'/'f') to Python Booleans
        TARGET_BOOL_COLS = [
            'host_is_superhost',
            'host_has_profile_pic',
            'host_identity_verified',
            'instant_bookable',
            'has_availability'
        ]
        
        # Verify columns exist before processing
        bool_cols_present = [c for c in TARGET_BOOL_COLS if c in df.columns]

        for col in bool_cols_present:
            pre_mapping_nulls = df[col].isnull().sum()
            
            # Explicit mapping: 't' -> True, 'f' -> False. 
            df[col] = df[col].map({'t': True, 'f': False})
            
            post_mapping_nulls = df[col].isnull().sum()
            
            # Validation check: Identify if unexpected values were introduced
            if post_mapping_nulls > pre_mapping_nulls:
                unexpected_count = post_mapping_nulls - pre_mapping_nulls
                print(f'  {col}: {unexpected_count} unexpected values coerced to NaN')
            else:
                print(f'  {col}: Parsed successfully')

        print('Boolean parsing: OK')

        # ---------- RATIOS TRANSFORMATION ----------
        # Scale percentage values to ratios [0, 1]
        RATE_COLS = ['host_response_rate', 'host_acceptance_rate']

        for col in RATE_COLS:
            if col not in df.columns:
                print(f'  {col} no encontrada, se omite')
                continue
            df[col] = (
                pd.to_numeric(
                    df[col].astype(str).str.replace('%', '', regex=False), 
                    errors='coerce'
                ) / 100
            )
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
        
        # ---------- DATE PARSING & TEMPORAL VALIDATION ----------
        # Convert date-format strings to datetime objects for time-series analysis.
        DATE_COLS = ['host_since', 'first_review', 'last_review']

        for col in DATE_COLS:
            if col not in df.columns:
                print(f'  Warning: {col} not found, skipping.')
                continue
            
            # Parse strings to datetime; invalid formats are coerced to NaT (Not a Time)
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Logic Check: Identify "future" dates that exceed the scraping timestamp.
            invalid_future_count = (df[col] > SCRAPE_DATE).sum()
            
            if invalid_future_count:
                print(f'  {col}: {invalid_future_count} future dates detected → Invalidated to NaT')
                # Set future dates to NaT to maintain data integrity
                df.loc[df[col] > SCRAPE_DATE, col] = pd.NaT
            else:
                print(f'  {col}: Date parsing successful')

        print('Temporal parsing: OK')

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

        
        
        # ---------- BATHROOMS_TEXT to BATHROOMS ----------
        # Parse bathroom_text into numeric format
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

        # ---------- HOST_IS_SUPERHOST ----------
        # Intra-host consistency: Fill nulls using the most frequent value (mode) for the same host_id
        df['host_is_superhost'] = df.groupby('host_id')['host_is_superhost'].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else pd.NA)
        )

        # Heuristic-based imputation: Define Superhost criteria based on Airbnb official standards
        superhost_criteria = (
            (df['review_scores_rating'] >= 4.8) & 
            (df['host_response_rate'] >= 0.9) & 
            (df['host_acceptance_rate'] >= 0.9) &
            (df['number_of_reviews'] >= 5)
        )

        # Apply True only to remaining nulls that meet all performance thresholds
        df.loc[df['host_is_superhost'].isna() & superhost_criteria, 'host_is_superhost'] = True

        # Default any remaining nulls to False
        df['host_is_superhost'] = df['host_is_superhost'].fillna(False).astype(bool)


       


        # ---------- HOST_RESPONSE_TIME IMPUTATION ----------     
        # Fill missing values using the most frequent response time (mode) 
        # segmented by Superhost status. This accounts for the higher 
        df['host_response_time'] = df.groupby('host_is_superhost')['host_response_time'].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "unknown")
        )



        # ---------- PERFORMANCE RATES IMPUTATION (STOCHASTIC) ----------
        
        def impute_median_with_noise(series):
            """
            Fills nulls using the group's median plus a small amount of random noise.
            This prevents 'spikes' in the distribution and maintains natural variance.
            """
            if series.isnull().all():
                return series
            
            median = series.median()
            # Calculate standard deviation for noise scaling
            std_dev = series.std()
            
            # Default to 1% noise if the group has no variance
            if pd.isna(std_dev) or std_dev == 0:
                std_dev = 0.01
            
            null_mask = series.isna()
            num_nulls = null_mask.sum()
            
            if num_nulls > 0:
                # Generate Gaussian noise (centered at 0, 10% of the group's std dev)
                noise = np.random.normal(loc=0, scale=std_dev * 0.1, size=num_nulls)
                
                # Apply noise to median and clip to ensure valid percentage range [0, 1]
                imputed_values = np.clip(median + noise, 0, 1)
                
                # Align indices and fill missing values
                replacements = pd.Series(imputed_values, index=series.index[null_mask])
                series = series.fillna(replacements)
                
            return series

        # Impute Acceptance Rate segmented by Superhost status
        df['host_acceptance_rate'] = df.groupby('host_is_superhost')['host_acceptance_rate'].transform(impute_median_with_noise)

        # Impute Response Rate segmented by both Superhost status and Response Time
        df['host_response_rate'] = df.groupby(
            ['host_is_superhost', 'host_response_time'], 
            group_keys=False
        )['host_response_rate'].apply(impute_median_with_noise)



        # ---------- FEATURE SELECTION & COLUMN REMOVAL ----------
        # Define redundant, high-cardinality, or administrative columns to be dropped.
        # This includes URLs, redundant night constraints, and metadata used only for scraping.
        cols_to_drop = [
            'scrape_id', 'host_thumbnail_url', 'host_picture_url', 'price', 
            'minimum_minimum_nights', 'maximum_minimum_nights', 
            'minimum_maximum_nights', 'maximum_maximum_nights',
            'host_listings_count', 'host_total_listings_count',
            'host_about', 'host_url', 'host_neighbourhood',
            'neighborhood_overview', 'neighbourhood', 'neighbourhood_group_cleansed', 
            'calendar_updated'
        ]

        # errors='ignore' ensures the pipeline doesn't crash if a column was already removed.
        df_cleaned = df.drop(columns=cols_to_drop, errors='ignore')

        # ---------- FEATURE RENAMING ----------
        # Map calculated host listing counts to shorter, more descriptive names
        renames = {
            'calculated_host_listings_count': 'host_listings_count',
            'calculated_host_listings_count_entire_homes': 'host_listings_count_eh',
            'calculated_host_listings_count_private_rooms': 'host_listings_count_pr',
            'calculated_host_listings_count_shared_rooms': 'host_listings_count_sr',
            'calculated_host_listings_count_hotel_rooms': 'host_listings_count_hr'
        }

        # Apply renaming. Using inplace=True to modify the existing DataFrame.
        df_cleaned.rename(columns=renames, inplace=True)

        # ---------- DATA EXPORT & PERSISTENCE ----------
        # Log transformation summary
        print(f"Cleaning and renaming completed. Current column count: {len(df_cleaned.columns)}")
        print(f"Columns dropped: {len(cols_to_drop)}")
        print(f"Columns remaining: {df_cleaned.shape[1]}")

        # Define the final output path for the processed dataset
        path_transformed = "/tmp/listings_transformed.parquet"
        
        # Persist the cleaned DataFrame to disk in Parquet format
        df_cleaned.to_parquet(path_transformed, index=False)
        
        # 3. Return the absolute path as a string for downstream XCom tasks
        return path_transformed
    

    @task()
    def enrichment(path_archivo):
        """
        Feature Engineering: Creates new metrics and domain-specific indicators
        to enhance the analytical value of the dataset.
        """
        df = pd.read_parquet(path_archivo)
        
        # Ensure date format for temporal calculations
        df['host_since'] = pd.to_datetime(df['host_since'])

        # ---------- HOST TENURE ----------
        # Calculate how many days the host has been active since the scraping date
        df['host_tenure_days'] = (SCRAPE_DATE - df['host_since']).dt.days

        # ---------- REVIEW IMPUTATION ----------
        # Logic: Listings with zero total reviews must have zero reviews per month
        if 'reviews_per_month' in df.columns:
            no_reviews_mask = df['number_of_reviews'] == 0
            df.loc[no_reviews_mask, 'reviews_per_month'] = 0
            print(f'  Reviews_per_month imputed to 0 for {no_reviews_mask.sum()} listings')

        # ---------- HOST SEGMENTATION ----------
        # Categorize hosts based on their listing volume to distinguish between
        # individual owners and professional agencies/investors.
        def segment_host(count):
            if count == 1: return 'Individual'
            if count <= 5: return 'Small_Investor'
            return 'Agency'

        df['host_segment'] = df['host_listings_count'].apply(segment_host)

        # ---------- STRATEGIC AMENITIES ----------
        # Flag key features that impact booking decisions using string pattern matching.
        amenities_series = df['amenities'].fillna('').str
        df['has_ac'] = amenities_series.contains('Air conditioning|Central air conditioning', case=False).astype(int)
        df['has_workspace'] = amenities_series.contains('Dedicated workspace', case=False).astype(int)
        df['has_parking'] = amenities_series.contains('Free parking|Paid parking', case=False).astype(int)

        # Complexity metric: Total count of unique amenities offered
        df['amenities_count'] = df['amenities'].apply(lambda x: len(str(x).split(',')) if pd.notnull(x) else 0)

        # ---------- HABITABILITY RATIO ----------
        # Beds-to-Guests ratio: 1.0 means one bed per person. 
        df['ratio_beds_accommodates'] = np.where(
            df['accommodates'] > 0, 
            df['beds'] / df['accommodates'], 
            0
        )

        # ---------- GEOSPATIAL ANALYSIS ----------
        # Calculate Haversine distance to Malaga Center
        def haversine_distance(lat1, lon1, lat2, lon2):
            R = 6371.0 # Earth radius in km
            lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
            c = 2 * np.arcsin(np.sqrt(a))
            return R * c

        MALAGA_CENTER_COORDS = (36.7213, -4.4214)
        df['dist_center_km'] = haversine_distance(
            df['latitude'], df['longitude'], 
            *MALAGA_CENTER_COORDS
        )

        # ---------- TRUST SCORE (WEIGHTED RATING) ----------
        # Combines review quality (rating) with quantity (log-scaled reviews)
        # to penalize high ratings with very few reviews.
        df['trust_score'] = df['review_scores_rating'].fillna(0) * np.log1p(df['number_of_reviews'])

        # ---------- DATA PERSISTENCE ----------
        print(f"Enrichment completed. Records processed: {len(df)}")
        
        path_enriquecido = "/tmp/listings_enriched.parquet"
        df.to_parquet(path_enriquecido, index=False)
        
        return path_enriquecido
    

    @task()
    def eda_report(path_archivo):
        """
        Exploratory Data Analysis (EDA): Generates an interactive HTML dashboard 
        using Jinja2 templating and Chart.js.
        """
        df = pd.read_parquet(path_archivo)
        os.makedirs(EDA_OUTPUT_DIR, exist_ok=True)

        # ---------- SERIALIZATION HELPERS ----------
        def to_json(obj):
            """Custom encoder for NumPy and Pandas types to ensure JSON compatibility."""
            if isinstance(obj, (np.integer,)):  return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.ndarray,)):  return obj.tolist()
            if isinstance(obj, pd.Timestamp):   return str(obj)
            return obj

        # ---------- GENERAL DATASET STATISTICS ----------
        total_rows   = len(df)
        total_cols   = len(df.columns)
        num_cols     = df.select_dtypes(include='number').columns.tolist()
        cat_cols     = df.select_dtypes(include='object').columns.tolist()
        null_pct     = (df.isnull().mean() * 100).round(2).to_dict()

        # ---------- PRICE DISTRIBUTION (CLIPPED AT €500) ----------
        price_series = df['price_num'].dropna()
        # Histogram calculation for visualization
        price_hist   = np.histogram(price_series.clip(upper=500), bins=40)
        price_data   = {
            "bins":   [round(float(x), 1) for x in price_hist[1][:-1]],
            "counts": price_hist[0].tolist(),
            "mean":   round(float(price_series.mean()), 2),
            "median": round(float(price_series.median()), 2),
            "p95":    round(float(price_series.quantile(0.95)), 2),
        }

        # ---------- ROOM TYPE & HOST SEGMENTATION ----------
        room_counts = df['room_type'].value_counts().to_dict() if 'room_type' in df.columns else {}
        seg_counts  = df['host_segment'].value_counts().to_dict() if 'host_segment' in df.columns else {}

        # ---------- REVIEW SCORES & RATINGS ----------
        rating_series = df['review_scores_rating'].dropna() if 'review_scores_rating' in df.columns else pd.Series([], dtype=float)
        rating_hist   = np.histogram(rating_series, bins=20)
        rating_data   = {
            "bins":   [round(float(x), 2) for x in rating_hist[1][:-1]],
            "counts": rating_hist[0].tolist(),
            "mean":   round(float(rating_series.mean()), 3) if len(rating_series) else None,
        }

        # ---------- ANNUAL AVAILABILITY (365 DAYS) ----------
        avail_series = df['availability_365'].dropna() if 'availability_365' in df.columns else pd.Series([], dtype=float)
        avail_hist   = np.histogram(avail_series, bins=24)
        avail_data   = {
            "bins":   [int(x) for x in avail_hist[1][:-1]],
            "counts": avail_hist[0].tolist(),
        }

        # ---------- GEOSPATIAL DISTANCE TO CENTER ----------
        dist_series = df['dist_center_km'].dropna() if 'dist_center_km' in df.columns else pd.Series([], dtype=float)
        dist_hist   = np.histogram(dist_series.clip(upper=20), bins=30)
        dist_data   = {
            "bins":   [round(float(x), 2) for x in dist_hist[1][:-1]],
            "counts": dist_hist[0].tolist(),
        }

        # ---------- NEIGHBOURHOOD PRICE ANALYSIS ----------
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

        # ---------- DATA QUALITY: NULL VALUES (TOP 20) ----------
        null_sorted = sorted(null_pct.items(), key=lambda x: -x[1])[:20]
        null_data   = {"cols": [k for k, _ in null_sorted], "pct": [v for _, v in null_sorted]}

        # ---------- MULTIVARIATE CORRELATION MATRIX ----------
        corr_cols   = ['price_num', 'review_scores_rating', 'number_of_reviews',
                    'accommodates', 'dist_center_km', 'amenities_count',
                    'host_tenure_days', 'trust_score']
        corr_cols   = [c for c in corr_cols if c in df.columns]
        corr_matrix = df[corr_cols].corr().round(3)
        corr_data   = {
            "labels": corr_cols,
            "matrix": corr_matrix.values.tolist(),
        }

        # ---------- SUPERHOST PERFORMANCE COMPARISON ----------
        superhost_data = {}
        if 'host_is_superhost' in df.columns:
            for val, label in [(True, 'Superhost'), (False, 'Regular')]:
                sub = df[df['host_is_superhost'] == val]['price_num'].dropna()
                superhost_data[label] = {
                    "median": round(float(sub.median()), 2) if len(sub) else None,
                    "mean":   round(float(sub.mean()), 2)   if len(sub) else None,
                    "n":      int(len(sub)),
                }

        # ---------- STRATEGIC AMENITIES FREQUENCY ----------
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

        # ---------- BUILD JSON PAYLOAD FOR TEMPLATE ----------
        payload_json = json.dumps({
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
        }, default=to_json)

        # ---------- JINJA2 TEMPLATE RENDERING ----------
        script_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(script_dir, "report_template.html")

        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        # Render the data into the HTML structure
        template = Template(template_content)
        html_final = template.render(payload=payload_json)

        # ---------- SAVE FINAL DASHBOARD ----------
        out_path = os.path.join(EDA_OUTPUT_DIR, "eda_dashboard.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_final)

        print(f"EDA dashboard generated successfully at: {out_path}")
        return path_archivo
    @task()

    # ---------- DATA LOADING & PERSISTENCE ----------
    @task()
    def load(path_final: str):
        """
        3. LOAD: Final persistence stage. Converts the processed Parquet 
        back to CSV for business compatibility and final delivery.
        """
        import pandas as pd
        import os

        # Load the final enriched dataset
        df = pd.read_parquet(path_final)
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Export to CSV for end-user accessibility
        df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"PIPELINE COMPLETED. Final file saved at: {OUTPUT_FILE}")
        print(f"Final record count: {len(df)}")

    # ---------- DAG EXECUTION FLOW ----------
    # 1. Extraction: Get raw data from the source
    raw_data_path = extract()      
    
    # 2. Transformation: Clean, parse, and handle nulls
    cleaned_data_path = transform(raw_data_path) 
    
    # 3. Enrichment: Feature engineering and geospatial analysis
    enriched_data_path = enrichment(cleaned_data_path) 

    # 4. Reporting: Generate interactive EDA Dashboard
    # This task runs in parallel with the final load
    eda_report(enriched_data_path) 

    # 5. Load: Persist the final enriched dataset to the output destination
    load(enriched_data_path)

# Instantiate the DAG
dag_instance = airbnb_listings_pipeline()