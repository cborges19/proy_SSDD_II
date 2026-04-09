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
import ast
import orjson

BASE_DIR = "/home/vboxuser/Documentos/proy_ssdd_II/data/"
PATH_LOCAL_CSV = os.path.join(BASE_DIR, "listings.csv")
OUTPUT_FILE = "/home/vboxuser/Documentos/resultados_listings_final.csv"
EDA_OUTPUT_DIR = "/home/vboxuser/Documentos/output/"
SCRAPE_DATE = pd.Timestamp('2025-09-30')


def impute_median_with_noise(series):
    if series.isnull().all(): return series
    median = series.median()
    std_dev = series.std()
    if pd.isna(std_dev) or std_dev == 0: std_dev = 0.1 # Un poco más de ruido
    
    null_mask = series.isna()
    if null_mask.sum() > 0:
        noise = np.random.normal(loc=0, scale=std_dev * 0.1, size=null_mask.sum())
        imputed_values = (median + noise).round().clip(min=1) 
        series.loc[null_mask] = imputed_values
    return series
def serialize_amenities(x):
                # Ensure we are dealing with a list before serializing
                if isinstance(x, str):
                    # Basic cleaning if it comes as a string representation of a list
                    x = x.strip('[]').replace('"', '').split(',')
                
                # orjson.dumps returns bytes, so we decode to utf-8 string
                return orjson.dumps(x).decode('utf-8')



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

        # ---------- MANUAL DATA CORRECTION (OUTLIER REPAIR) ----------
        # Correcting a specific price outlier for listing ID 47444051.
        # Original value (~21,000€) is identified as a data entry error on website.
        # Real value according to airbnb.com: 182€.
        LISTING_ID_TO_FIX = 47444051
        REAL_PRICE = 182.0

        if 'id' in df.columns and 'price_num' in df.columns:
            # Check if the listing exists in the current shard/dataframe
            mask = df['id'] == LISTING_ID_TO_FIX
            
            if mask.any():
                df.loc[mask, 'price_num'] = REAL_PRICE
                print(f"  Manual fix applied: Listing {LISTING_ID_TO_FIX} price set to {REAL_PRICE}€.")
            else:
                print(f"  Manual fix skipped: Listing {LISTING_ID_TO_FIX} not found in this batch.")

       # ---------- BOOLEAN PARSING & MAPPING ----------
        # Convert string indicators ('t'/'f') to Python Booleans
        TARGET_BOOL_COLS = [
            'host_is_superhost',
            'host_has_profile_pic',
            'host_identity_verified',
            'instant_bookable',
            'has_availability',
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
        # ---------- HOST_RESPONSE_TIME BINARIZATION ----------
        # Create a binary feature: 1 if the host responds within an hour, 0 otherwise.
        if 'host_response_time' in df.columns:
            df['host_response_time_bin'] = (df['host_response_time'] == 'within an hour').astype(int)
            print(f"Binarization complete: {df['host_response_time_bin'].sum()} hosts respond within an hour.")
        # ---------- HOST_RESPONSE_TIME IMPUTATION ----------     
        # Fill missing values using the most frequent response time (mode) 
        # segmented by Superhost status. This accounts for the higher 
        df['host_response_time'] = df.groupby('host_is_superhost')['host_response_time'].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "unknown")
        )

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
                print(f'  {col}: {invalid_future_count} future dates detected - Invalidated to NaT')
                # Set future dates to NaT to maintain data integrity
                df.loc[df[col] > SCRAPE_DATE, col] = pd.NaT
            else:
                print(f'  {col}: Date parsing successful')

        print('Temporal parsing: OK')

       
        
        
        # ---------- BATHROOMS_TEXT to BATHROOMS ----------
        # Parse bathroom_text into numeric format
        df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')

        if 'bathrooms_text' in df.columns:
            def parse_bathrooms_text(text):
                if pd.isna(text): return np.nan
                text = text.lower().strip()
                if 'half' in text or 'medio' in text: 
                    return 0.5
                match = re.search(r'[\d\.]+', text)
                return float(match.group()) if match else np.nan

        print('clean_bathrooms OK')
        print(f"Nulos finales en 'bathrooms': {df['bathrooms'].isnull().sum()}")

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

        # ---------- HOST_VERIFICATIONS ONE-HOT ENCODING (OHE) ----------
        # Parse the string representation of lists into actual Python lists, 
        if 'host_verifications' in df.columns:            
            #  Convert string representation of list to actual list objects
            df['host_verifications'] = df['host_verifications'].apply(
                lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') else []
            )
            
            # Use MultiLabelBinarizer or get_dummies with explode for OHE
            verifications_ohe = df['host_verifications'].str.join('|').str.get_dummies()
            
            # Prefix columns for clarity and join back to the main DataFrame
            verifications_ohe = verifications_ohe.add_prefix('verification_')
            df = pd.concat([df, verifications_ohe], axis=1)
            
            print(f"OHE complete: {verifications_ohe.shape[1]} verification features created.")
       
        # ---------- HAS_AVAILABILITY LOGIC IMPUTATION ----------
        # Impute missing 'has_availability' values based on the 'availability_365' column.
        # Logic: If available days > 0, set to True; otherwise, set to False.
        if 'has_availability' in df.columns and 'availability_365' in df.columns:
            # Identify nulls in the target column
            avail_nulls = df['has_availability'].isna()
            
            # Apply logic: any availability greater than zero counts as True
            df.loc[avail_nulls, 'has_availability'] = df.loc[avail_nulls, 'availability_365'] > 0
            
            # Ensure the column is strictly boolean (handling any leftover edge cases)
            df['has_availability'] = df['has_availability'].fillna(False).astype(bool)
            
            print(f"Inferred availability for {avail_nulls.sum()} missing records.")

        # ---------- ESTIMATED REVENUE IMPUTATION ----------
        # Impute missing 'estimated_revenue' values using the occupancy proxy formula.
        # Formula: price_num * (number_of_reviews * 2)
        if 'estimated_revenue' in df.columns:
            # Identify records where revenue is missing but price and reviews exist
            revenue_nulls = df['estimated_revenue'].isna()
            
            # Apply the estimation formula only to missing values
            # Using price_num (cleaned price) and number_of_reviews as primary drivers
            df.loc[revenue_nulls, 'estimated_revenue'] = (
                df.loc[revenue_nulls, 'price_num'] * (df.loc[revenue_nulls, 'number_of_reviews'] * 2)
            ).fillna(0) # Default to 0 if components are missing
            
            print(f"Revenue estimation complete: {revenue_nulls.sum()} missing values imputed.")

        # ---------- BEDROOMS IMPUTATION ----------
        # Refines the 'bedrooms' column by combining domain knowledge 
        # for single rooms and statistical grouping for entire homes.
        if 'bedrooms' in df.columns and 'room_type' in df.columns:
            # DOMAIN LOGIC: Private/Shared rooms are assigned 1 bedroom
            single_room_mask = (df['bedrooms'].isna()) & \
                               (df['room_type'].str.contains('Private room|Shared room', case=False, na=False))
            df.loc[single_room_mask, 'bedrooms'] = 1
            
            # STATISTICAL GROUPING: Fill remaining nulls (Entire homes/studios)
            # We group by 'accommodates' to ensure the imputed bedroom count 
            # aligns with the property's guest capacity.
            if df['bedrooms'].isna().any():
                df['bedrooms'] = df.groupby('accommodates')['bedrooms'].transform(impute_median_with_noise)

            # Final Cleanup: Ensure no floats and no zeros
            df['bedrooms'] = df['bedrooms'].fillna(1).clip(lower=1).astype(int)
            
            print(f"Bedrooms imputation completed. Final distribution: {df['bedrooms'].value_counts().to_dict()}")

        # ---------- BEDS STATISTICAL IMPUTATION ----------
        # Impute missing values for 'beds' using the grouped median 
        # (by 'accommodates') plus random noise. 
        if 'beds' in df.columns and 'accommodates' in df.columns:
            # We use the previously defined 'impute_median_with_noise' function
            # to fill gaps while maintaining a realistic distribution.
            if df['beds'].isna().any():
                df['beds'] = df.groupby('accommodates')['beds'].transform(impute_median_with_noise)

            # Final Cleanup: Ensure no floats and a minimum of 1 bed per listing
            df['beds'] = df['beds'].fillna(1).clip(lower=1).astype(int)
            
            print(f"Beds imputation complete. Average beds per listing: {df['beds'].mean():.2f}")


        # ---------- PERFORMANCE RATES IMPUTATION (STOCHASTIC) ----------
        # Impute Acceptance Rate segmented by Superhost status
        df['host_acceptance_rate'] = df.groupby('host_is_superhost')['host_acceptance_rate'].transform(impute_median_with_noise)

        # Impute Response Rate segmented by both Superhost status and Response Time
        df['host_response_rate'] = df.groupby(
            ['host_is_superhost', 'host_response_time'], 
            group_keys=False
        )['host_response_rate'].apply(impute_median_with_noise)

        # ---------- AMENITIES SERIALIZATION (ORJSON) ----------
        # Use the high-performance 'orjson' library to serialize the amenities list.
        # This converts Python lists into a minified, web-ready JSON string.
        if 'amenities' in df.columns:
            df['amenities_json'] = df['amenities'].apply(serialize_amenities)
            
            # Verify if the output is string type for CSV compatibility
            if not isinstance(df['amenities_json'].iloc[0], str):
                 df['amenities_json'] = df['amenities_json'].str.decode('utf-8')
            
            print(f"Amenities serialization complete using ORJSON for {len(df)} records.")  


        # Define the final output path for the processed dataset
        path_transformed = "/tmp/listings_transformed.parquet"
        
        # Persist the cleaned DataFrame to disk in Parquet format
        df.to_parquet(path_transformed, index=False)
        
        # Return the absolute path as a string for downstream XCom tasks
        return path_transformed
    
    @task 
    def validation(path_archivo):
        df = pd.read_parquet(path_archivo)
        print("Starting validations...")

        # ---------- REVIEW DATES CHRONOLOGY VALIDATION ----------
        # Ensure 'first_review' is not later than 'last_review'.
        # Invalid chronological order is reset to NaT to maintain temporal integrity.
        if 'first_review' in df.columns and 'last_review' in df.columns:
            invalid_order = (
                df['first_review'].notna() &
                df['last_review'].notna() &
                (df['first_review'] > df['last_review'])
            ).sum()
            
            if invalid_order:
                print(f'  Inconsistency: first_review > last_review in {invalid_order} rows → Resetting to NaT')
                mask = df['first_review'] > df['last_review']
                df.loc[mask, ['first_review', 'last_review']] = pd.NaT
            else:
                print('  Validation OK: first_review / last_review chronology.')

        # ---------- HOST_ID INTEGRITY VALIDATION ----------
        # Validate that host-specific metadata is consistent across all listings 
        # belonging to the same host_id. Identifies data entry discrepancies.
        host_metadata_cols = [
            'host_name', 'host_since', 'host_location', 
            'host_is_superhost', 'host_identity_verified'
        ]
        
        valid_meta_cols = [c for c in host_metadata_cols if c in df.columns]

        for col in valid_meta_cols:
            inconsistency_count = (df.groupby('host_id')[col].nunique() > 1).sum()
            if inconsistency_count > 0:
                print(f"  Warning: Found {inconsistency_count} hosts with inconsistent '{col}' data.")
            else:
                print(f"  Validation OK: '{col}' is consistent across all host_id records.")

        # ---------- GEOSPATIAL BOUNDARY VALIDATION ----------
        # Validate that all listings fall within the official geographic 
        # boundaries of the Málaga province to filter out coordinate outliers.
        LAT_LIMITS = (36.3, 37.3)
        LON_LIMITS = (-5.6, -3.6)

        out_of_bounds = (
            (df['latitude'] < LAT_LIMITS[0]) | (df['latitude'] > LAT_LIMITS[1]) |
            (df['longitude'] < LON_LIMITS[0]) | (df['longitude'] > LON_LIMITS[1])
        )

        invalid_count = out_of_bounds.sum()
        if invalid_count > 0:
            print(f"  Warning: {invalid_count} records found outside Málaga boundaries. Invalidating coordinates.")
            df.loc[out_of_bounds, ['latitude', 'longitude']] = np.nan
        else:
            print("  Geospatial Validation OK: All records are within provincial limits.")

        # ---------- STAY DURATION LOGIC VALIDATION ----------
        # Ensure that 'maximum_nights' is consistently greater than or equal 
        # to 'minimum_nights' and that both values are positive integers.
        if 'maximum_nights' in df.columns and 'minimum_nights' in df.columns:
            df['minimum_nights'] = df['minimum_nights'].apply(lambda x: max(1, int(x)) if pd.notnull(x) else 1)
            df['maximum_nights'] = df['maximum_nights'].apply(lambda x: max(1, int(x)) if pd.notnull(x) else 1)

            invalid_range_mask = df['maximum_nights'] < df['minimum_nights']
            df.loc[invalid_range_mask, 'maximum_nights'] = df.loc[invalid_range_mask, 'minimum_nights']
            
            error_count = invalid_range_mask.sum()
            if error_count > 0:
                print(f"  Data Repair: Adjusted {error_count} records where max_nights < min_nights.")
            else:
                print("  Stay Duration Validation OK: Booking ranges are consistent.")

        # ---------- UNIQUE LISTING INTEGRITY VALIDATION ----------
        # Primary Key Check: Detect duplicate IDs and ensure listing name consistency.
        if 'id' in df.columns:
            duplicate_ids = df['id'].duplicated().sum()
            if duplicate_ids > 0:
                print(f"  Warning: Found {duplicate_ids} duplicate listing IDs. Removing duplicates...")
                df = df.drop_duplicates(subset=['id'], keep='first')
            else:
                print("  Validation OK: All listing IDs are unique.")

            if 'name' in df.columns:
                name_inconsistency = (df.groupby('id')['name'].nunique() > 1).sum()
                if name_inconsistency > 0:
                    print(f"  Warning: {name_inconsistency} IDs have inconsistent names.")
                else:
                    print("  Validation OK: Listing names are consistent with their IDs.")

        # ---------- NUMERIC RANGE VALIDATION ----------
        # Enforce physical constraints on capacity and feature variables.
        # This ensures all quantitative data is logically sound for EDA.
        if 'accommodates' in df.columns:
            invalid_acc = (df['accommodates'] < 1).sum()
            df['accommodates'] = df['accommodates'].clip(lower=1)
            if invalid_acc > 0:
                print(f"  Range Correction: Adjusted {invalid_acc} records for 'accommodates' < 1.")

        pos_features = ['bathrooms', 'bedrooms', 'beds', 'amenities_count']
        for feature in pos_features:
            if feature in df.columns:
                invalid_feat = (df[feature] < 0).sum()
                if invalid_feat > 0:
                    df[feature] = df[feature].clip(lower=0)
                    print(f"  Range Correction: Fixed {invalid_feat} negative values in '{feature}'.")

        # ---------- DATA PERSISTENCE ----------
        # Persist the validated and cleaned dataset for downstream analysis.
        print(f"Validation completed. Records processed: {len(df)}")
        
        path_validado = "/tmp/listings_validated.parquet"
        df.to_parquet(path_validado, index=False)
        
        return path_validado


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

        # ---------- CALCULATED HOTEL ROOMS DERIVATION ----------
        # Derive the 'host_listings_count_hr' (hotel rooms) by subtracting 
        # all specific room types from the total calculated listings count.
        if 'host_listings_count' in df.columns:
            # We subtract entire homes (eh), private rooms (pr), and shared rooms (sr)
            # any remainder is attributed to hotel rooms (hr).
            df['host_listings_count_hr'] = (
                df['host_listings_count'] - 
                df['calculated_host_listings_count_entire_homes'] - 
                df['calculated_host_listings_count_private_rooms'] - 
                df['calculated_host_listings_count_shared_rooms'] # Usamos el nombre original antes del rename
            ).clip(lower=0) # Ensure no negative values due to data inconsistencies
            
            print("Hotel rooms count derived successfully.")

        # ---------- HOST_LOCATION PROXIMITY ANALYSIS ----------
        # Create a boolean flag 'host_near' to identify local hosts.
        # A host is considered 'local' if their location contains 'Málaga' 
        # or known surrounding areas, accounting for common misspellings/accents.
        if 'host_location' in df.columns:
            # Define local keywords (Málaga city and province areas)
            local_pattern = r'Málaga|Malaga|Marbella|Torremolinos|Fuengirola|Mijas|Nerja|Rincón de la Victoria|Andulucia|Andalucía'
            
            # Use string containment to flag local hosts (case insensitive)
            df['host_near'] = df['host_location'].str.contains(
                local_pattern, case=False, na=False, regex=True
            ).astype(int)
            
            print(f"Host proximity tagging complete: {df['host_near'].sum()} local hosts identified.")

        # ---------- HOST_LOCATION CATEGORIZATION ----------
        # Vectorized mapping of 'host_location' into 4 key groups: 
        # Málaga, Spain, International, and Unknown.
        if 'host_location' in df.columns:
            loc_lower = df['host_location'].str.lower().fillna('')

            df['host_location_cat'] = 'Iternational'
            
            spain_mask = loc_lower.str.contains('spain|españa', na=False)
            df.loc[spain_mask, 'host_location_cat'] = 'Spain'
            
            malaga_mask = loc_lower.str.contains('málaga|malaga', na=False)
            df.loc[malaga_mask, 'host_location_cat'] = 'Málaga'
            
            # Handle missing values
            df.loc[df['host_location'].isna(), 'host_location_cat'] = 'Unknown'

            # Convert to category type for optimization
            df['host_location_cat'] = df['host_location_cat'].astype('category')
            
            print("Host location successfully categorized into 4 levels.")

        # ---------- NEIGHBOURHOOD CENTER BINARIZATION ----------
        # Create a boolean flag 'neighbourhood_center' based on proximity.
        # Listings within a 2.0km radius of the city center coordinates 
        # are flagged as '1' (Central), otherwise '0'.
        if 'dist_center_km' in df.columns:
            # Threshold set at 2.0 km for urban core identification
            PROXIMITY_THRESHOLD = 2.0
            
            df['neighbourhood_center'] = (df['dist_center_km'] <= PROXIMITY_THRESHOLD).astype(int)
            
            central_count = df['neighbourhood_center'].sum()
            print(f"Center flagging complete: {central_count} listings identified in the urban core.")

        # ---------- REVENUE LOGARITHMIC TRANSFORMATION ----------
        # Apply a log transformation (log1p) to 'estimated_revenue' to normalize 
        if 'estimated_revenue' in df.columns:
            # Use np.log1p (calculates log(1 + x)) to safely handle zero values 
            # and avoid mathematical errors with non-positive numbers.
            df['revenue_log_scale'] = np.log1p(df['estimated_revenue'])
            
            print(f"Logarithmic transformation complete. New feature 'revenue_log_scale' created.")

        # ---------- MINIMUM_NIGHTS LOG TRANSFORMATION ----------
        # Apply a logarithmic transformation (log1p) to 'minimum_nights'.
        # This reduces the impact of extreme outliers 
        if 'minimum_nights' in df.columns:
            # We use log1p which calculates log(1 + x) to handle potential zero 
            # values and maintain numerical stability.
            df['minimum_nights_log'] = np.log1p(df['minimum_nights'])
            
            print(f"Transformation complete: 'minimum_nights_log' created from {len(df)} records.")

        # ---------- PRICE LOGARITHMIC TRANSFORMATION ----------
        # Apply a log transformation (log1p) to the 'price' variable.
        if 'price_num' in df.columns:
            # We use 'price_num' (the cleaned numeric version of price)
            # np.log1p handles potential zero prices safely.
            df['price_log'] = np.log1p(df['price_num'])
            
            print(f"Price transformation complete: 'price_log' created.")


        # ---------- LONG_STAY INDICATOR DERIVATION ----------
        # Create a boolean flag 'long_stay' to identify listings that allow 
        # bookings exceeding one year (365 nights).
        if 'maximum_nights' in df.columns:
            # Threshold set at 365 nights to define long-term availability
            YEAR_THRESHOLD = 365
            
            # Vectorized comparison: 1 if maximum_nights > 365, else 0
            df['long_stay'] = (df['maximum_nights'] > YEAR_THRESHOLD).astype(int)
            
            long_stay_count = df['long_stay'].sum()
            print(f"Long-stay segmentation complete: {long_stay_count} listings allow >1 year bookings.")

        # ---------- BATHROOM PRIVACY SEGMENTATION ----------
        # Extract the 'bathroom_shared' binary flag from the 'bathrooms_text' column.
        if 'bathrooms_text' in df.columns:
            # Normalize text to lowercase and handle missing values
            bath_text_lower = df['bathrooms_text'].str.lower().fillna('')
            
            # Define patterns that indicate a shared facility
            shared_pattern = r'shared|compartido|compartida'
            
            # Vectorized flag creation: 1 for shared, 0 for private
            df['bathroom_shared'] = bath_text_lower.str.contains(
                shared_pattern, regex=True
            ).astype(int)
            
            shared_count = df['bathroom_shared'].sum()
            print(f"Bathroom segmentation complete: {shared_count} shared facilities identified.")

        # We do the rename and feature selection inside enrihcment so we can work with some 
        # columns before deleting them.
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
            'calendar_updated','bathrooms_text','source','picture_url','license',
            'last_scraped','calendar_last_scraped'
        ]

        # errors='ignore' ensures the pipeline doesn't crash if a column was already removed.
        df = df.drop(columns=cols_to_drop, errors='ignore')

        # ---------- FEATURE RENAMING ----------
        # Map calculated host listing counts to shorter, more descriptive names
        renames = {
            'calculated_host_listings_count': 'host_listings_count',
            'calculated_host_listings_count_entire_homes': 'host_listings_count_eh',
            'calculated_host_listings_count_private_rooms': 'host_listings_count_pr',
            'calculated_host_listings_count_shared_rooms': 'host_listings_count_sr',
            'calculated_host_listings_count_hotel_rooms': 'host_listings_count_hr',
            'host_response_time': 'host_response_within_an_hour'
        }

        # Apply renaming. Using inplace=True to modify the existing DataFrame.
        df.rename(columns=renames, inplace=True)

        # ---------- DATA PERSISTENCE ----------
        print(f"Enrichment completed. Records processed: {len(df)}")
        
        path_enriquecido = "/tmp/listings_enriched.parquet"
        df.to_parquet(path_enriquecido, index=False)
        
        return path_enriquecido
    

    @task()
    def eda_report(path_archivo):
        """
        Exploratory Data Analysis (EDA): Generates an interactive HTML dashboard 
        with enriched features from the Malaga dataset.
        """
        df = pd.read_parquet(path_archivo)
        os.makedirs(EDA_OUTPUT_DIR, exist_ok=True)

        # ---------- SERIALIZATION HELPERS ----------
        def to_json(obj):
            if isinstance(obj, (np.integer,)):  return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.ndarray,)):  return obj.tolist()
            if isinstance(obj, pd.Timestamp):   return str(obj)
            return obj

        # ---------- GENERAL DATASET STATISTICS ----------
        total_rows = len(df)
        total_cols = len(df.columns)
        null_pct = (df.isnull().mean() * 100).round(2).to_dict()

        # ---------- PRICE ANALYSIS (Standard & Log) ----------
        price_series = df['price_num'].dropna()
        price_hist = np.histogram(price_series.clip(upper=500), bins=40)
        
        price_log_series = df['price_log'].dropna()
        price_log_hist = np.histogram(price_log_series, bins=30)

        price_data = {
            "bins": [round(float(x), 1) for x in price_hist[1][:-1]],
            "counts": price_hist[0].tolist(),
            "mean": round(float(price_series.mean()), 2),
            "median": round(float(price_series.median()), 2),
            "log_bins": [round(float(x), 2) for x in price_log_hist[1][:-1]],
            "log_counts": price_log_hist[0].tolist()
        }

        # ---------- ENRICHMENT SEGMENTATION (Kpis) ----------
        # Proximity and Location insights
        enrichment_data = {
            "host_near": {
                "Local (Málaga)": int(df['host_near'].sum()),
                "Remote/Other": int(len(df) - df['host_near'].sum())
            },
            "location_cats": df['host_location_cat'].value_counts().to_dict(),
            "urban_core": {
                "Center (<=2km)": int(df['neighbourhood_center'].sum()),
                "Periphery": int(len(df) - df['neighbourhood_center'].sum())
            },
            "long_stay_pct": round(float(df['long_stay'].mean() * 100), 2)
        }

        # ---------- GEOSPATIAL: DISTANCE VS PRICE ----------
        # Grouping price by distance deciles to see the trend
        df['dist_bin'] = pd.cut(df['dist_center_km'], bins=10)
        dist_price_trend = df.groupby('dist_bin', observed=True)['price_num'].median().fillna(0).tolist()
        dist_bins_labels = [f"{round(b.left,1)}-{round(b.right,1)}km" for b in df['dist_bin'].cat.categories]

        # ---------- EXISTING DISTRIBUTIONS ----------
        room_counts = df['room_type'].value_counts().to_dict()
        seg_counts  = df['host_segment'].value_counts().to_dict()
        
        # Correlation Matrix (Updated with Enrichment columns)
        corr_cols = ['price_num', 'dist_center_km', 'amenities_count', 'trust_score', 'host_tenure_days']
        corr_cols = [c for c in corr_cols if c in df.columns]
        corr_matrix = df[corr_cols].corr().round(3).values.tolist()

        # ---------- BUILD ENRICHED JSON PAYLOAD ----------
        payload_json = json.dumps({
            "meta": {"rows": total_rows, "cols": total_cols},
            "price": price_data,
            "enrichment": enrichment_data,
            "geo_trend": {"labels": dist_bins_labels, "data": dist_price_trend},
            "room_type": {str(k): int(v) for k, v in room_counts.items()},
            "host_segment": {str(k): int(v) for k, v in seg_counts.items()},
            "null_data": sorted(null_pct.items(), key=lambda x: -x[1])[:15],
            "corr": {"labels": corr_cols, "matrix": corr_matrix}
        }, default=to_json)

        # ---------- JINJA2 RENDERING ----------
        template_path = "/home/vboxuser/Documentos/proy_ssdd_II/src/transformations/report_template.html"
        out_path = os.path.join(EDA_OUTPUT_DIR, "eda_dashboard.html")

        if os.path.exists(template_path):
            with open(template_path, "r", encoding="utf-8") as f:
                template = Template(f.read())
            html_final = template.render(payload=payload_json)
            
            out_path = os.path.join(EDA_OUTPUT_DIR, "eda_dashboard.html")
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html_final)
            print(f"Enriched EDA dashboard generated at: {out_path}")
        else:
            print("Warning: report_template.html not found. Skipping HTML generation.")

        return path_archivo

    # ---------- DATA LOADING & PERSISTENCE ----------
    @task()
    def load(path_final: str):
        """
        LOAD: Final persistence stage. Converts the processed Parquet 
        back to CSV for business compatibility and final delivery.
        """

        # Load the final enriched dataset
        df = pd.read_parquet(path_final)
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        
        # Export to CSV for end-user accessibility
        df.to_csv(OUTPUT_FILE, index=False)
        
        print(f"PIPELINE COMPLETED. Final file saved at: {OUTPUT_FILE}")
        print(f"Final record count: {len(df)}")

   # ---------- DAG EXECUTION FLOW ----------
    
    raw_data_path = extract()      
    

    transformed_data_path = transform(raw_data_path) 
    validated_data_path = validation(transformed_data_path)
    enriched_data_path = enrichment(validated_data_path) 
    eda_report(enriched_data_path) 
    load(enriched_data_path)

# Instantiate the DAG
dag_instance = airbnb_listings_pipeline()