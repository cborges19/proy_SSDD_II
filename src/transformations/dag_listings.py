from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import numpy as np
import os

from src.reports.report_listings import eda_listings
import logging

import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent

from src.utils import *
from src.utils import DATA_DIR, OUTPUT_DIR, TEMPLATES_DIR


TEMPLATE_PATH = TEMPLATES_DIR / "report_listings.html"
output_dir = str(OUTPUT_DIR / "reports" / "listings")

@dag(
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=['airbnb', 'listings', 'gold_layer', 'production']
)
def airbnb_master_pipeline():

    # ---------------------------------------------------------
    # TASK 1: EXTRACT
    # ---------------------------------------------------------
    @task
    def extract_raw_data():
        # Load raw data and convert to Parquet for pipeline performance
        return extract_data(str(DATA_DIR / 'listings.csv'), 'airbnb_listings_raw')
    
    # ---------------------------------------------------------
    # TASK 2: TRANSFORM (Physical Cleaning & Typification)
    # ---------------------------------------------------------
    @task
    def transform_data(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task") 
        df = pd.read_parquet(file_path)

        # --------- PROPERTY TYPE TRANSLATION ---------
        # Translate specific Spanish property types to English
        log.info("Translating property types to English")
        df = translate_values(df, 'property_type', {'Casa Particular': 'Private House'})

        # --------- BATHROOMS DATA EXTRACTION ---------
        # Extract numeric value from the text description of bathrooms
        log.info("Extracting numeric bathroom counts from text descriptions")
        df['bathrooms'] = parse_numeric_from_text(df['bathrooms_text'])

        # --------- MANUAL BATHROOM NULL IMPUTATION ---------
        # Fix specific IDs where bathrooms were identified manually
        log.info("Applying manual fixes for bathroom outliers by listing ID")
        df = manual_impute_by_id(df, 'id', [907846578011191658], 'bathrooms', 1.0)
        df = manual_impute_by_id(df, 'id', [1071778548709501720, 1149444780882723841, 
                                           1199390966969605315, 1209436626693592917], 'bathrooms', 0.0)

        # --------- BEDROOMS INITIAL IMPUTATION ---------
        # Shared or Private rooms are assumed to have at least 1 bedroom
        log.info("Imputing missing bedrooms using room type logic and median")
        room_mask = df['room_type'].isin(['Shared room', 'Private room']) & df['bedrooms'].isna()
        df.loc[room_mask, 'bedrooms'] = 1.0
        df = impute_median(df, 'bedrooms', 'accommodates')

        # --------- BEDS MEDIAN IMPUTATION ---------
        # Fill missing bed counts using the median grouped by accommodates
        log.info("Imputing missing beds using median grouped by accommodates")
        df = impute_median(df, 'beds', 'accommodates')

        # --------- CURRENCY AND PRICE CLEANING ---------
        # Clean currency symbols and fix specific outlier ID 47444051
        log.info("Cleaning currency symbols and normalizing price values")
        df = manual_impute_by_id(df, 'id', [47444051], 'price', "182.0")
        df['price'] = clean_currency(df['price'])

        # --------- HOST RATES CLEANING & IMPUTATION ---------
        # Clean rates strings to numerical floats
        log.info("Cleaning and imputing host response/acceptance rates")
        df['host_response_rate'] = clean_currency(df['host_response_rate'])
        df['host_acceptance_rate'] = clean_currency(df['host_acceptance_rate'])
        
        # Impute rates using median with noise grouped by superhost status
        df = impute_median_with_noise(df, 'host_acceptance_rate', 'host_is_superhost')
        # Response time imputation (mode) before using it as a grouping key for response rate
        df = impute_mode(df, 'host_response_time', 'host_is_superhost')
        df = impute_median_with_noise(df, 'host_response_rate', ['host_is_superhost', 'host_response_time'])

        # Normalizing rates to [0, 1] range
        rate_cols = ['host_response_rate', 'host_acceptance_rate']
        normalize_rates(df, rate_cols)

        # --------- DATE AND TIME NORMALIZATION ---------
        # Convert date strings to datetime objects for calculation
        log.info("Converting host and review columns to datetime objects")
        df['host_since'] = pd.to_datetime(df['host_since'], errors='coerce')
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
        df['first_review'] = pd.to_datetime(df['first_review'], errors='coerce')

        # --------- BOOLEAN MAPPING ---------
        # Standardize Airbnb 't'/'f' strings into Python boolean types
        bool_cols = [
            'host_is_superhost', 'host_has_profile_pic', 
            'host_identity_verified', 'has_availability', 'instant_bookable'
        ]
        log.info(f"Mapping Airbnb 't'/'f' to boolean for columns: {bool_cols}")
        df = map_airbnb_bool(df, bool_cols)

        output_path = "/tmp/listings_transformed.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 3: ENRICHMENT (Feature Engineering & Logic)
    # ---------------------------------------------------------
    @task
    def enrichment_data(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- BATHROOM SHARED FEATURE ---------
        # Create flag if 'shared' appears in text and bathroom count is relevant
        log.info("Applying heuristic imputation for superhost status and shared bathroom flags")
        df['bathroom_shared'] = binarize_by_keywords(df['bathrooms_text'], 'shared|compartido')

        # --------- SUPERHOST HEURISTIC IMPUTATION ---------
        # Fill missing superhost status based on Airbnb official requirements
        superhost_logic = (
            (df['number_of_reviews'] >= 10) & 
            (df['host_response_rate'] >= 90) & 
            (df['host_acceptance_rate'] >= 90) & 
            (df['review_scores_rating'] >= 4.8)
        )
        df['host_is_superhost'] = df['host_is_superhost'].fillna(superhost_logic)

        # --------- AMENITIES COUNT AND PARSING ---------
        log.info("Parsing amenities list and calculating feature counts")
        df['amenities_list'] = df['amenities'].apply(parse_amenities)
        df['num_amenities'] = df['amenities_list'].apply(len)

        # --------- HOST LISTINGS CALCULATIONS ---------
        # Calculate hotel room count and host-specific listing metrics
        log.info("Identifying professional agencies and calculating host proximity to Malaga")
        df['host_listings_count_hr'] = (
            df['calculated_host_listings_count'] - (
                df.get('calculated_host_listings_count_entire_homes', 0) + 
                df.get('calculated_host_listings_count_private_rooms', 0) + 
                df.get('calculated_host_listings_count_shared_rooms', 0)
            )
        ).clip(lower=0)

        # --------- COMPANY AND HOST PROXIMITY ---------
        # Identify company if keyword found OR host has 5 or more total listings
        keyword_company = binarize_by_keywords(df['host_name'], 'sl|sa|company|management|rentals|apartments')
        df['company'] = ((keyword_company == 1) | (df['host_total_listings_count'] >= 5)).astype(int)
        
        def categorize_location(loc):
            loc = str(loc).lower()
            if 'malaga' in loc or 'málaga' in loc: return 'Malaga'
            if 'spain' in loc or 'españa' in loc: return 'Spain'
            if 'nan' in loc or 'unknown' in loc: return 'Unknown'
            return 'International'
        
        df['host_location_cat'] = df['host_location'].apply(categorize_location)
        df['host_near'] = (df['host_location_cat'] == 'Malaga').astype(int)

        # --------- NEIGHBOURHOOD CENTER ANALYSIS ---------
        log.info("Flagging listings located in historical or central neighborhoods")
        df['neighbourhood_center'] = binarize_by_keywords(df['neighbourhood_cleansed'], 'centro|histórico|soho')
        df['description'] = df['description'].apply(clean_text_nlp)

        # --------- REVENUE AND AVAILABILITY LOGIC ---------
        log.info("Estimating annual revenue (L365D/N365D) based on occupancy and price")
        df['has_availability'] = df['has_availability'].fillna(df['availability_365'] > 0)
        df['estimated_revenue_l365d'] = (df['price'] * (df['estimated_occupancy_l365d'])).clip(lower=0)
        df['estimated_revenue_n365d'] = (df['price'] * (365 - df['availability_365'])).clip(lower=0)

        # --------- REVIEW RECENCY AND COUNTS ---------
        log.info("Calculating review recency metrics and filling missing counts")
        df['has_review'] = (df['number_of_reviews'] > 0).astype(int)
        df['has_review_ly'] = (df['last_review'].dt.year == 2025).astype(int)
        df['reviews_per_month'] = df['reviews_per_month'].fillna(0)
        df = calculate_days_since(df, 'last_review', reference_date='2025-09-30')

        # --------- HOST VERIFICATIONS OHE ---------
        # Expand verification list into multiple binary columns
        log.info("Expanding host verifications into one-hot encoded binary columns")
        df = get_ohe_from_list_column(df, 'host_verifications', 'host_verif')

        # --------- GEOSPATIAL ANALYSIS (HAVERSINE) ---------
        # Accurate distance to Malaga Center (36.7213, -4.4214)
        MALAGA_CENTER = (36.7213, -4.4214)
        df['dist_center_km'] = haversine_distance(
            df['latitude'], df['longitude'], MALAGA_CENTER[0], MALAGA_CENTER[1]
        )

        # --------- TRUST SCORE (WEIGHTED RATING) ---------
        # Penalizes high ratings with very few reviews
        df['trust_score'] = df['review_scores_rating'].fillna(0) * np.log1p(df['number_of_reviews'])

        # --------- HABITABILITY RATIO ---------
        # Ratio of 1.0 means 1 bed per guest
        df['ratio_beds_accommodates'] = np.where(
            df['accommodates'] > 0, df['beds'] / df['accommodates'], 0
        )

        # --------- HOST SEGMENTATION (3 LEVELS) ---------
        def segment_host(count):
            if count == 1: return 'Individual'
            if count < 5: return 'Small_Investor'
            return 'Agency'
        
        # Use the original column before renaming
        df['host_segment'] = df['calculated_host_listings_count'].apply(segment_host)

        # Standardize Airbnb 't'/'f' strings into Python boolean types
        bool_cols = [
            'host_is_superhost', 'host_has_profile_pic', 
            'host_identity_verified', 'has_availability', 'instant_bookable'
        ]
        log.info(f"Mapping Airbnb 't'/'f' to boolean for columns: {bool_cols}")
        df = map_airbnb_bool(df, bool_cols)

        # --------- VARIABLE RENAMING ---------
        # Before rename every columns, we need to drop the column host_listings_count, to avoid duplicated columns
        df = df.drop('host_listings_count', axis=1)

        # Rename specific columns to match business terminology
        rename_map = {
            'host_response_time': 'host_response_hour',
            'calculated_host_listings_count': 'host_listings_count',
            'calculated_host_listings_count_entire_homes': 'host_listings_count_eh',
            'calculated_host_listings_count_private_rooms': 'host_listings_count_pr',
            'calculated_host_listings_count_shared_rooms': 'host_listings_count_sr'
        }
        df = df.rename(columns=rename_map)
        # Final host_response_hour binarization (1 if <1h)
        df['host_response_hour'] = binarize_by_keywords(df['host_response_hour'], 'within an hour')

        # --------- LOG TRANSFORMATIONS & CLIPPING ---------
        log.info("Applying log1p transformations to skewed variables and clipping outliers")
        log_cols = ['price', 'estimated_revenue_l365d', 'estimated_revenue_n365d', 
                    'minimum_nights', 'number_of_reviews', 'number_of_reviews_ltm',
                    'number_of_reviews_l30d', 'number_of_reviews_ly', 'host_listings_count',
                    'host_listings_count_eh', 'host_listings_count_sr', 'host_listings_count_pr',
                    ]
        df = apply_log1p_transformation(df, log_cols)
        # DUDAS
        df['long_stay'] = (df['maximum_nights'] > 365).astype(int)
        df = apply_clip(df, 'maximum_nights', upper_limit=365)

        # --------- BOOLEAN MAPPING ---------
        # Standardize boolean variables
        bool_cols = [
            'host_response_hour', 'bathroom_shared', 
            'company', 'host_near', 'long_stay'
        ]
        log.info(f"Mapping to boolean for columns: {bool_cols}")
        df = map_airbnb_bool(df, bool_cols)

        # --------- FINAL COLUMN CLEANUP ---------
        log.info("Performing final column cleanup and business terminology renaming")
        drop_cols = [
            'scrape_id', 'host_thumbnail_url', 'host_picture_url', 
            'minimum_minimum_nights', 'maximum_minimum_nights', 
            'minimum_maximum_nights', 'maximum_maximum_nights',
            'minimum_nights_avg_ntm', 'maximum_nights_avg_ntm',
            'host_about', 'host_url', 'host_neighbourhood',
            'neighborhood_overview', 'neighbourhood', 'neighbourhood_group_cleansed', 
            'calendar_updated','bathrooms_text','source','picture_url','license',
            'last_scraped','calendar_last_scraped', 'property_type',
            'name', 'listing_url', 'host_location', 'host_verifications', 'amenities'
        ]
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])

        output_path = "/tmp/listings_enriched.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 4: VALIDATE
    # ---------------------------------------------------------
    @task
    def validate_data(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- DATA QUALITY RULES LIST ---------
        validation_rules = [
            {'column': 'id', 'check': 'unique', 'message': 'Duplicate Listing IDs found'},
            {'column': 'accommodates', 'check': 'range', 'params': (1, None), 'message': 'Accommodates must be >= 1'},
            {'column': 'bathrooms', 'check': 'range', 'params': (0, None), 'message': 'Bathrooms must be >= 0'},
            {'column': 'bedrooms', 'check': 'range', 'params': (0, None), 'message': 'Bedrooms must be >= 0'},
            {'column': 'price', 'check': 'range', 'params': (1, 100000), 'message': 'Price anomaly detected'},
            {'column': 'latitude', 'check': 'range', 'params': (36.3, 37.3), 'message': 'Latitude outside Malaga province'},
            {'column': 'longitude', 'check': 'range', 'params': (-5.5, -3.8), 'message': 'Longitude outside Malaga province'},
            {'column': 'last_review', 'check': 'date_limit', 'params': '2025-09-30', 'message': 'Future review date detected'},
            # Availability ranges
            {'column': 'availability_30', 'check': 'range', 'params': (0, 30), 'message': 'Availability_30 out of range'},
            {'column': 'availability_60', 'check': 'range', 'params': (0, 60), 'message': 'Availability_60 out of range'},
            {'column': 'availability_90', 'check': 'range', 'params': (0, 90), 'message': 'Availability_90 out of range'},
            {'column': 'availability_365', 'check': 'range', 'params': (0, 365), 'message': 'Availability_365 out of range'},
            {'column': 'availability_eoy', 'check': 'range', 'params': (0, 62), 'message': 'Availability_eoy out of range'}
        ]

        # Ratings ranges (0-5)
        rating_cols = ['review_scores_rating', 'review_scores_location', 'review_scores_value', 
                       'review_scores_accuracy', 'review_scores_cleanliness', 'review_scores_checkin', 
                       'review_scores_communication']
        for col in rating_cols:
            validation_rules.append({'column': col, 'check': 'range', 'params': (0, 5), 'message': f'{col} out of range (0-5)'})

        # Logic checks
        validation_rules.append({
            'check': 'logic', 
            'params': lambda row: row['maximum_nights'] >= row['minimum_nights'], 
            'message': 'Maximum nights logic failure'
        })
        validation_rules.append({
            'check': 'logic', 
            'params': lambda row: row['bedrooms'] in [0, 1] if row['room_type'] in ['Private room', 'Shared room'] else True, 
            'message': 'Room type bedroom consistency error'
        })
        # If has_availability is False, other availability must be 0
        validation_rules.append({
            'check': 'logic',
            'params': lambda row: row['availability_365'] == 0 if row['has_availability'] == False else True,
            'message': 'Availability mismatch when has_availability is False'
        })
        
        validate_and_report(df, validation_rules)
        return file_path

    # ---------------------------------------------------------
    # TASK 5: EDA (Reporting & Insights)
    # ---------------------------------------------------------
    @task
    def generate_eda_report(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        # --------- ENRICHED LISTINGS VISUAL ANALYSIS ---------        
        # Define specific report storage path
        output_dir = "data/output/reports/listings"
        
        # Execute the Python-native report generation
        eda_listings(
            file_path=file_path, 
            output_dir=output_dir, 
            log=log,
            template_path=TEMPLATE_PATH
        )
        
        return file_path

    @task
    def load_to_kafka(file_path: str):
        produce_to_kafka_avro(file_path=file_path, topic='airbnb_listings_gold')

    # Workflow Definition
    raw_data = extract_raw_data()
    phys_clean = transform_data(raw_data)
    logic_enriched = enrichment_data(phys_clean)
    validated_data = validate_data(logic_enriched)
    eda_done = generate_eda_report(validated_data)
    load_to_kafka(eda_done)

# Instantiate the DAG
dag_instance = airbnb_master_pipeline()