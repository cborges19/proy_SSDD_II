from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

from src.reports.report_reviews import eda_reviews
import logging

from src.utils import *
from src.kafka.producer_kafka import produce_to_kafka_avro

from src.utils import DATA_DIR, OUTPUT_DIR, TEMPLATES_DIR

TEMPLATE_PATH = TEMPLATES_DIR / "report_reviews.html"
output_dir = str(OUTPUT_DIR / "reports" / "reviews")

# Initialize the Airflow task logger
log = logging.getLogger("airflow.task")

@dag(
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=['airbnb', 'reviews', 'nlp', 'gold_layer']
)
def airbnb_reviews_pipeline():

    # ---------------------------------------------------------
    # TASK 1: EXTRACT
    # ---------------------------------------------------------
    @task
    def extract_raw_reviews():
        # Load raw review data and convert to Parquet for performance
        return extract_data(str(DATA_DIR / 'reviews.csv'), 'airbnb_reviews_raw')
    
    # ---------------------------------------------------------
    # TASK 2: TRANSFORM (Physical Cleaning & Null Management)
    # ---------------------------------------------------------
    @task
    def transform_reviews(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- REMOVE NULL CRITICAL ROWS ---------
        # Delete rows where reviewer_id or comments are missing
        # If comments are missing, the review provides no NLP value
        rows_before = len(df)
        df = df.dropna(subset=['id', 'reviewer_id', 'comments'])
        rows_after = len(df)
        log.info(f"Cleaned nulls: dropped {rows_before - rows_after} rows. Remaining: {rows_after} records.")

        # --------- DATE NORMALIZATION ---------
        # Convert date strings to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        invalid_dates = df['date'].isna().sum()
        log.info(f"Date normalization completed. {invalid_dates} invalid dates found and coerced to null.")

        # --------- DROP UNNECESSARY COLUMNS ---------
        # Remove reviewer_name as it is not needed for the analysis
        log.info("Removing non-analytical columns for data optimization")
        df = df.drop(columns=['reviewer_name'], errors='ignore')

        output_path = "/tmp/reviews_transformed.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 3: ENRICHMENT (NLP & Text Metrics)
    # ---------------------------------------------------------
    @task
    def enrichment_reviews(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- COMMENTS LENGTH ---------
        # Calculate the raw character count of the review
        log.info("Calculating review comment lengths")
        df['comments_len'] = df['comments'].str.len().fillna(0).astype(int)

        # --------- NLP PREPROCESSING ---------
        # Apply standardized NLP cleaning using the utility function
        # This includes lowercase, HTML removal, and symbol normalization
        log.info("Starting NLP text cleaning and normalization")
        df['comments_clean'] = df['comments'].apply(clean_text_nlp)
        log.info("NLP preprocessing completed successfully")

        # --------- TEMPORAL FEATURES ---------
        # Extract Year/Month for the upcoming EDA tasks
        log.info("Extracting temporal features (year, month) from dates")
        df['review_year'] = df['date'].dt.year
        df['review_month'] = df['date'].dt.month

        output_path = "/tmp/reviews_enriched.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 4: VALIDATE
    # ---------------------------------------------------------
    @task
    def validate_reviews(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- DATA QUALITY RULES ---------
        # Ensure listing_id is valid and comments actually have content
        validation_rules = [
            {'column': 'date', 'check': 'date_limit', 'params': '2025-09-30', 
             'message': 'Review date is in the future relative to extraction'},
            {'column': 'comments_len', 'check': 'range', 'params': (1, None), 
             'message': 'Review comment is empty after cleaning'}
        ]
        
        # Report failures to Kafka DLQ for observability
        validate_and_report(df, validation_rules)
        
        return file_path

    # ---------------------------------------------------------
    # TASK 5: EDA (Reporting & NLP Insights)
    # ---------------------------------------------------------
    @task
    def generate_reviews_eda(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        # --------- REVIEWS VISUAL ANALYSIS ---------        
        # Define the output directory for review reports
        output_dir = "data/output/reports/reviews"
        
        # Execute the Python-native EDA function
        eda_reviews(
            file_path=file_path, 
            output_dir=output_dir, 
            log=log,
            template_path=TEMPLATE_PATH
        )
        
        return file_path

    # ---------------------------------------------------------
    # TASK 6: LOAD
    # ---------------------------------------------------------
    @task
    def load_reviews_to_kafka(file_path: str):
        # Produce the final gold reviews data to Kafka in Avro format
        produce_to_kafka_avro(
            file_path=file_path,
            topic='airbnb_reviews_gold'
        )

    # Workflow Definition
    raw_data = extract_raw_reviews()
    phys_clean = transform_reviews(raw_data)
    logic_enriched = enrichment_reviews(phys_clean)
    validated_data = validate_reviews(logic_enriched)
    eda_done = generate_reviews_eda(validated_data)
    load_reviews_to_kafka(eda_done)

# Instantiate the DAG
reviews_dag = airbnb_reviews_pipeline()