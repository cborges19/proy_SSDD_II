from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os

from src.reports.report_calendar import eda_calendar
import logging

from src.utils import *
from src.kafka.producer_kafka import produce_to_kafka_avro

from src.utils import DATA_DIR, OUTPUT_DIR, TEMPLATES_DIR

TEMPLATE_PATH = TEMPLATES_DIR / "report_calendar.html"
output_dir = str(OUTPUT_DIR / "reports" / "calendar")

# Initialize the Airflow task logger for real-time observability
log = logging.getLogger("airflow.task")

@dag(
    start_date=datetime(2026, 4, 9),
    schedule=None,
    catchup=False,
    tags=['airbnb', 'calendar', 'time_series', 'production']
)
def airbnb_calendar_pipeline():

    # ---------------------------------------------------------
    # TASK 1: EXTRACT
    # ---------------------------------------------------------
    @task
    def extract_raw_calendar():
        # Load raw calendar data and convert to Parquet for performance
        return extract_data(str(DATA_DIR / 'calendar.csv'), 'airbnb_calendar_raw')

    # ---------------------------------------------------------
    # TASK 2: TRANSFORM (Physical Cleaning & Typification)
    # ---------------------------------------------------------
    @task
    def transform_calendar(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- AVAILABLE TO BOOLEAN ---------
        # Convert Airbnb 't'/'f' strings into Python boolean types
        log.info("Mapping Airbnb 't'/'f' strings to boolean types")
        df = map_airbnb_bool(df, ['available'])

        # --------- DATE NORMALIZATION ---------
        # Convert date strings to datetime objects for temporal analysis
        log.info("Normalizing date column for temporal analysis")
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # --------- DROP NULL COLUMNS ---------
        # Remove price and adjusted_price as they are identified as 100% null
        log.info("Dropping columns identified as 100% null: ['price', 'adjusted_price']")
        df = df.drop(columns=['price', 'adjusted_price'], errors='ignore')

        output_path = "/tmp/calendar_transformed.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 3: ENRICHMENT (Temporal Feature Engineering)
    # ---------------------------------------------------------
    @task
    def enrichment_calendar(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- TEMPORAL VARIABLES ---------
        # Extract weekend flag and time components to analyze occupation patterns
        log.info("Extracting temporal components and weekend flags")
        df['day_of_week'] = df['date'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(bool)
        df['month'] = df['date'].dt.month

        # --------- DAYS OF EVENT ---------
        log.info("Mapping special events based on calendar dates")
        unique_date = df['date'].unique()
        event_map = {date: get_event(pd.Timestamp(date)) for date in unique_date}
        df['event'] = df['date'].map(event_map)

        # ---------- OCCUPANCY VARIABLE ----------
        log.info("Calculating occupancy variable (booked) from availability status")
        df['booked'] = ~df['available']

        output_path = "/tmp/calendar_enriched.parquet"
        df.to_parquet(output_path, index=False)
        return output_path

    # ---------------------------------------------------------
    # TASK 4: VALIDATE
    # ---------------------------------------------------------
    @task
    def validate_calendar(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        df = pd.read_parquet(file_path)

        # --------- DATA QUALITY RULES ---------
        # Define validation criteria for boolean and date formats
        validation_rules = [
            {'column': 'available', 'check': 'category', 'params': [True, False], 
             'message': 'Invalid format in available column'},
            {'column': 'date', 'check': 'range', 'params': (None, '2027-12-31'), 
             'message': 'Date out of logical range or null'}
        ]
        
        # Report failures to Kafka sidecar topic for observability
        validate_and_report(df, validation_rules)
        
        return file_path

# ---------------------------------------------------------
    # TASK 5: EDA (Reporting)
    # ---------------------------------------------------------
    @task
    def generate_calendar_eda(file_path: str):
        # Access Airflow's internal task logger
        log = logging.getLogger("airflow.task")
        # --------- TEMPORAL OCCUPANCY ANALYSIS ---------           
        # Define the destination directory for plots and the HTML dashboard
        output_dir = "data/output/reports/calendar"
    
        # Execute the automated visual analysis and report generation
        eda_calendar(
            transformed_path=file_path, 
            output_dir=output_dir, 
            log=log,
            template_path=TEMPLATE_PATH
        )

        return file_path

    # ---------------------------------------------------------
    # TASK 6: LOAD
    # ---------------------------------------------------------
    @task
    def load_calendar_to_kafka(file_path: str):
        # Produce the final gold calendar data to Kafka
        produce_to_kafka_avro(
            file_path=file_path,
            topic='airbnb_calendar_gold'
        )

    # Workflow Definition
    raw_data = extract_raw_calendar()
    phys_clean = transform_calendar(raw_data)
    logic_enriched = enrichment_calendar(phys_clean)
    validated_data = validate_calendar(logic_enriched)
    eda_done = generate_calendar_eda(validated_data)
    load_calendar_to_kafka(eda_done)

# Instantiate the DAG
calendar_dag = airbnb_calendar_pipeline()