import pandas as pd
import os
import re
from airflow.decorators import task, dag
from datetime import datetime
import matplotlib.pyplot as plt
from collections import Counter

# ---------- Configuration & Paths ----------
DATA_DIR = "/opt/airflow/data"
INPUT_FILE = f"{DATA_DIR}/reviews.csv"
TMP_DIR = f"{DATA_DIR}/tmp_reviews_processing"
# Directory dedicated to EDA images
PLOT_DIR = f"{DATA_DIR}/eda_results"

@dag(
    schedule=None,
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["reviews", "processing", "code_comments", "png_outputs"],
)
def dag_reviews_analysis():

    @task()
    def extract():
        # ---------- Initializing Extraction ----------
        os.makedirs(TMP_DIR, exist_ok=True)
        
        # ---------- Reading Input CSV ----------
        df = pd.read_csv(INPUT_FILE)
        
        output_path = f"{TMP_DIR}/raw_data.parquet"
        
        # ---------- Saving to Parquet ----------
        df.to_parquet(output_path, index=False)
        
        return output_path

    @task()
    def transform(path):
        # ---------- Loading Data for Transformation ----------
        df = pd.read_parquet(path)
        
        # ---------- Converting Dates ----------
        df['date'] = pd.to_datetime(df['date'])
        
        # ---------- Cleaning Null Values ----------
        # Drop rows with null comments and reviewer IDs
        df = df.dropna(subset=['comments', 'reviewer_id'])
        
        # ---------- Adding New Data ----------
        df['comments_len'] = df['comments'].astype(str).apply(len)
        
        # ---------- Dropping Unnecessary Columns ----------
        if 'reviewer_name' in df.columns:
            df = df.drop(columns=['reviewer_name'])
            
        output_path = f"{TMP_DIR}/transformed_data.parquet"
        
        # ---------- Saving Transformed Parquet ----------
        df.to_parquet(output_path, index=False)
        return output_path

    @task()
    def validate(path):
        # ---------- Starting Validation Process ----------
        df = pd.read_parquet(path)
        
        # ---------- Checking Critical Columns ----------
        if df['listing_id'].isnull().any():
            # ---------- Validation Error Found ----------
            raise ValueError("Data Integrity Error: Null listing_ids detected.")
            
        # ---------- Validation Successful ----------
        return path

    @task()
    def eda(path):
        # ---------- Starting EDA Visualizations ----------
        df = pd.read_parquet(path)
        # Ensure the plots directory exists
        os.makedirs(PLOT_DIR, exist_ok=True)
        
        # ---------- Generating Time Series Plot ----------
        df['year'] = df['date'].dt.year
        plt.figure(figsize=(10, 5))
        df.groupby('year').size().plot(kind='line', marker='o', color='navy')
        plt.title('Reviews per Year')
        
        # ---------- Saving Time Series Plot as PNG ----------
        plt.savefig(f"{PLOT_DIR}/reviews_per_year.png", format='png')
        plt.close()

        # ---------- Analyzing Top Reviewers ----------
        plt.figure(figsize=(10, 5))
        df['reviewer_id'].value_counts().head(10).plot(kind='bar', color='orange')
        plt.title('Top 10 Reviewers by ID')
        
        # ---------- Saving Top Reviewers Plot as PNG ----------
        plt.savefig(f"{PLOT_DIR}/top_reviewers_id.png", format='png')
        plt.close()

        # ---------- Computing Word Frequency ----------
        all_text = " ".join(df['comments'].astype(str).str.lower())
        cleaned_text = re.sub(r'[^a-z\s]', '', all_text)
        STOPWORDS = {'the', 'a', 'an', 'and', 'is', 'in', 'it', 'of', 'for', 'with', 'to'}
        words = [w for w in cleaned_text.split() if w not in STOPWORDS and len(w) > 2]
        
        word_counts = Counter(words)
        df_words = pd.DataFrame(word_counts.most_common(20), columns=['Word', 'Count'])
        
        # ---------- Generating Word Frequency Plot ----------
        plt.figure(figsize=(10, 8))
        plt.barh(df_words['Word'], df_words['Count'], color='teal')
        plt.gca().invert_yaxis()
        plt.title('Top 20 Frequent Words')
        
        # ---------- Saving Word Frequency Plot as PNG ----------
        plt.savefig(f"{PLOT_DIR}/word_frequency.png", format='png')
        plt.close()
        
        # ---------- EDA Complete ----------

    @task()
    def load(path):
        # ---------- Starting Final Load ----------
        df = pd.read_parquet(path)
        final_path = f"{DATA_DIR}/final_reviews_dataset.csv"
        
        # ---------- Exporting Final CSV ----------
        df.to_csv(final_path, index=False)
        # ---------- Pipeline Finished Successfully ----------

    # ---------- Pipeline Execution Flow ----------
    raw_path = extract()
    transformed_path = transform(raw_path)
    validated_path = validate(transformed_path)
    
    # Run EDA and Load tasks in parallel
    eda(validated_path)
    load(validated_path)

# ---------- DAG Instance Initialization ----------
dag_instance = dag_reviews_analysis()