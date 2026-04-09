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

        # ---------- PREPARE DATA FOR HTML PAYLOAD ----------
        # 1. Basic Stats
        total_rows = len(df)
        unique_users = int(df['reviewer_id'].nunique())
        unique_listings = int(df['listing_id'].nunique())
        avg_comment_len = float(df['comments_len'].mean())

        # 2. Timeline Data (Using the 'year' column you already created)
        yearly_counts = df.groupby('year').size().sort_index()
        yearly_data = {
            "labels": [str(y) for y in yearly_counts.index],
            "data": yearly_counts.values.tolist()
        }

        # 3. Top Reviewers Data
        top_reviewers = df['reviewer_id'].value_counts().head(10)
        reviewers_data = {
            "labels": [str(i) for i in top_reviewers.index],
            "data": top_reviewers.values.tolist()
        }

        # 4. Length Distribution Data (Histogram)
        # Bins for the length distribution chart
        counts, bins = np.histogram(df['comments_len'], bins=10, range=(0, int(df['comments_len'].quantile(0.95))))
        length_data = {
            "labels": [f"{int(bins[i])}-{int(bins[i+1])}" for i in range(len(bins)-1)],
            "data": counts.tolist()
        }

        # 5. Word Frequency Data (Using your existing df_words)
        word_data = {
            "labels": df_words['Word'].tolist(),
            "data": df_words['Count'].tolist()
        }

        # ---------- BUILD JSON PAYLOAD FOR TEMPLATE ----------
        payload_json = json.dumps({
            "meta": {
                "total_reviews": total_rows,
                "unique_users": unique_users,
                "unique_listings": unique_listings,
                "avg_len": round(avg_comment_len, 2)
            },
            "yearly": yearly_data,
            "reviewers": reviewers_data,
            "length_dist": length_data,
            "words": word_data
        })

        # ---------- JINJA2 TEMPLATE RENDERING ----------
        # Get directory of the current DAG file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        template_path = os.path.join(script_dir, "report_reviews.html")

        # Read the HTML template file
        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        # Render the payload into the HTML structure
        template = Template(template_content)
        html_final = template.render(payload=payload_json)

        # ---------- SAVE FINAL DASHBOARD ----------
        # Save the generated HTML to the PLOT_DIR
        out_path = os.path.join(PLOT_DIR, "eda_dashboard_reviews.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_final)

        print(f"---------- EDA Dashboard generated successfully at: {out_path} ----------")
        
        # ---------- EDA Complete ----------
        return path

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