import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import re
import json
from collections import Counter
from jinja2 import Template

def eda_reviews(file_path, output_dir, log, template_path):
    """
    Performs Exploratory Data Analysis on the reviews dataset, 
    generating time series, word frequency analysis, and an HTML dashboard.
    """
    # ---------- STARTING EDA VISUALIZATIONS ----------
    df = pd.read_parquet(file_path)
    # Ensure the plots directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # ---------- GENERATING TIME SERIES PLOT ----------
    # Grouping by year to visualize review volume over time
    df['year'] = df['date'].dt.year
    plt.figure(figsize=(10, 5))
    df.groupby('year').size().plot(kind='line', marker='o', color='navy')
    plt.title('Reviews per Year')
    
    # ---------- SAVING TIME SERIES PLOT AS PNG ----------
    plt.savefig(f"{output_dir}/reviews_per_year.png", format='png')
    plt.close()

    # ---------- ANALYZING TOP REVIEWERS ----------
    # Identifying the most active users in the dataset
    plt.figure(figsize=(10, 5))
    df['reviewer_id'].value_counts().head(10).plot(kind='bar', color='orange')
    plt.title('Top 10 Reviewers by ID')
    
    # ---------- SAVING TOP REVIEWERS PLOT AS PNG ----------
    plt.savefig(f"{output_dir}/top_reviewers_id.png", format='png')
    plt.close()

    # ---------- COMPUTING WORD FREQUENCY ----------
    # Text normalization and stopword removal for NLP insights
    all_text = " ".join(df['comments'].astype(str).str.lower())
    cleaned_text = re.sub(r'[^a-z\s]', '', all_text)
    STOPWORDS = {'the', 'a', 'an', 'and', 'is', 'in', 'it', 'of', 'for', 'with', 'to', 'was', 'were', 'very'}
    words = [w for w in cleaned_text.split() if w not in STOPWORDS and len(w) > 2]
    
    word_counts = Counter(words)
    df_words = pd.DataFrame(word_counts.most_common(20), columns=['Word', 'Count'])
    
    # ---------- GENERATING WORD FREQUENCY PLOT ----------
    plt.figure(figsize=(10, 8))
    plt.barh(df_words['Word'], df_words['Count'], color='teal')
    plt.gca().invert_yaxis()
    plt.title('Top 20 Frequent Words')
    
    # ---------- SAVING WORD FREQUENCY PLOT AS PNG ----------
    plt.savefig(f"{output_dir}/word_frequency.png", format='png')
    plt.close()

    # ---------- PREPARE DATA FOR HTML PAYLOAD ----------
    # Extracting high-level KPIs for the dashboard header
    total_rows = len(df)
    unique_users = int(df['reviewer_id'].nunique())
    unique_listings = int(df['listing_id'].nunique())
    avg_comment_len = float(df['comments_len'].mean())

    # ---------- TIMELINE DATA ----------
    yearly_counts = df.groupby('year').size().sort_index()
    yearly_data = {
        "labels": [str(y) for y in yearly_counts.index],
        "data": yearly_counts.values.tolist()
    }

    # ---------- TOP REVIEWERS DATA ----------
    top_reviewers = df['reviewer_id'].value_counts().head(10)
    reviewers_data = {
        "labels": [str(i) for i in top_reviewers.index],
        "data": top_reviewers.values.tolist()
    }

    # ---------- LENGTH DISTRIBUTION DATA ----------
    # Histogram calculation for comment length analysis
    counts, bins = np.histogram(df['comments_len'], bins=10, range=(0, int(df['comments_len'].quantile(0.95))))
    length_data = {
        "labels": [f"{int(bins[i])}-{int(bins[i+1])}" for i in range(len(bins)-1)],
        "data": counts.tolist()
    }

    # ---------- WORD FREQUENCY DATA ----------
    word_data = {
        "labels": df_words['Word'].tolist(),
        "data": df_words['Count'].tolist()
    }

    # ---------- BUILD JSON PAYLOAD FOR TEMPLATE ----------
    payload_json = {
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
    }

    # ---------- JINJA2 TEMPLATE RENDERING ----------
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            template_content = f.read()

        # Injecting the data payload into the HTML structure
        template = Template(template_content)
        html_final = template.render(payload=payload_json)

        # ---------- SAVE FINAL DASHBOARD ----------
        out_path = os.path.join(output_dir, "eda_dashboard_reviews.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_final)
        
        log.info(f"EDA Dashboard generated successfully at: {out_path}")
    else:
        log.warning(f"Template not found at {template_path}. Skipping dashboard generation.")
    
    return file_path