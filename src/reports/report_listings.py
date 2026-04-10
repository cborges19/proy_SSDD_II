import pandas as pd
import numpy as np
import os
import json
from jinja2 import Template
import matplotlib.pyplot as plt

import pathlib

def eda_listings(file_path, output_dir, log, template_path):
    """
    Exploratory Data Analysis (EDA): Generates an interactive HTML dashboard 
    with enriched features from the Malaga dataset.
    """
   
    df = pd.read_parquet(file_path)
    os.makedirs(output_dir, exist_ok=True)

    # ---------- GENERATING PRICE DISTRIBUTION PLOT ----------
    log.info("Generating price distribution plot")
    plt.figure(figsize=(10, 6))
    plt.hist(df['price'].clip(upper=500), bins=40, color='skyblue', edgecolor='black')
    plt.title('Price Distribution (Clipped at 500€)')
    plt.xlabel('Price (€)')
    plt.ylabel('Frequency')
    plt.savefig(f"{output_dir}/price_distribution.png", format='png')
    plt.close()

    # ---------- GENERATING DISTANCE VS PRICE TREND ----------
    log.info("Generating distance vs price trend plot")
    df['dist_bin'] = pd.cut(df['dist_center_km'], bins=10)
    dist_price_trend = df.groupby('dist_bin', observed=True)['price'].median().fillna(0)
    
    plt.figure(figsize=(12, 6))
    dist_price_trend.plot(kind='bar', color='salmon', edgecolor='black')
    plt.title('Median Price by Distance to Center')
    plt.xlabel('Distance Range (km)')
    plt.ylabel('Median Price (€)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/distance_price_trend.png", format='png')
    plt.close()

    # ---------- GENERATING HOST SEGMENTATION PLOT ----------
    log.info("Generating host segmentation plot")
    plt.figure(figsize=(8, 8))
    df['host_segment'].value_counts().plot(kind='pie', autopct='%1.1f%%', colors=['#ff9999','#66b3ff','#99ff99'])
    plt.title('Host Segmentation Share')
    plt.ylabel('') # Remove y-label for pie chart
    plt.savefig(f"{output_dir}/host_segmentation_pie.png", format='png')
    plt.close()

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
    price_series = df['price'].dropna()
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
    dist_price_trend = df.groupby('dist_bin', observed=True)['price'].median().fillna(0).tolist()
    dist_bins_labels = [f"{round(b.left,1)}-{round(b.right,1)}km" for b in df['dist_bin'].cat.categories]

    # ---------- NEIGHBOURHOOD PRICE ANALYSIS ----------
    # Grouping price by neighbourhood, and calculate median
    neigh_stats = df.groupby('neighbourhood_cleansed')['price'].median().sort_values(ascending=False).head(15)
    
    neigh_data = {
        "neighbourhood_cleansed": neigh_stats.index.tolist(),
        "median_price": neigh_stats.values.tolist()
    }

    # ---------- EXISTING DISTRIBUTIONS ----------
    room_counts = df['room_type'].value_counts().to_dict()
    seg_counts  = df['host_segment'].value_counts().to_dict()
        
    # Correlation Matrix (Updated with Enrichment columns)
    corr_cols = ['price', 'dist_center_km', 'amenities_count', 'trust_score', 'host_tenure_days']
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
        "corr": {"labels": corr_cols, "matrix": corr_matrix},
        "neigh_price": neigh_data
    }, default=to_json)

    # ---------- JINJA2 RENDERING ----------
    out_path = os.path.join(output_dir, "eda_dashboard.html")

    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            template = Template(f.read())
        html_final = template.render(payload=payload_json)
            
        out_path = os.path.join(output_dir, "eda_dashboard.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_final)
        print(f"Enriched EDA dashboard generated at: {out_path}")
    else:
        print("Warning: report_template.html not found. Skipping HTML generation.")

    return file_path