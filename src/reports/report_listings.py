import pandas as pd
import numpy as np
import os
import json
from jinja2 import Template

import pathlib

def eda_listings(file_path, output_dir, log, template_path):
    """
    Exploratory Data Analysis (EDA): Generates an interactive HTML dashboard 
    with enriched features from the Malaga dataset.
    """
    # --------- LOAD DATA ---------
    df = pd.read_parquet(file_path)
    os.makedirs(output_dir, exist_ok=True)

    # --------- SERIALIZATION HELPERS ---------
    def to_json(obj):
        # Custom encoder to handle NumPy types for JSON compatibility
        if isinstance(obj, (np.integer,)):  return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, (np.ndarray,)):  return obj.tolist()
        if isinstance(obj, pd.Timestamp):   return str(obj)
        return obj

    # --------- GENERAL DATASET STATISTICS ---------
    total_rows = len(df)
    total_cols = len(df.columns)
    null_pct = (df.isnull().mean() * 100).round(2).to_dict()

    # --------- PRICE ANALYSIS (Standard & Log) ---------
    # Histogram for standard price clipped at €500
    price_series = df['price_num'].dropna()
    price_hist = np.histogram(price_series.clip(upper=500), bins=40)
    
    # Histogram for log-transformed price
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

    # --------- ENRICHMENT SEGMENTATION (KPIs) ---------
    # Calculate proximity and location metrics for the dashboard
    enrichment_data = {
        "host_near": {
            "Local (Málaga)": int(df['host_near'].sum()) if 'host_near' in df.columns else 0,
            "Remote/Other": int(len(df) - df['host_near'].sum()) if 'host_near' in df.columns else 0
        },
        "location_cats": df['host_location_cat'].value_counts().to_dict() if 'host_location_cat' in df.columns else {},
        "urban_core": {
            "Center (<=2km)": int(df['neighbourhood_center'].sum()) if 'neighbourhood_center' in df.columns else 0,
            "Periphery": int(len(df) - df['neighbourhood_center'].sum()) if 'neighbourhood_center' in df.columns else 0
        },
        "long_stay_pct": round(float(df['long_stay'].mean() * 100), 2) if 'long_stay' in df.columns else 0
    }

    # --------- GEOSPATIAL: DISTANCE VS PRICE ---------
    # Aggregate median price by distance deciles to identify trends
    dist_price_trend = []
    dist_bins_labels = []
    if 'dist_center_km' in df.columns and 'price_num' in df.columns:
        df['dist_bin'] = pd.cut(df['dist_center_km'], bins=10)
        dist_price_trend = df.groupby('dist_bin', observed=True)['price_num'].median().fillna(0).tolist()
        dist_bins_labels = [f"{round(b.left,1)}-{round(b.right,1)}km" for b in df['dist_bin'].cat.categories]

    # --------- DISTRIBUTIONS & CORRELATIONS ---------
    room_counts = df['room_type'].value_counts().to_dict() if 'room_type' in df.columns else {}
    seg_counts  = df['host_segment'].value_counts().to_dict() if 'host_segment' in df.columns else {}
    
    # Correlation Matrix limited to strategic numerical columns
    corr_cols = ['price_num', 'dist_center_km', 'amenities_count', 'trust_score', 'host_tenure_days']
    corr_cols = [c for c in corr_cols if c in df.columns]
    corr_matrix = df[corr_cols].corr().round(3).values.tolist()

    # --------- BUILD ENRICHED JSON PAYLOAD ---------
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

    # --------- JINJA2 RENDERING ---------    
    if os.path.exists(template_path):
        with open(template_path, "r", encoding="utf-8") as f:
            template = Template(f.read())
        
        html_final = template.render(payload=payload_json)
        out_path = os.path.join(output_dir, "eda_dashboard.html")
        
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_final)
        log.info(f"Enriched EDA dashboard generated successfully at: {out_path}")
    else:
        log.warning(f"Template not found at {template_path}. Skipping HTML generation.")

    return file_path