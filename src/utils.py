import pandas as pd
import numpy as np
import re
import json
import ujson
from datetime import datetime, timedelta
import holidays

from src.kafka.producer_kafka import produce_error_to_kafka


import pathlib
import tomllib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent

with open(PROJECT_ROOT / "config.toml", "rb") as f:
    config = tomllib.load(f)
# ... usa config["kafka"]["bootstrap_servers"] en tus funciones

DATA_DIR = PROJECT_ROOT / config["paths"]["data_dir"]
OUTPUT_DIR = PROJECT_ROOT / config["paths"]["output_dir"]
TEMPLATES_DIR = PROJECT_ROOT / config["paths"]["templates_dir"]


# ==========================================
# 1. EXTRACTION & BASIC CLEANING
# ==========================================

def extract_data(file_path, record_name):
    """Reads a CSV file and converts it to Parquet for speed."""
    df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    output_path = f"/tmp/{record_name}_processed.parquet"
    df.to_parquet(output_path, engine='pyarrow', index=False)
    return output_path

def clean_currency(series):
    """Removes currency symbols and returns float."""
    return series.astype(str).replace(r'[\$,%€]', '', regex=True).replace(r',', '', regex=True).astype(float)

def map_airbnb_bool(df, columns):
    """Maps Airbnb 't'/'f' to Python Booleans."""
    mapping = {'t': True, 'f': False, True: True, False: False, 1: True, 0: False}
    for col in columns:
        if col in df.columns: df[col] = df[col].map(mapping)
    return df

def clean_text_nlp(text):
    """Basic NLP cleaning: lowercase, remove HTML, and extra spaces."""
    if pd.isna(text): return ""
    text = str(text).lower()
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'[^\w\s]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()

def translate_values(df, col, mapping):
    """Translates specific values (e.g., property types) using a dict."""
    if col in df.columns: df[col] = df[col].replace(mapping)
    return df

# ==========================================
# 2. IMPUTATION & NUMERICS
# ==========================================

def manual_impute_by_id(df, id_col, id_list, target_col, value):
    """Assigns a specific value to a target column for a list of IDs."""
    df.loc[df[id_col].isin(id_list), target_col] = value
    return df

def parse_numeric_from_text(series):
    """Extracts numeric values from strings like '1.5 baths' or 'half'."""
    def extract_logic(text):
        if pd.isna(text): return np.nan
        text = text.lower().strip()
        if 'half' in text or 'medio' in text: return 0.5
        match = re.search(r'[\d\.]+', text)
        return float(match.group()) if match else np.nan
    return series.apply(extract_logic)

def impute_median(df, target_col, group_col):
    """Standard median imputation grouped by another column."""
    df[target_col] = df[target_col].fillna(df.groupby(group_col)[target_col].transform('median'))
    return df

def impute_mode(df, target_col, group_col):
    """Standard mode imputation grouped by another column."""
    df[target_col] = df[target_col].fillna(
        df.groupby(group_col)[target_col].transform(lambda x: x.mode()[0] if not x.mode().empty else np.nan)
    )
    return df

def impute_median_with_noise(df, target_col, group_col):
    """Fills nulls using grouped median + 10% Gaussian noise."""
    def add_noise(series):
        if series.isnull().all(): return series
        median, std = series.median(), series.std()
        scale = (std * 0.1) if not (pd.isna(std) or std == 0) else 0.05
        null_mask = series.isna()
        if null_mask.any():
            noise = np.random.normal(loc=0, scale=scale, size=null_mask.sum())
            series.loc[null_mask] = median + noise
        return series
    df[target_col] = df.groupby(group_col)[target_col].transform(add_noise)
    return df

def apply_clip(df, target_col, upper_limit=None, lower_limit=None):
    """Clips values in a column to a specific range."""
    df[target_col] = df[target_col].clip(lower=lower_limit, upper=upper_limit)
    return df

def apply_log1p_transformation(df, columns):
    """Applies log(1+x) to handle skewed distributions."""
    for col in columns:
        if col in df.columns: df[f'{col}_log'] = np.log1p(df[col])
    return df

def normalize_rates(df, columns):
    for col in columns:
        df[col] = df[col] / 100.0
    return df

# ==========================================
# 3. FEATURE ENGINEERING
# ==========================================

def binarize_by_keywords(series, regex):
    """Returns 1 if keywords found, else 0."""
    return series.astype(str).str.contains(regex, case=False, regex=True, na=False).astype(int)

def parse_amenities(amenities_str):
    """Parses Airbnb amenities JSON string into a list."""
    if pd.isna(amenities_str): return []
    try: return ujson.loads(amenities_str.replace("'", '"'))
    except: return []

def get_ohe_from_list_column(df, col, prefix):
    """Expands list-like strings into One-Hot Encoded columns, avoiding empty categories."""
    # Clean characters and strip potential leading/trailing spaces
    clean_series = df[col].astype(str).str.replace(r"[\[\]\']", "", regex=True).str.strip()
    
    # Generate dummies
    dummies = clean_series.str.get_dummies(sep=', ').astype(bool)
    
    # Remove the empty string column if it was generated from empty lists
    if "" in dummies.columns:
        dummies = dummies.drop(columns=[""])
        
    return pd.concat([df, dummies.add_prefix(f"{prefix}_")], axis=1)

def calculate_days_since(df, date_col, reference_date=None):
    """Converts a date column to days elapsed."""
    ref = pd.to_datetime(reference_date) if reference_date else datetime.now()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[f'days_since_{date_col}'] = (ref - df[date_col]).dt.days.fillna(0)
    return df

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculates the distance in km between two points on Earth."""
    R = 6371.0  # Earth radius in km
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def get_event(date):
    """
    Categorizes a given date into specific seasonal events or holidays, 
    including Easter (Semana Santa), Málaga's Fair, and summer periods.

    Args:
        date (datetime or pd.Timestamp): The date to be categorized.

    Returns:
        str: The name of the detected event (e.g., 'Semana Santa', 'Feria de Málaga', 
            'Verano') or 'Temporada normal' if no special event is matched.
    """
    year = date.year
    holiday_es = holidays.Spain(years=year, subdiv='AN')
    easter = [d for d, name in holiday_es.items() if 'Viernes Santo' in name]
    
    if easter:
        viernes_santo = pd.Timestamp(easter[0])
        ramos = viernes_santo - pd.Timedelta(days=7)
        if ramos <= date <= viernes_santo + pd.Timedelta(days=1):
            return 'Semana Santa'
        
    if pd.Timestamp(f'{year}-08-15') <= date <= pd.Timestamp(f'{year}-08-22'): 
        return 'Feria de Málaga'
    
    if date.month == 6 and date.day == 23:
        return 'Noche de San Juan'
    
    if date.month in [6, 7, 8]:
        return 'Verano'
    
    if (date.month == 12 and date.day >= 20) or (date.month == 1 and date.day <= 6):
        return 'Navidad/Reyes'
    
    return 'Temporada normal'

# ==========================================
# 4. VALIDATION ENGINE
# ==========================================

def validate_and_report(df, rules):
    """Engine for validating rules and reporting failures to Kafka DLQ."""
    for rule in rules:
        col, check, msg = rule.get('column'), rule.get('check'), rule.get('message')
        failed = False
        if check == 'range':
            min_v, max_v = rule.get('params')
            mask = pd.Series(False, index=df.index)
            if min_v is not None: mask |= (df[col] <= min_v)
            if max_v is not None: mask |= (df[col] >= max_v)
            failed = mask.any()
        elif check == 'unique': failed = df[col].duplicated().any()
        elif check == 'category': failed = (~df[col].isin(rule.get('params'))).any()
        elif check == 'logic': failed = (~df.apply(rule.get('params'), axis=1)).any()
        elif check == 'date_limit':
            limit = pd.to_datetime(rule.get('params'))
            failed = (pd.to_datetime(df[col]) > limit).any()
        
        if failed: produce_error_to_kafka(col or "logic_check", msg)
    return df