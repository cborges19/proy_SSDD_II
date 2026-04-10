import pandas as pd
import numpy as np
import re
import json
import ujson
from datetime import datetime, timedelta
import holidays
from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

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
# 1. KAFKA, AVRO & ERROR REPORTING
# ==========================================

def generate_avro_schema(df, record_name):
    """Generates an Avro schema string from a Pandas DataFrame with support for Arrays and Logical Types."""
    fields = []
    for col, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        
        # --- COMPLEX TYPE DETECTION (Arrays) ---
        if dtype_str == 'object':
            # Check the first non-null value to determine if it's a list/array
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, (list, np.ndarray)):
                avro_type = {
                    "type": "array",
                    "items": "string"
                }
            else:
                avro_type = "string"
        
        # --- LOGICAL TYPES (Timestamps) ---
        elif 'datetime64' in dtype_str:
            avro_type = {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
            
        # --- STANDARD PRIMITIVE TYPES ---
        elif 'int64' in dtype_str:
            avro_type = "long"
        elif 'int32' in dtype_str:
            avro_type = "int"
        elif 'float' in dtype_str: # Catches float64 and float32
            avro_type = "double"
        elif 'bool' in dtype_str:
            avro_type = "boolean"
        else:
            avro_type = "string"

        # All fields are nullable by default for schema evolution robustness
        fields.append({"name": col, "type": ["null", avro_type], "default": None})
    
    schema_dict = {
        "type": "record",
        "name": record_name,
        "namespace": "com.airbnb.data",
        "fields": fields
    }
    return json.dumps(schema_dict)

def produce_to_kafka_avro(file_path, topic, 
                          schema_registry_url=config["kafka"]["schema_registry_url"], 
                          bootstrap_servers=config["kafka"]["bootstrap_servers"]):
    """Produces Parquet data to Kafka after normalizing types for Avro serialization."""
    df = pd.read_parquet(file_path)

    # --- DATA NORMALIZATION FOR AVRO COMPATIBILITY ---
    for col in df.columns:
        # Convert Timestamps to milliseconds (Avro long + logicalType)
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype('int64') // 10**6
        
        # Convert NumPy ndarrays to native Python lists
        # Avro Serializer does not support numpy types natively
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        if isinstance(sample, np.ndarray):
            df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

    # --- SCHEMA REGISTRY & PRODUCER SETUP ---
    avro_schema_str = generate_avro_schema(df, topic)
    sr_client = SchemaRegistryClient({'url': schema_registry_url})
    avro_serializer = AvroSerializer(sr_client, avro_schema_str)

    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'value.serializer': avro_serializer,
        'linger.ms': 10,
        'compression.type': 'snappy'
    }
    producer = SerializingProducer(producer_conf)

    # --- STREAMING RECORDS WITH NULL HANDLING ---
    # We use .replace({np.nan: None}) to ensure Pandas NaNs become Python None
    # which matches Avro's 'null' type perfectly.
    records = df.replace({np.nan: None}).to_dict('records')
    for row in records:
        # Values are already cleaned and match the generated schema
        producer.produce(topic=topic, value=row)
        producer.poll(0)
    
    producer.flush()

def produce_error_to_kafka(variable, message, bootstrap_servers=config["kafka"]["bootstrap_servers"]):
    """Sends a JSON error payload to the pipeline_errors topic (DLQ)."""
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    payload = {
        "task": "validate", "variable": variable,
        "message": message, "timestamp": datetime.now().isoformat()
    }
    producer.produce(topic="pipeline_errors", value=json.dumps(payload).encode('utf-8'))
    producer.flush()

# ==========================================
# 2. EXTRACTION & BASIC CLEANING
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
# 3. IMPUTATION & NUMERICS
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

# ==========================================
# 4. FEATURE ENGINEERING
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
# 5. VALIDATION ENGINE
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