import pathlib
import tomllib
import logging
import json
import ujson
import pandas as pd
import numpy as np

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime, timedelta


logger = logging.getLogger("airflow.task")


PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

with open(PROJECT_ROOT / "config.toml", "rb") as f:
    config = tomllib.load(f)
# ... usa config["kafka"]["bootstrap_servers"] en tus funciones


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
    
    logger.info(f"Iniciando producción Avro a Kafka. Topic: {topic}")
    logger.info(f"Leyendo archivo: {file_path}")
    
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"Parquet cargado exitosamente. Filas detectadas: {len(df)}")

        # --- DATA NORMALIZATION ---
        logger.debug("Normalizando tipos de datos para compatibilidad con Avro...")
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].astype('int64') // 10**6
            
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, np.ndarray):
                df[col] = df[col].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)

        # --- SCHEMA REGISTRY & PRODUCER SETUP ---
        avro_schema_str = generate_avro_schema(df, topic)
        logger.debug(f"Esquema Avro generado para el topic {topic}")
        
        sr_client = SchemaRegistryClient({'url': schema_registry_url})
        avro_serializer = AvroSerializer(sr_client, avro_schema_str)

        producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'value.serializer': avro_serializer,
            'linger.ms': 10,
            'compression.type': 'snappy'
        }
        producer = SerializingProducer(producer_conf)

        # --- STREAMING RECORDS ---
        records = df.replace({np.nan: None}).to_dict('records')
        logger.info(f"Enviando {len(records)} registros a Kafka...")
        
        for i, row in enumerate(records):
            producer.produce(topic=topic, value=row)
            producer.poll(0)
            if (i + 1) % 1000 == 0:
                logger.info(f"Progreso: {i + 1} registros enviados...")
        
        producer.flush()
        logger.info(f"Proceso finalizado. Registros confirmados en el topic: {topic}")

    except Exception as e:
        logger.error(f"Error en produce_to_kafka_avro: {str(e)}")
        raise  

def produce_error_to_kafka(variable, message, bootstrap_servers=config["kafka"]["bootstrap_servers"]):
    """Sends a JSON error payload to the pipeline_errors topic (DLQ)."""
    
    logger.warning(f"Reportando error en validación para la variable: {variable}")
    
    try:
        producer = Producer({'bootstrap.servers': bootstrap_servers})
        payload = {
            "task": "validate", "variable": variable,
            "message": message, "timestamp": datetime.now().isoformat()
        }
        producer.produce(topic="pipeline_errors", value=json.dumps(payload).encode('utf-8'))
        producer.flush()
        logger.info("Error enviado correctamente a la DLQ (pipeline_errors).")
    except Exception as e:
        logger.error(f"Falló el envío a la DLQ: {str(e)}")
