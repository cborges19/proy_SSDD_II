import pathlib
import logging
import os
import time
import sys
import pandas as pd
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# --- 1. CONFIGURACIÓN DE RUTAS ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
# Nueva ruta organizada: logs/consumer_logs
LOG_DIR = PROJECT_ROOT / "logs" / "consumer_logs"
DATA_DIR = PROJECT_ROOT / "data" / "gold"

LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("AirbnbConsumer")
logger.setLevel(logging.INFO)

def consumer_from_kafka_avro(topic):
    # Carpeta específica para los datos del tópico (Estructura para Spark)
    TOPIC_DATA_DIR = DATA_DIR / topic
    TOPIC_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # --- 2. CONFIGURACIÓN DE LOGS DINÁMICOS (OPCIÓN A) ---
    specific_log_file = LOG_DIR / f"consumer_{topic}.log"
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    file_handler = logging.FileHandler(specific_log_file)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Iniciando consumer para: {topic}")
    
    # --- 3. CONFIGURACIÓN DE KAFKA ---
    sr_conf = {'url': 'http://localhost:8081'}
    try:
        sr_client = SchemaRegistryClient(sr_conf)
        avro_deserializer = AvroDeserializer(sr_client)
        string_deserializer = StringDeserializer('utf_8')
        logger.info("Conexión con Schema Registry establecida.")
    except Exception as e:
        logger.error(f"Fallo Schema Registry: {e}")
        return

    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': string_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': f"{topic}_spark_group_final",
        'auto.offset.reset': 'earliest'      
    }

    try:
        consumer = DeserializingConsumer(consumer_conf)
        consumer.subscribe([topic])
    except Exception as e:
        logger.error(f"Error al crear consumer: {e}")
        return

    # --- LÓGICA DE BUFFER ---
    if "calendar" in topic:
        MAX_BUFFER_SIZE = 50000  
        LOG_INTERVAL = 10000
    elif "reviews" in topic:
        MAX_BUFFER_SIZE = 15000  
        LOG_INTERVAL = 5000
    # Listings
    else:
        MAX_BUFFER_SIZE = 500   
        LOG_INTERVAL = 250

    buffer_datos = []
    last_msg_time = time.time()
    total_procesados = 0

    try:
        while True:
            # Poll con timeout de 1 segundo
            msg = consumer.poll(1.0)
            
            if msg is None:
                # TIMEOUT: Si hay datos y llevamos 5s sin recibir nada, guardamos lo que haya
                if buffer_datos and (time.time() - last_msg_time) > 5:
                    logger.info(f"Timeout: Guardando buffer parcial de {topic} ({len(buffer_datos)} filas)")
                    persist_to_parquet(buffer_datos, topic, TOPIC_DATA_DIR)
                    buffer_datos = []
                continue
            
            if msg.error():
                logger.warning(f"Kafka Error: {msg.error()}")
                continue

            # Procesamiento de mensaje
            data = msg.value()
            buffer_datos.append(data)
            last_msg_time = time.time()
            total_procesados += 1
            
            if len(buffer_datos) % LOG_INTERVAL == 0:
                logger.info(f"{topic}: {len(buffer_datos)} acumulados en buffer (Total: {total_procesados})")

            # Persistir cuando el buffer se llena
            if len(buffer_datos) >= MAX_BUFFER_SIZE:
                persist_to_parquet(buffer_datos, topic, TOPIC_DATA_DIR)
                buffer_datos = []
            
    except KeyboardInterrupt:
        logger.warning(f"Cierre manual detectado en {topic}.")
        if buffer_datos:
            persist_to_parquet(buffer_datos, topic, TOPIC_DATA_DIR)
    except Exception as e:
        logger.error(f"Error inesperado en {topic}: {e}", exc_info=True)
    finally:
        consumer.close()
        logger.info(f"Consumer de {topic} finalizado y cerrado.")

def persist_to_parquet(data_list, topic, directory):
    """Guarda el buffer de datos en un fragmento Parquet único."""
    timestamp = int(time.time() * 1000) # Milisegundos para evitar colisiones
    file_name = f"part_{topic}_{timestamp}.parquet"
    full_path = directory / file_name
    
    try:
        df = pd.DataFrame(data_list)
        # Usamos pyarrow para máxima compatibilidad con Spark
        df.to_parquet(full_path, index=False, engine='pyarrow')
        logger.info(f"ARCHIVO GENERADO: {file_name} con {len(data_list)} filas.")
    except Exception as e:
        logger.error(f"Error al persistir Parquet: {e}")

if __name__ == "__main__":

    consumer_from_kafka_avro('airbnb_listings_gold')

    consumer_from_kafka_avro('airbnb_calendar_gold')

    consumer_from_kafka_avro('airbnb_reviews_gold')