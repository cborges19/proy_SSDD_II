import pathlib
import logging
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# --- CONFIGURACIÓN DE RUTAS ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
LOG_DIR = PROJECT_ROOT / "logs" / "consumer_logs"

logger = logging.getLogger("AirbnbMonitor")
logger.setLevel(logging.INFO)

def consumer_kafka_avro(topic, idle_timeout_seconds=30, log_to_file=True):
    # --- CONFIGURACIÓN DE LOGS ---
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    # Limpiamos manejadores previos para evitar duplicados si se llama varias veces
    if logger.hasHandlers():
        logger.handlers.clear()

    # Siempre añadimos la consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Añadimos archivo solo si se solicita
    if log_to_file:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        specific_log_file = LOG_DIR / f"consumer_{topic}.log"
        file_handler = logging.FileHandler(specific_log_file, mode='a') 
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Escritura de logs activada en: {specific_log_file}")
    else:
        logger.info("Escritura de logs en archivo desactivada.")

    logger.info(f"--- INICIANDO MONITOR PARA: {topic} ---")
    
    # --- CONFIGURACIÓN DE KAFKA ---
    sr_conf = {'url': 'http://localhost:8081'}
    try:
        sr_client = SchemaRegistryClient(sr_conf)
        avro_deserializer = AvroDeserializer(sr_client)
        string_deserializer = StringDeserializer('utf_8')
    except Exception as e:
        logger.error(f"Fallo conexión Schema Registry: {e}")
        return

    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': string_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': f"{topic}_consumer_group", 
        'auto.offset.reset': 'earliest'      
    }

    try:
        consumer = DeserializingConsumer(consumer_conf)
        consumer.subscribe([topic])
    except Exception as e:
        logger.error(f"Error al crear el consumer: {e}")
        return

    # --- LÓGICA DE VISUALIZACIÓN ---
    # Determinamos intervalo según el contenido del nombre del topic
    if "calendar" in topic:
        LOG_INTERVAL = 10000
    elif "reviews" in topic:
        LOG_INTERVAL = 5000
    else:
        LOG_INTERVAL = 500

    total_leidos = 0
    num_columnas = 0
    last_msg_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                elapsed = time.time() - last_msg_time
                if elapsed > idle_timeout_seconds:
                    logger.info(f"Timeout alcanzado: {idle_timeout_seconds}s sin mensajes. Cerrando...")
                    break
                continue
            
            if msg.error():
                logger.warning(f"Kafka Error: {msg.error()}")
                continue

            last_msg_time = time.time() 
            dato = msg.value()
            total_leidos += 1
            
            if total_leidos == 1 and dato:
                num_columnas = len(dato.keys())
                logger.info(f"Esquema detectado: {topic} tiene {num_columnas} columnas.")

            if total_leidos % LOG_INTERVAL == 0:
                logger.info(f"Topic {topic}: {total_leidos} mensajes procesados.")

    except KeyboardInterrupt:
        logger.warning(f"Monitor de {topic} detenido manualmente.")
    except Exception as e:
        logger.error(f"Error en consumer {topic}: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        
        logger.info("="*60)
        logger.info(f"RESUMEN FINAL - TÓPICO: {topic}")
        logger.info(f"- Total mensajes: {total_leidos}")
        logger.info(f"- Columnas: {num_columnas}")
        logger.info("="*60)

if __name__ == "__main__":
    consumer_kafka_avro(
        topic='airbnb_listings_gold', 
        idle_timeout_seconds=10, 
        log_to_file=False
    )