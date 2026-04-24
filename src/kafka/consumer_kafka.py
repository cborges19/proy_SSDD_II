from confluent_kafka import DeserializingConsumer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


def consumer_from_kafka_avro(topic):
    # Configuración del Schema Registry (Localhost porque el script corre fuera de Docker)
    sr_conf = {'url': 'http://localhost:8081'}
    sr_client = SchemaRegistryClient(sr_conf)

    avro_deserializer = AvroDeserializer(sr_client)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': string_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': topic,
        'auto.offset.reset': 'earliest'      
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['airbnb_listings_gold'])

    print("Consumidor iniciado. Esperando datos de Airbnb...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # El mensaje ya viene deserializado como un dict de Python
            data = msg.value()
            print(f"Recibido listing: {data.get('id', 'Sin nombre')} - Precio: {data.get('price', 'N/A')}")
            
    except KeyboardInterrupt:
        print("Cerrando consumidor...")
    finally:
        print(data.keys())
        consumer.close()
