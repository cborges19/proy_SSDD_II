"""
Kafka Request-Reply Client for Streamlit
=========================================
Provides lightweight producer / consumer utilities to support the
request-reply pattern between Streamlit and Spark Streaming:

  Streamlit  ──produce──▶  topic_peticiones / topic_likes
  Spark      ──produce──▶  topic_precios    / topic_sugerencias
  Streamlit  ──consume──▶  (waits for matching request_id)

Each interaction is identified by a unique UUID so that multiple
simultaneous Streamlit sessions do not mix up their responses.
"""

import json
import time
import uuid
import logging

from confluent_kafka import Producer, Consumer, KafkaError

logger = logging.getLogger(__name__)

# ----- KAFKA TOPIC NAMES -----
TOPIC_PETICIONES  = "topic_peticiones"   # Streamlit → Spark (price requests)
TOPIC_PRECIOS     = "topic_precios"      # Spark → Streamlit (price results)
TOPIC_LIKES       = "topic_likes"        # Streamlit → Spark (like events)
TOPIC_SUGERENCIAS = "topic_sugerencias"  # Spark → Streamlit (KNN results)

# ----- DEFAULT POLL TIMEOUT  -----
DEFAULT_TIMEOUT = 45


def _build_producer(bootstrap_servers: str) -> Producer:
    """Creates and returns a configured confluent_kafka Producer."""
    return Producer({"bootstrap.servers": bootstrap_servers})


def _build_consumer(bootstrap_servers: str, request_id: str) -> Consumer:
    """
    Creates a Consumer with a unique group.id derived from the request_id so
    that every request reads independently from the latest offset on the
    response topic, avoiding stale message contamination.
    """
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"streamlit_{request_id}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def produce_request(topic: str, payload: dict, bootstrap_servers: str) -> str:
    """
    Serialises *payload* to JSON, injects a fresh UUID as 'request_id',
    produces the message to *topic*, and returns the request_id so the
    caller can match the eventual response.

    Parameters
    ----------
    topic : str
        Destination Kafka topic.
    payload : dict
        Data to send.  Must be JSON-serialisable.
    bootstrap_servers : str
        Kafka bootstrap string (e.g. 'localhost:9092').

    Returns
    -------
    str
        The UUID request_id injected into the payload.
    """
    request_id = str(uuid.uuid4())
    payload["request_id"] = request_id

    producer = _build_producer(bootstrap_servers)
    producer.produce(
        topic=topic,
        value=json.dumps(payload, default=str).encode("utf-8"),
    )
    producer.flush()
    logger.info("Produced request %s to topic '%s'", request_id, topic)
    return request_id


def consume_response(
    topic: str,
    request_id: str,
    bootstrap_servers: str,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict | None:
    """
    Polls *topic* until a message whose 'request_id' matches is received
    or *timeout* seconds elapse.

    Parameters
    ----------
    topic : str
        Response Kafka topic to poll.
    request_id : str
        UUID to match against incoming messages.
    bootstrap_servers : str
        Kafka bootstrap string.
    timeout : int
        Maximum seconds to wait before returning None.

    Returns
    -------
    dict | None
        Decoded response payload, or None on timeout.
    """
    consumer = _build_consumer(bootstrap_servers, request_id)
    consumer.subscribe([topic])

    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning("Kafka consumer error: %s", msg.error())
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError:
                logger.warning("Received non-JSON message — skipping.")
                continue

            if data.get("request_id") == request_id:
                logger.info("Matched response for request %s", request_id)
                return data

    finally:
        consumer.close()

    logger.warning("Timeout waiting for request_id %s on topic '%s'", request_id, topic)
    return None


def request_price_prediction(features: dict, bootstrap_servers: str) -> dict | None:
    """
    High-level helper: sends a price-prediction request and waits for the
    Spark ML processor to publish the result.

    Parameters
    ----------
    features : dict
        Listing features to send as prediction input.
    bootstrap_servers : str
        Kafka bootstrap string.

    Returns
    -------
    dict | None
        {'predicted_price': float, 'request_id': str} or None on timeout.
    """
    request_id = produce_request(TOPIC_PETICIONES, features, bootstrap_servers)
    return consume_response(TOPIC_PRECIOS, request_id, bootstrap_servers)


def request_recommendations(listing_id: int, bootstrap_servers: str) -> dict | None:
    """
    High-level helper: sends a 'like' event and waits for the KNN
    recommendations from the Spark ML processor.

    Parameters
    ----------
    listing_id : int
        ID of the listing the user liked.
    bootstrap_servers : str
        Kafka bootstrap string.

    Returns
    -------
    dict | None
        {'recommendations': [...], 'request_id': str} or None on timeout.
    """
    payload = {"listing_id": int(listing_id)}
    request_id = produce_request(TOPIC_LIKES, payload, bootstrap_servers)
    return consume_response(TOPIC_SUGERENCIAS, request_id, bootstrap_servers)
