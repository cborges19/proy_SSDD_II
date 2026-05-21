"""
Spark Streaming ML Processor
==============================
Runs two concurrent Spark Structured Streaming jobs:

  Job 1 · Price Prediction
    topic_peticiones → RF sklearn pipeline → topic_precios

  Job 2 · KNN Recommendations
    topic_likes → NearestNeighbors → topic_sugerencias

Both jobs use ``foreachBatch`` to apply scikit-learn models loaded once
per executor via broadcast variables, maximising throughput and avoiding
repeated I/O on every micro-batch.

Usage
-----
    python src/queries/ml_processor.py

Prerequisites
-------------
  - Kafka containers running   (docker compose up -d)
  - Gold parquet present        (run Airflow DAG first)
  - Models trained              (python src/models/train_models.py)
"""

import json
import logging
import pathlib
import sys
from typing import Any

import joblib
import numpy as np
import pandas as pd
from confluent_kafka import Producer
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# ----- PATH SETUP -----
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("MLProcessor")

# ----- KAFKA SETTINGS -----
KAFKA_BOOTSTRAP    = "localhost:9092"
TOPIC_PETICIONES   = "topic_peticiones"
TOPIC_PRECIOS      = "topic_precios"
TOPIC_LIKES        = "topic_likes"
TOPIC_SUGERENCIAS  = "topic_sugerencias"

# ----- MODEL PATHS -----
MODELS_DIR   = PROJECT_ROOT / "models"
PRICE_MODEL  = MODELS_DIR / "price_model.pkl"
KNN_MODEL    = MODELS_DIR / "knn_model.pkl"

# ----- PRICE PREDICTION FEATURES (must match train_models.py) -----
PRICE_FEATURES_NUM  = ["accommodates", "bathrooms", "bedrooms", "beds", "num_amenities", "dist_center_km"]
PRICE_FEATURES_CAT  = ["room_type", "neighbourhood_cleansed"]
PRICE_FEATURES_BOOL = ["host_is_superhost", "instant_bookable"]
ALL_PRICE_FEATURES  = PRICE_FEATURES_NUM + PRICE_FEATURES_CAT + PRICE_FEATURES_BOOL


# ===========================================================================
# MODEL LOADING
# ===========================================================================

def _load_price_model() -> Any:
    """Loads the sklearn price prediction Pipeline from disk."""
    if not PRICE_MODEL.exists():
        raise FileNotFoundError(
            f"Price model not found at {PRICE_MODEL}.  "
            "Run: python src/models/train_models.py"
        )
    model = joblib.load(PRICE_MODEL)
    logger.info("Price model loaded from: %s", PRICE_MODEL)
    return model


def _load_knn_bundle() -> dict:
    """Loads the KNN bundle (model + scaler + feature matrix + metadata)."""
    if not KNN_MODEL.exists():
        raise FileNotFoundError(
            f"KNN model not found at {KNN_MODEL}.  "
            "Run: python src/models/train_models.py"
        )
    bundle = joblib.load(KNN_MODEL)
    logger.info("KNN bundle loaded from: %s", KNN_MODEL)
    return bundle


# ===========================================================================
# KAFKA PRODUCER UTILITY
# ===========================================================================

def _get_producer() -> Producer:
    """Creates a confluent_kafka Producer (one per batch call is acceptable)."""
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5})


def _produce(producer: Producer, topic: str, payload: dict) -> None:
    """Serialises *payload* to JSON and produces it to *topic*."""
    producer.produce(
        topic=topic,
        value=json.dumps(payload, default=str).encode("utf-8"),
    )
    producer.poll(0)


# ===========================================================================
# BATCH PROCESSING FUNCTIONS  (called by foreachBatch)
# ===========================================================================

def _process_price_batch(
    batch_df: DataFrame,
    epoch_id: int,
    price_model: Any,
) -> None:
    """
    Processes a micro-batch of price prediction requests.

    Each row is a JSON payload with listing features + request_id.
    The function applies the sklearn pipeline and publishes the result.

    Parameters
    ----------
    batch_df : DataFrame
        Spark DataFrame with columns ['value'] (raw Kafka bytes).
    epoch_id : int
        Spark micro-batch epoch identifier.
    price_model : Any
        Pre-loaded sklearn Pipeline.
    """
    if batch_df.count() == 0:
        return

    logger.info("[Epoch %d] Processing %d price request(s).", epoch_id, batch_df.count())

    # Convert Kafka bytes to pandas
    pdf = batch_df.selectExpr("CAST(value AS STRING)").toPandas()

    producer = _get_producer()
    errors = 0

    for raw_value in pdf["value"]:
        try:
            request = json.loads(raw_value)
            request_id = request.get("request_id", "unknown")

            # Build feature DataFrame (single row)
            feature_row = {col: [request.get(col)] for col in ALL_PRICE_FEATURES}
            X = pd.DataFrame(feature_row)

            # Predict
            predicted_price = float(price_model.predict(X)[0])
            predicted_price = max(0.0, round(predicted_price, 2))

            response = {
                "request_id":      request_id,
                "predicted_price": predicted_price,
                "status":          "ok",
            }
            _produce(producer, TOPIC_PRECIOS, response)

        except Exception as exc:
            errors += 1
            logger.error("Price prediction error for a request: %s", exc)
            try:
                # Best-effort error response so Streamlit doesn't hang
                _produce(producer, TOPIC_PRECIOS, {
                    "request_id": request.get("request_id", "unknown"),
                    "error":      str(exc),
                    "status":     "error",
                })
            except Exception:
                pass

    producer.flush()
    logger.info(
        "[Epoch %d] Completed. Errors: %d/%d.", epoch_id, errors, batch_df.count()
    )


def _process_knn_batch(
    batch_df: DataFrame,
    epoch_id: int,
    knn_bundle: dict,
) -> None:
    """
    Processes a micro-batch of 'like' events and returns KNN recommendations.

    Each row contains {'listing_id': int, 'request_id': str}.
    The function finds the K-nearest neighbours and publishes their metadata.

    Parameters
    ----------
    batch_df : DataFrame
        Spark DataFrame with raw Kafka messages.
    epoch_id : int
        Spark micro-batch epoch identifier.
    knn_bundle : dict
        Pre-loaded KNN model bundle (knn_model, scaler, listing_ids, ...).
    """
    if batch_df.count() == 0:
        return

    logger.info("[Epoch %d] Processing %d like event(s).", epoch_id, batch_df.count())

    pdf = batch_df.selectExpr("CAST(value AS STRING)").toPandas()

    # Unpack bundle
    knn            = knn_bundle["knn_model"]
    feature_matrix = knn_bundle["feature_matrix"]   # already scaled
    listing_ids    = knn_bundle["listing_ids"]
    display_df     = knn_bundle["display_df"]

    producer = _get_producer()
    errors = 0

    for raw_value in pdf["value"]:
        try:
            event      = json.loads(raw_value)
            request_id = event.get("request_id", "unknown")
            listing_id = int(event.get("listing_id"))

            # Find the query listing in the feature matrix
            id_positions = np.where(listing_ids == listing_id)[0]
            if len(id_positions) == 0:
                logger.warning("listing_id %d not found in KNN index.", listing_id)
                _produce(producer, TOPIC_SUGERENCIAS, {
                    "request_id":    request_id,
                    "recommendations": [],
                    "status":        "listing_not_found",
                })
                continue

            query_idx    = id_positions[0]
            query_vector = feature_matrix[query_idx].reshape(1, -1)

            # Query KNN (returns indices including self)
            distances, indices = knn.kneighbors(query_vector)

            recommendations = []
            for dist, idx in zip(distances[0], indices[0]):
                neighbour_id = int(listing_ids[idx])

                # Skip the query listing itself
                if neighbour_id == listing_id:
                    continue

                # Build recommendation metadata from display_df
                row_matches = display_df[display_df["id"] == neighbour_id]
                if row_matches.empty:
                    continue

                row = row_matches.iloc[0].to_dict()
                # Convert numpy types for JSON serialisation
                rec = {k: (v.item() if hasattr(v, "item") else v) for k, v in row.items()}
                rec["similarity_score"] = round(float(1 - dist), 4)
                recommendations.append(rec)

                if len(recommendations) >= 3:
                    break

            _produce(producer, TOPIC_SUGERENCIAS, {
                "request_id":      request_id,
                "recommendations": recommendations,
                "status":          "ok",
            })

        except Exception as exc:
            errors += 1
            logger.error("KNN recommendation error: %s", exc)
            try:
                _produce(producer, TOPIC_SUGERENCIAS, {
                    "request_id":      event.get("request_id", "unknown"),
                    "recommendations": [],
                    "error":           str(exc),
                    "status":          "error",
                })
            except Exception:
                pass

    producer.flush()
    logger.info(
        "[Epoch %d] Completed. Errors: %d/%d.", epoch_id, errors, batch_df.count()
    )


# ===========================================================================
# SPARK SESSION & STREAM SETUP
# ===========================================================================

def _build_spark_session() -> SparkSession:
    """Creates and returns the configured SparkSession."""
    return (
        SparkSession.builder
        .appName("AirbnbMLProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
        )
        # Reduce log verbosity from Spark internals
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def _build_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    """
    Creates a raw Kafka readStream subscribed to *topic*.
    Returns a DataFrame with Kafka's default schema (key, value, ...).
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    """
    Entry point.
    Loads models, starts both streaming queries, and blocks until termination.
    """
    # ----- LOAD MODELS -----
    price_model = _load_price_model()
    knn_bundle  = _load_knn_bundle()

    # ----- SPARK SESSION -----
    spark = _build_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialised.")

    # ----- STREAM 1: Price Prediction -----
    price_stream = _build_kafka_stream(spark, TOPIC_PETICIONES)

    price_query = (
        price_stream
        .writeStream
        .foreachBatch(lambda df, epoch: _process_price_batch(df, epoch, price_model))
        .trigger(processingTime="5 seconds")
        .queryName("price_prediction_stream")
        .start()
    )
    logger.info("Price prediction stream started (topic: %s).", TOPIC_PETICIONES)

    # ----- STREAM 2: KNN Recommendations -----
    knn_stream = _build_kafka_stream(spark, TOPIC_LIKES)

    knn_query = (
        knn_stream
        .writeStream
        .foreachBatch(lambda df, epoch: _process_knn_batch(df, epoch, knn_bundle))
        .trigger(processingTime="5 seconds")
        .queryName("knn_recommendation_stream")
        .start()
    )
    logger.info("KNN recommendation stream started (topic: %s).", TOPIC_LIKES)

    logger.info("ML Processor is running. Press Ctrl+C to stop.")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown requested. Stopping streams…")
        price_query.stop()
        knn_query.stop()
        spark.stop()
        logger.info("ML Processor stopped cleanly.")


if __name__ == "__main__":
    main()
