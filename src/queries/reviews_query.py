import sys
import pathlib

# --------- PATH CONFIGURATION ---------
current_file = pathlib.Path(__file__).resolve()
project_root = current_file.parent.parent.parent

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.kafka.consumer_kafka import create_kafka_stream_df, consumer_kafka_avro

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, window
from pyspark.sql.types import StringType
import pandas as pd

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# --------- NLP MODEL DEFINITION ---------
@pandas_udf(StringType())
def classify_review(comentarios: pd.Series) -> pd.Series:
    nltk.download('vader_lexicon', quiet=True)
    
    # Initialize VADER analyzer
    sia = SentimentIntensityAnalyzer()
    
    def evaluate_feeling(texto):
        if not texto or pd.isna(texto):
            return "informativa"
        
        # Get VADER polarity scores
        scores = sia.polarity_scores(str(texto))
        
        # Extract compound score
        compound = scores['compound']
        
        # Apply thresholds
        if compound > 0.15: 
            return "buena"
        elif compound < -0.15: 
            return "mala"
        else: 
            return "informativa"
            
    return comentarios.apply(evaluate_feeling)

spark = SparkSession.builder \
    .appName("reviews_query") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.spark:spark-avro_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session iniciada con éxito.")

# Read stream using imported function
df_reviews_crudo = create_kafka_stream_df(spark, "airbnb_reviews_gold")

# Apply classification and date cast
df_procesado = df_reviews_crudo \
    .withColumn("tipo_review", classify_review(col("comments"))) \
    .withColumn("event_timestamp", col("date").cast("timestamp"))

# Window and count
df_agrupado = df_procesado \
    .withWatermark("event_timestamp", "7 days") \
    .groupBy(
        window(col("event_timestamp"), "365 days"),
        col("tipo_review")
    ).count()

# Sort chronologically
df_final = df_agrupado.orderBy(col("window.start").desc(), col("tipo_review"))

# Inicializate stream
query_reviews = df_final.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("tabla_resultados_reviews") \
    .start()

query_reviews.awaitTermination()