import sys
import pathlib

# --------- PATH CONFIGURATION ---------
current_file = pathlib.Path(__file__).resolve()
project_root = current_file.parent.parent.parent

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.kafka.consumer_kafka import create_kafka_stream_df, consumer_kafka_avro
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, sum as spark_sum

spark = (SparkSession.builder
        .appName("calendar_query")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.spark:spark-avro_2.13:4.1.1")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark Session iniciada con éxito.")

# Read stream using imported function
df_calendar_raw = create_kafka_stream_df(spark, "airbnb_calendar_gold")

# Apply needed transformations and date cast
df_processed = (df_calendar_raw
        .withColumn("event_timestamp", col("date").cast("timestamp"))
        .withColumn("booked", col("booked").cast("integer"))
)

# Window and calculate availability
df_group = (df_processed
        .withWatermark("event_timestamp", "7 days")
        .groupBy(
            window(col("event_timestamp"), "30 days", "7 days")
        ).agg(
            (avg(col("booked")) * 100).alias("ocupacion_media"),
            spark_sum(col("booked")).alias("reservas_totales")
        )
)

# Sort chronologically
df_final = df_group.orderBy(col("window.start").desc())

# Inicializate stream
query_calendar = (df_final.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .queryName("tabla_resultados_calendar")
        .start()
)

query_calendar.awaitTermination()