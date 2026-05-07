from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# --------- INITIALIZE THE SPARK SESSION ---------
spark = SparkSession.builder \
    .appName("KafkaErrorStreamCounter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

# --------- DEFINE THE SCHEMA FOR ERROR JSON ---------
error_schema = StructType([
    StructField("task", StringType(), True),
    StructField("variable", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", StringType(), True)
])  

# --------- CONNECT TO THE KAFKA STREAM ---------
error_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pipeline_errors") \
    .option("startingOffsets", "earliest") \
    .load()

# --------- DESERIALIZE THE JSON DATA ---------
parsed_errors_df = error_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), error_schema).alias("data")) \
    .select("data.*")

# --------- QUERY: COUNT ERRORS PER VARIABLE ---------
error_details_df = parsed_errors_df.select("timestamp", "task", "variable", "message")
  
# --------- SINK: DISPLAY THE RESULTS ---------
query = error_details_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()