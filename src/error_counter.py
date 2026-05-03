from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# ---------- 1. INITIALIZE THE SPARK SESSION ----------
spark = SparkSession.builder \
    .appname("KafkaErrorStreamCounter") \
    .getOrCreate()

# ---------- 2. DEFINE THE SCHEMA FOR ERROR JSON ----------
error_schema = StructType([
    StructField("task", StringType(), True),
    StructField("variable", StringType(), True),
    StructField("message", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# ---------- 3. CONNECT TO THE KAFKA STREAM ----------
error_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pipeline_errors") \
    .option("startingOffsets", "earliest") \
    .load()

# ---------- 4. DESERIALIZE THE JSON DATA ----------
parsed_errors_df = error_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), error_schema).alias("data")) \
    .select("data.*")

# ---------- 5. QUERY: COUNT ERRORS PER VARIABLE ----------
error_counts_df = parsed_errors_df.groupBy("variable").count()
  
# ---------- 6. SINK: DISPLAY THE RESULTS ----------
query = error_counts_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()