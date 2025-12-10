import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

schema = (
    StructType()
    .add("device_id", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("timestamp", LongType())
    .add("city_code", StringType())
)

spark = SparkSession.builder \
    .appName("IoT Kafka Test") \
    .master("local[*]") \
    .getOrCreate()

kafka_broker = "localhost:9092"
kafka_topic = "iot-data"

print(f"Connecting to Kafka: {kafka_broker}, Topic: {kafka_topic}")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("✅ Spark is reading from Kafka and displaying to console...")
query.awaitTermination()
