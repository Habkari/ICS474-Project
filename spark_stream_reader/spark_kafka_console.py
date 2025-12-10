import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

schema = (
    StructType()
    .add("device_id", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("timestamp", LongType())
    .add("city_code", StringType())
)

print("Starting Spark with Kafka...")

# Use file:/// protocol with forward slashes
spark = SparkSession.builder \
    .appName("IoT Kafka Spark Stream") \
    .master("local[*]") \
    .config("spark.jars", "file:///C:/spark_jars/spark-sql-kafka-0-10_2.13-3.5.0.jar,file:///C:/spark_jars/kafka-clients-3.5.0.jar,file:///C:/spark_jars/commons-pool2-2.11.1.jar,file:///C:/spark_jars/spark-token-provider-kafka-0-10_2.13-3.5.0.jar") \
    .getOrCreate()

kafka_broker = "localhost:9092"
kafka_topic = "iot-data"

print(f"Kafka broker: {kafka_broker}, Topic: {kafka_topic}")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

json_df = json_df.withColumn("date", from_unixtime(col("timestamp"), "yyyy-MM-dd"))

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("✅ Spark Streaming from Kafka is running!")
query.awaitTermination()
