import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Define schema
schema = (
    StructType()
    .add("device_id", StringType())
    .add("temperature", DoubleType())
    .add("humidity", DoubleType())
    .add("timestamp", LongType())
    .add("city_code", StringType())
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SparkStreamingKafka")

logger.info("Starting Spark Streaming Kafka application with MongoDB.")

spark = SparkSession.builder \
    .appName("IoT Kafka Spark MongoDB Stream") \
    .master("local[*]") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/iot_pipeline.sensor_data") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.2.0") \
    .getOrCreate()

# Get Kafka broker and topic from environment variables
kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
kafka_topic = os.getenv("KAFKA_TOPIC", "iot-data")

logger.info(f"Kafka broker: {kafka_broker}, Topic: {kafka_topic}")
logger.info(f"MongoDB: mongodb://localhost:27017/iot_pipeline.sensor_data")

try:
    # Read stream from Kafka
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .load()

    # Deserialize and process
    json_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Convert timestamp to proper datetime
    json_df = json_df.withColumn("timestamp", to_timestamp(col("timestamp")))
    json_df = json_df.withColumn("date", from_unixtime(col("timestamp").cast("long"), "yyyy-MM-dd"))

    # Write to MongoDB
    def write_to_mongodb(batch_df, batch_id):
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "iot_pipeline") \
            .option("collection", "sensor_data") \
            .save()
        logger.info(f"Batch {batch_id}: Wrote {batch_df.count()} records to MongoDB")

    query = json_df.writeStream \
        .foreachBatch(write_to_mongodb) \
        .outputMode("append") \
        .start()

    logger.info("Spark Streaming application is running and writing to MongoDB.")
    query.awaitTermination()
except Exception as e:
    logger.error(f"An error occurred: {e}", exc_info=True)
    raise
