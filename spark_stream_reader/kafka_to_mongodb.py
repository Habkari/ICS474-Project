from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger("KafkaToMongoDB")

# Kafka & MongoDB config
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "iot-data"
MONGO_URI = "mongodb://localhost:27017"
DATABASE = "iot_pipeline"
COLLECTION = "sensor_data"

logger.info("🚀 Starting real-time data ingestion: Kafka → MongoDB")

# Connect to MongoDB
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE]
collection = db[COLLECTION]
logger.info(f"✅ Connected to MongoDB: {DATABASE}.{COLLECTION}")

# Connect to Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='mongodb-consumer-group'
)
logger.info(f"✅ Connected to Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")

count = 0
try:
    for message in consumer:
        data = message.value
        data['timestamp'] = datetime.fromtimestamp(data['timestamp'])
        collection.insert_one(data)
        count += 1
        logger.info(f"📊 Record {count}: {data['device_id']} | Temp: {data['temperature']}°C | {data['city_code']}")
except KeyboardInterrupt:
    logger.info(f"\n✅ Stopped. Total records: {count}")
finally:
    consumer.close()
    mongo_client.close()
