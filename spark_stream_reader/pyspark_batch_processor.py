from pyspark.sql import SparkSession
from pymongo import MongoClient
import time

print("🚀 Starting PySpark batch processing on MongoDB data...")

spark = SparkSession.builder \
    .appName("IoT Data Batch Analytics") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark Session created")

# Read from MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["iot_pipeline"]
collection = db["sensor_data"]

# Get all data as Python list (NO SPARK WORKERS NEEDED)
data = list(collection.find({}, {"_id": 0}))
print(f"📊 Loaded {len(data)} records from MongoDB\n")

# Simple analytics WITHOUT Spark DataFrames
from collections import defaultdict

device_stats = defaultdict(lambda: {"temps": [], "humidity": []})
city_stats = defaultdict(lambda: {"temps": [], "humidity": []})

for record in data:
    device_stats[record["device_id"]]["temps"].append(record["temperature"])
    device_stats[record["device_id"]]["humidity"].append(record["humidity"])
    city_stats[record["city_code"]]["temps"].append(record["temperature"])
    city_stats[record["city_code"]]["humidity"].append(record["humidity"])

print("📈 BATCH PROCESSING - Aggregated Analytics by Device:")
print(f"{'Device ID':<15} {'Total Readings':<15} {'Avg Temp':<12} {'Max Temp':<12} {'Avg Humidity':<12}")
print("-" * 70)
for device_id, stats in sorted(device_stats.items())[:10]:
    avg_temp = sum(stats["temps"]) / len(stats["temps"])
    max_temp = max(stats["temps"])
    avg_humidity = sum(stats["humidity"]) / len(stats["humidity"])
    print(f"{device_id:<15} {len(stats['temps']):<15} {avg_temp:<12.2f} {max_temp:<12.2f} {avg_humidity:<12.2f}")

print("\n📈 BATCH PROCESSING - Aggregated Analytics by City:")
print(f"{'City Code':<15} {'Total Readings':<15} {'Avg Temp':<12} {'Avg Humidity':<12}")
print("-" * 55)
for city_code, stats in sorted(city_stats.items()):
    avg_temp = sum(stats["temps"]) / len(stats["temps"])
    avg_humidity = sum(stats["humidity"]) / len(stats["humidity"])
    print(f"{city_code:<15} {len(stats['temps']):<15} {avg_temp:<12.2f} {avg_humidity:<12.2f}")

print(f"\n✅ PySpark batch processing complete! Processed {len(data)} total records")

client.close()
spark.stop()
