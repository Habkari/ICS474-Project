from pymongo import MongoClient
from collections import defaultdict
import time

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["iot_pipeline"]
collection = db["sensor_data"]

device_stats = defaultdict(lambda: {"temps": [], "humidity": []})
city_stats = defaultdict(lambda: {"temps": [], "humidity": []})

def apply_record(r):
    device_stats[r["device_id"]]["temps"].append(r["temperature"])
    device_stats[r["device_id"]]["humidity"].append(r["humidity"])
    city_stats[r["city_code"]]["temps"].append(r["temperature"])
    city_stats[r["city_code"]]["humidity"].append(r["humidity"])

def print_summary():
    print("\n📈 LIVE - by Device (top 10)")
    print(f"{'Device ID':<15} {'Total':<8} {'Avg Temp':<10} {'Max Temp':<10} {'Avg Hum':<10}")
    print("-" * 65)
    for device_id, stats in list(sorted(device_stats.items()))[:10]:
        avg_temp = sum(stats["temps"]) / len(stats["temps"])
        max_temp = max(stats["temps"])
        avg_hum = sum(stats["humidity"]) / len(stats["humidity"])
        print(f"{device_id:<15} {len(stats['temps']):<8} {avg_temp:<10.2f} {max_temp:<10.2f} {avg_hum:<10.2f}")

# Bootstrap existing docs once
last_ts = None
for doc in collection.find({}, {"_id": 0}).sort("timestamp", 1):
    apply_record(doc)
    last_ts = doc.get("timestamp")

print("✅ Bootstrapped. Polling for new inserts every 2 seconds...")

while True:
    query = {}
    if last_ts is not None:
        query = {"timestamp": {"$gt": last_ts}}

    new_docs = list(collection.find(query, {"_id": 0}).sort("timestamp", 1))
    if new_docs:
        for doc in new_docs:
            apply_record(doc)
        last_ts = new_docs[-1]["timestamp"]
        print(f"\n✅ New docs: {len(new_docs)} (latest timestamp={last_ts})")
        print_summary()

    time.sleep(2)
