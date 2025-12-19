from flask import Flask, jsonify
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

client = MongoClient("mongodb://localhost:27017")
db = client["iot_pipeline"]
collection = db["sensor_data"]

@app.route('/api/data', methods=['GET'])
def get_data():
    # Get the latest 1000 readings
    data = list(collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(1000))

    # Return oldest->newest (better for time-series plotting)
    data.reverse()

    # Make timestamps JSON serializable + Grafana-friendly (ISO 8601)
    for d in data:
        ts = d.get("timestamp")
        if hasattr(ts, "isoformat"):
            d["timestamp"] = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    return jsonify(data)

if __name__ == '__main__':
    print("🚀 API Server running on http://localhost:5000/api/data")
    app.run(debug=False, port=5000, threaded=True)
