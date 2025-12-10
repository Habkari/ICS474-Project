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
    data = list(collection.find({}, {"_id": 0}).limit(1000))
    return jsonify(data)

if __name__ == '__main__':
    print("🚀 API Server running on http://localhost:5000/api/data")
    app.run(debug=False, port=5000, threaded=True)
