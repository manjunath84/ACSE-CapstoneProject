import requests
import json
import random
from pymongo import MongoClient

# MongoDB connection details
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"
SERVICE_AREA_COLLECTION = "service_areas"

# Constants
API_ENDPOINT = "https://udmqfurfie.execute-api.us-east-2.amazonaws.com/dev/taxis"
TAXI_TYPES = ["Luxury", "Basic", "Deluxe"]
NUM_TAXIS = 50

# Establish a connection to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
service_area_collection = db[SERVICE_AREA_COLLECTION]

def fetch_service_area_boundary():
    """Fetch the service area boundary from MongoDB."""
    service_area_doc = service_area_collection.find_one({"name": "Default Service Area"})
    if service_area_doc and 'boundary' in service_area_doc:
        return service_area_doc['boundary']
    else:
        raise ValueError("Service area boundary not found in the database.")

# Generate random taxi data within the service area boundary
def generate_taxi_data(num_taxis, boundary):
    taxis_data = []
    min_long, min_lat = boundary["from"]
    max_long, max_lat = boundary["to"]

    for i in range(num_taxis):
        taxi_data = {
            'name': f"Taxi_{i + 1:03d}",
            'type': random.choice(TAXI_TYPES),
            'location': {
                'type': "Point",
                'coordinates': [
                    random.uniform(min_long, max_long),  # Longitude within boundary
                    random.uniform(min_lat, max_lat)     # Latitude within boundary
                ]
            }
        }
        taxis_data.append(taxi_data)
    return taxis_data

# Post the data to the API
def post_taxi_data(taxis_data):
    headers = {"Content-Type": "application/json"}
    response = requests.post(API_ENDPOINT, json={"taxis": taxis_data}, headers=headers)
    print(response.text)  # Optional: Print response for debugging
    return response

# Main execution
if __name__ == "__main__":
    boundary = fetch_service_area_boundary()  # Fetch boundary from MongoDB
    taxis_data = generate_taxi_data(NUM_TAXIS, boundary)
    response = post_taxi_data(taxis_data)
    print(f"Status Code: {response.status_code}")  # Print status code for debugging