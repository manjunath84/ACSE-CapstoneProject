import time
import random
from pymongo import MongoClient, GEOSPHERE

# MongoDB connection details
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"
COLLECTION_NAME = "taxis"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Ensure the collection has a 2dsphere index on the location field for geospatial queries
collection.create_index([("location", GEOSPHERE)])

def update_taxi_locations():
    """Simulate slow movement by slightly altering the location of each taxi."""
    for taxi in collection.find():
        new_location = {
            'type': "Point",
            'coordinates': [
                taxi['location']['coordinates'][0] + random.uniform(-0.01, 0.01),
                taxi['location']['coordinates'][1] + random.uniform(-0.01, 0.01)
            ]
        }
        print( f"Updating taxi {taxi['name']} location to {new_location['coordinates']}")
        collection.update_one({'_id': taxi['_id']}, {'$set': {'location': new_location}})

def main():
    num_taxis = 50  # Number of taxis to simulate
    update_interval = 60  # Update interval in seconds

    while True:
        update_taxi_locations()
        print(f"Updated taxi locations at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(update_interval)

if __name__ == "__main__":
    main()
