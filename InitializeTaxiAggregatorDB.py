from pymongo import MongoClient, GEOSPHERE

# MongoDB connection details
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"


def initialize_db():
    # Establish a connection to the MongoDB database
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Initialize collections
    taxis = db['taxis']
    customers = db['customers']
    service_areas = db['service_areas']

    # Create a 2dsphere index on the 'location' field for the taxis and customers collections
    taxis.create_index([('location', GEOSPHERE)])
    print("Created geospatial index for 'taxis' collection.")

    customers.create_index([('location', GEOSPHERE)])
    print("Created geospatial index for 'customers' collection.")

    # Initialize the service_areas collection with a default document if it doesn't exist
    if service_areas.count_documents({}) == 0:
        default_service_area = {
            "_id": "65d147e4862ac3cd54f7bf36",
            "name": "Default Service Area",
            "boundary": {
                "from": [76.23149, 27.61123],  # Southwest corner
                "to": [78.36493, 29.70292]  # Northeast corner
            }
        }
        service_areas.insert_one(default_service_area)
        print("Inserted default service area document into 'service_areas' collection.")
    else:
        print("'service_areas' collection already initialized.")

    # Close the database connection
    client.close()


if __name__ == "__main__":
    initialize_db()
