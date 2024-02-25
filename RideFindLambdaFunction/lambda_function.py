import json
from pymongo import MongoClient, GEOSPHERE

# Environment variables for MongoDB connection
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"

def fetch_nearest_taxis(customer_loc, taxi_type=None, limit=2):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    taxis = db['taxis']

    # Create a geospatial query based on customer location and taxi type preference
    query = {'location': {"$near": {"$geometry": customer_loc}}}
    if taxi_type and taxi_type.lower() != 'all':
        query['type'] = taxi_type

    # Fetch the nearest taxis based on the query
    nearest_taxis = list(taxis.find(query).limit(limit))

    client.close()
    return nearest_taxis

def lambda_handler(event, context):
    # Directly use the event as the body if it doesn't come from API Gateway
    body = event

    customer_loc = body.get('location')
    taxi_type = body.get('type', 'All')  # Default to 'All' if not specified

    # Ensure customer location is provided
    if not customer_loc or 'coordinates' not in customer_loc:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Customer location is required and must include coordinates'})
        }

    # Fetch nearest taxis
    nearest_taxis = fetch_nearest_taxis(customer_loc, taxi_type)

    # Return the nearest taxis in the response
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(nearest_taxis, default=str)  # Convert ObjectId to str
    }
