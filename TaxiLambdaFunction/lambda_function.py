import json
import pymongo

# Connect to your MongoDB (replace with your actual connection details)
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"


def fetch_service_area_boundary(db):
    print("Fetching service area boundary...")
    service_area = db.service_areas.find_one({"name": "Default Service Area"})
    if service_area:
        return service_area['boundary']
    else:
        print("Service area boundary not found.")
        return None


def is_within_boundary(point, boundary):
    print("Checking if point is within the service area boundary...")
    point_long, point_lat = point['coordinates']
    from_long, from_lat = boundary['from']
    to_long, to_lat = boundary['to']

    print("Boundary from:", boundary['from'], "to:", boundary['to'])
    print("Point coordinates:", point['coordinates'])

    return (from_long <= point_long <= to_long) and (from_lat <= point_lat <= to_lat)


def lambda_handler(event, context):
    print("Lambda function started...")
    print(f"Processing {event} events.")
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    customer_collection = db['customers']

    boundary = fetch_service_area_boundary(db)
    if not boundary:
        print("Service area boundary not found, aborting operation.")
        return {'statusCode': 500, 'body': json.dumps({'message': 'Service area boundary not found'})}

    if 'location' in event and 'name' in event:  # Simplified check for required fields
        if is_within_boundary(event['location'], boundary):
            try:
                result = customer_collection.insert_one(event)
                response_message = 'Successfully registered customer within the service area'
                response_data = {'registered_id': str(result.inserted_id)}
                print(response_message)
            except Exception as e:
                print(f"Error inserting customer into MongoDB: {e}")
                response_message = 'Failed to register customer'
                response_data = {}
        else:
            response_message = 'Customer not within the service area'
            response_data = {}
            print(response_message)
    else:
        response_message = 'Invalid customer data'
        response_data = {}
        print(response_message)

    client.close()
    return {'statusCode': 200, 'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': response_message, **response_data})}

