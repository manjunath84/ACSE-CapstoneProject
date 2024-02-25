import json
import pymongo

# Connect to your MongoDB (replace with your actual connection details)
MONGO_URI = "mongodb://ec2-3-143-22-60.us-east-2.compute.amazonaws.com:27018/"
DB_NAME = "TaxiAggregator"


def fetch_service_area_boundary(db):
    """
    Fetch the service area boundary from MongoDB.
    """
    print("Fetching service area boundary...")
    service_area = db.service_areas.find_one({"name": "Default Service Area"})
    if not service_area:
        print("Service area boundary not found.")
    return service_area['boundary'] if service_area else None


def is_within_boundary(point, boundary):
    """
    Check if the point is within the rectangular service area boundary.
    """
    print("is_within_boundary function started...")
    point_long, point_lat = point['coordinates']
    from_long, from_lat = boundary['from']
    to_long, to_lat = boundary['to']

    # Check if the point's coordinates are within the boundary's 'from' and 'to' coordinates
    return (from_long <= point_long <= to_long) and (from_lat <= point_lat <= to_lat)


def lambda_handler(event, context):
    print("Lambda function started...")
    print(f"Processing {event} events.")
    print("Received path:", event.get('path'))
    # Establish a connection to MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    taxis_collection = db['taxis']  # The collection where taxi data is stored
    print("Connected to MongoDB.")

    # Fetch service area boundary from MongoDB
    boundary = fetch_service_area_boundary(db)
    if not boundary:
        print("Closing MongoDB connection due to missing boundary.")
        client.close()
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Service area boundary not found'})
        }
    # Determine the operation based on the event structure
    if 'taxis' in event:
        # Handle bulk taxi registrations
        taxis = event['taxis']
        operation = "bulk"
    elif 'name' in event and 'location' in event:
        # Handle single taxi registration
        taxis = [event]
        operation = "single"
    else:
        print("Invalid event structure")
        return {'statusCode': 400, 'body': json.dumps({'message': 'Invalid event structure'})}

    print(f"Operation: {operation}, Processing {len(taxis)} taxis...")

    # Filter taxis within the service area boundary
    taxis_within_boundary = [taxi for taxi in taxis if is_within_boundary(taxi['location'], boundary)]
    print(f"{len(taxis_within_boundary)} taxis are within the service area boundary.")

    # Insert taxis within the boundary into MongoDB
    if taxis_within_boundary:
        result = taxis_collection.insert_many(taxis_within_boundary)
        inserted_ids = result.inserted_ids
        response_message = 'Successfully registered taxis within the service area'
        response_data = {'registered_ids': [str(_id) for _id in inserted_ids]}
        print(f"Inserted {len(inserted_ids)} taxis.")
    else:
        response_message = 'No taxis were registered; none were within the service area'
        response_data = {}
        print("No taxis were registered.")

    # Close the MongoDB connection
    client.close()

    # Return the response
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': response_message,
            **response_data
        })
    }
