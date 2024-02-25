import json
import pymongo
from bson import ObjectId

# Replace with your actual DocumentDB URI
MONGO_URI = "mongodb://username:password@your-mongodb-uri:port"
DB_NAME = "your_db_name"


def fetch_service_area_boundary(db):
    """
    Fetch the service area boundary from MongoDB.
    """
    service_area = db.service_areas.find_one({"name": "Default Service Area"})
    return service_area['boundary'] if service_area else None


def is_within_boundary(point, boundary):
    """
    Check if the point is within the rectangular service area boundary.
    """
    point_long, point_lat = point['coordinates']
    from_long, from_lat = boundary['from']
    to_long, to_lat = boundary['to']

    # Check if the point's coordinates are within the boundary's 'from' and 'to' coordinates
    return (from_long <= point_long <= to_long) and (from_lat <= point_lat <= to_lat)


def lambda_handler(event, context):
    # Connect to MongoDB (DocumentDB)
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    taxis_collection = db['taxis']  # Replace with your actual collection name

    # Fetch the service area boundary
    service_area_boundary = fetch_service_area_boundary(db)

    if not service_area_boundary:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Service area boundary not found'})
        }

    # Parse the event body
    try:
        body = json.loads(event['body'])
    except (TypeError, json.JSONDecodeError):
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Invalid JSON format'})
        }

    taxis = body.get('taxis', []) if event['path'] == "/register/taxis/bulk" else [body]

    # Filter out taxis outside the service area boundary
    taxis_within_boundary = [
        taxi for taxi in taxis
        if is_within_boundary(taxi['location'], service_area_boundary)
    ]

    # Insert the filtered taxis into the collection
    response_data = {}
    if taxis_within_boundary:
        result = taxis_collection.insert_many(taxis_within_boundary)
        inserted_ids = [str(_id) for _id in result.inserted_ids]
        response_message = 'Successfully registered taxis within the service area'
        response_data['registered_taxis'] = inserted_ids
    else:
        response_message = 'No taxis were registered; none were within the service area'

    # Close the connection
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
