import requests
import json

# API endpoint URL
API_URL = "https://udmqfurfie.execute-api.us-east-2.amazonaws.com/dev/customer"  # Assuming the endpoint for a single user is '/user'

# Sample user data
users_data = [
    {
        'name': "Mohan",
        'location': {
            'type': "Point",
            'coordinates': [77.23098, 28.64454]
        }
    },
    {
        'name': "Isaac",
        'location': {
            'type': "Point",
            'coordinates': [77.23156, 28.67676]
        }
    },
    {
        'name': "Amir",
        'location': {
            'type': "Point",
            'coordinates': [77.23121, 28.65232]
        }
    },
    {
        'name': "Ashok",
        'location': {
            'type': "Point",
            'coordinates': [77.23176, 28.66132]
        }
    },
    {
        'name': "Leonard",
        'location': {
            'type': "Point",
            'coordinates': [77.23172, 28.67423]
        }
    }
]

def post_user(user_data):
    """Posts a single user data to the specified API endpoint."""
    headers = {'Content-Type': 'application/json'}
    response = requests.post(API_URL, data=json.dumps(user_data), headers=headers)
    if response.status_code == 200 or response.status_code == 201:
        print(f"User {user_data['name']} registered successfully.")
    else:
        print(f"Failed to register user {user_data['name']}. Response: {response.text}")

if __name__ == "__main__":
    for user in users_data:
        post_user(user)
