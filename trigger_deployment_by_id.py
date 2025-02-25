import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve variables from .env file
ACCOUNT_ID = os.getenv("account_id")
WORKSPACE_ID = os.getenv("workspace_id")
DEPLOYMENT_ID = os.getenv("deployment_id")
API_KEY = os.getenv("prefect_api_key")


# Ensure all required variables are set
if not ACCOUNT_ID or not WORKSPACE_ID or not DEPLOYMENT_ID or not API_KEY:
    raise ValueError(
        "Missing required environment variables. Please check your .env file."
    )

# Construct the API URL
API_URL = f"https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}/deployments/{DEPLOYMENT_ID}/create_flow_run"

# Make the API request
try:
    response = requests.post(
        API_URL,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        json={}, 
    )

    # Check the response
    if response.status_code == 201:
        print("Flow run triggered successfully!")
        print("Flow Run ID:", response.json().get("id"))
    else:
        print(f"Failed to trigger flow run. Status code: {response.status_code}")
        print(f"Response: {response.text}")

except requests.exceptions.RequestException as e:
    print(f"An error occurred while making the API request: {e}")

