import os
import requests
from dotenv import load_dotenv

load_dotenv()

instance = os.getenv("SERVICENOW_INSTANCE")
username = os.getenv("SERVICENOW_USERNAME")
password = os.getenv("SERVICENOW_PASSWORD")

print("Instance:", instance)
print("Username:", username)
print("Password configured:", bool(password))

url = f"{instance.rstrip('/')}/api/now/table/incident"

response = requests.get(
    url,
    auth=(username, password),
    headers={
        "Accept": "application/json"
    },
    params={
        "sysparm_limit": 5
    },
    timeout=30
)

print("HTTP Status:", response.status_code)
print("Response:")

try:
    print(response.json())
except Exception:
    print(response.text)
