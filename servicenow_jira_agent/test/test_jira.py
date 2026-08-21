import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# Jira Configuration
# ============================================================

JIRA_URL = os.getenv("JIRA_URL")
JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
JIRA_PROJECT = os.getenv("JIRA_PROJECT")


# ============================================================
# Jira Authentication
# ============================================================

auth = HTTPBasicAuth(
    JIRA_EMAIL,
    JIRA_API_TOKEN
)

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}


# ============================================================
# Step 1: Test Jira Connection
# ============================================================

print("Testing Jira connection...")

response = requests.get(
    f"{JIRA_URL}/rest/api/3/myself",
    auth=auth,
    headers={
        "Accept": "application/json"
    }
)

print("HTTP Status:", response.status_code)

if response.status_code != 200:
    print("Jira connection failed.")
    print(response.text)
    exit(1)

user = response.json()

print("Connected successfully!")
print("Jira user:", user.get("displayName"))
print()


# ============================================================
# Step 2: Create Jira Test Ticket
# ============================================================

print("Creating Jira test ticket...")

payload = {
    "fields": {
        "project": {
            "key": JIRA_PROJECT
        },

        "summary": "[POC] Test ticket created from Python",

        "issuetype": {
            "name": "Task"
        },

        "description": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "This is a test Jira ticket created "
                                "from Python for the ServiceNow POC."
                            )
                        }
                    ]
                }
            ]
        }
    }
}


response = requests.post(
    f"{JIRA_URL}/rest/api/3/issue",
    auth=auth,
    headers=headers,
    json=payload
)


print("HTTP Status:", response.status_code)

if response.status_code == 201:

    result = response.json()

    jira_key = result["key"]

    print()
    print("===================================")
    print("Jira ticket created successfully!")
    print("Jira Key:", jira_key)
    print("===================================")

else:

    print("Failed to create Jira ticket.")
    print(response.text)
