import os
import requests
from requests.auth import HTTPBasicAuth


# ============================================================
# Jira Configuration
# ============================================================

JIRA_URL = "https://aipocsri.atlassian.net"
JIRA_EMAIL = "srividhyaganesan1988@gmail.com"
JIRA_API_TOKEN = "ATATT3xFfGF0I080ue1C5AOeK9iJgFVtBxqt-_XawcY1onUdH_ifuDq4JtRzWijl2sQrq9Y74R7X-VnSU-3B1l4RFzsWlFSkxkjspy85ttiCNhxNgHSL_e-yxJs0SiuAZZ3QmDpF91ebQhLiF73mvT9nuL5Jnvx6CNrdmNDuQP8uo-gDf0liXJU=6E1BEC79"
JIRA_PROJECT = "SCRUM"


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
