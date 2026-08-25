"""
Thin REST client for creating issues in a Jira Cloud project.

Uses the Jira REST API v3 (https://<site>.atlassian.net/rest/api/3) with
basic auth (email + API token) -- the same approach validated by the
connectivity check in test/test_jira.py.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests


class JiraConfigError(RuntimeError):
    """Raised when required Jira connection settings are missing."""


class JiraAPIError(RuntimeError):
    """Raised when the Jira API returns an error response."""


@dataclass
class JiraIssue:
    key: str
    url: str


class JiraClient:
    """Minimal client for creating issues in a Jira Cloud project."""

    def __init__(
        self,
        url: str | None = None,
        email: str | None = None,
        api_token: str | None = None,
        project: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.url = (url or os.getenv("JIRA_URL") or "").strip().rstrip("/")
        self.email = email or os.getenv("JIRA_EMAIL")
        self.api_token = api_token or os.getenv("JIRA_API_TOKEN")
        self.project = project or os.getenv("JIRA_PROJECT")
        self.timeout = timeout

        if not all([self.url, self.email, self.api_token, self.project]):
            raise JiraConfigError(
                "Missing Jira connection settings. Set JIRA_URL, JIRA_EMAIL, "
                "JIRA_API_TOKEN and JIRA_PROJECT (env vars or .env file)."
            )

    def create_issue(
        self,
        summary: str,
        description: str = "",
        issue_type: str = "Task",
        extra_fields: dict[str, Any] | None = None,
    ) -> JiraIssue:
        """
        Create an issue in the configured Jira project and return its key
        (e.g. "SCRUM-42") and browse URL.
        """
        fields: dict[str, Any] = {
            "project": {"key": self.project},
            "summary": summary,
            "issuetype": {"name": issue_type},
        }
        if description:
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": description}],
                    }
                ],
            }
        if extra_fields:
            fields.update(extra_fields)

        response = requests.post(
            f"{self.url}/rest/api/3/issue",
            auth=(self.email, self.api_token),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json={"fields": fields},
            timeout=self.timeout,
        )

        if response.status_code != 201:
            raise JiraAPIError(
                f"Jira API returned {response.status_code}: {response.text[:500]}"
            )

        result = response.json()
        key = result["key"]
        return JiraIssue(key=key, url=f"{self.url}/browse/{key}")
