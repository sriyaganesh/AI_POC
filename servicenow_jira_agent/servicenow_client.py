"""
Thin REST client for a ServiceNow Personal Developer Instance (PDI).

Uses the Table API (https://<instance>.service-now.com/api/now/table/incident)
with basic auth, which is what PDIs support out of the box.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests


class ServiceNowConfigError(RuntimeError):
    """Raised when required ServiceNow connection settings are missing."""


class ServiceNowAPIError(RuntimeError):
    """Raised when the ServiceNow API returns an error response."""


@dataclass
class Incident:
    number: str
    sys_id: str
    short_description: str
    state: str
    priority: str
    opened_at: str
    caller: str
    assignment_group: str
    description: str = ""

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "Incident":
        def _val(field: str) -> str:
            # Table API fields can come back as plain strings or as
            # {"value": ..., "display_value": ...} dicts when sysparm_display_value is used.
            v = record.get(field, "")
            if isinstance(v, dict):
                return v.get("display_value") or v.get("value") or ""
            return v or ""

        return cls(
            number=_val("number"),
            sys_id=_val("sys_id"),
            short_description=_val("short_description"),
            state=_val("state"),
            priority=_val("priority"),
            opened_at=_val("opened_at"),
            caller=_val("caller_id"),
            assignment_group=_val("assignment_group"),
            description=_val("description"),
        )


class ServiceNowClient:
    """Minimal client for reading incidents from a ServiceNow PDI."""

    def __init__(
        self,
        instance: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: int = 30,
    ) -> None:
        self.instance = instance or os.getenv("SERVICENOW_INSTANCE")
        self.username = username or os.getenv("SERVICENOW_USERNAME")
        self.password = password or os.getenv("SERVICENOW_PASSWORD")
        self.timeout = timeout

        if not all([self.instance, self.username, self.password]):
            raise ServiceNowConfigError(
                "Missing ServiceNow connection settings. Set SERVICENOW_INSTANCE, "
                "SERVICENOW_USERNAME and SERVICENOW_PASSWORD (env vars or .env file)."
            )

        # Allow either "dev12345" or a full URL to be supplied.
        if self.instance.startswith("http"):
            self.base_url = self.instance.rstrip("/")
        else:
            self.base_url = f"https://{self.instance}.service-now.com"

    def list_incidents(
        self,
        limit: int = 10,
        state: str | None = None,
        query: str | None = None,
        order_by_newest: bool = True,
    ) -> list[Incident]:
        """
        Fetch incidents from the PDI.

        Args:
            limit: max number of records to return (sysparm_limit).
            state: optional ServiceNow incident state to filter on, e.g.
                "1" (New), "2" (In Progress), "6" (Resolved), "7" (Closed).
            query: optional raw ServiceNow encoded query (sysparm_query) for
                advanced filtering, e.g. "priority=1^active=true".
            order_by_newest: if True, sorts by opened_at descending.
        """
        url = f"{self.base_url}/api/now/table/incident"

        query_parts = []
        if query:
            query_parts.append(query)
        if state:
            query_parts.append(f"state={state}")
        if order_by_newest:
            query_parts.append("ORDERBYDESCopened_at")

        params: dict[str, Any] = {
            "sysparm_limit": limit,
            "sysparm_display_value": "true",
            "sysparm_fields": (
                "number,sys_id,short_description,description,state,priority,"
                "opened_at,caller_id,assignment_group"
            ),
        }
        if query_parts:
            params["sysparm_query"] = "^".join(query_parts)

        response = requests.get(
            url,
            auth=(self.username, self.password),
            headers={"Accept": "application/json"},
            params=params,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise ServiceNowAPIError(
                f"ServiceNow API returned {response.status_code}: {response.text[:500]}"
            )

        records = response.json().get("result", [])
        return [Incident.from_record(r) for r in records]

    def find_group_sys_id(self, name: str) -> str | None:
        """
        Look up an assignment group (sys_user_group) by exact name and return
        its sys_id, or None if no group matches -- including when the API
        user lacks read access to sys_user_group, which ServiceNow reports
        as an empty result rather than an error.
        """
        response = requests.get(
            f"{self.base_url}/api/now/table/sys_user_group",
            auth=(self.username, self.password),
            headers={"Accept": "application/json"},
            params={"sysparm_query": f"name={name}", "sysparm_fields": "sys_id", "sysparm_limit": 1},
            timeout=self.timeout,
        )

        if response.status_code != 200:
            raise ServiceNowAPIError(
                f"ServiceNow API returned {response.status_code}: {response.text[:500]}"
            )

        records = response.json().get("result", [])
        return records[0]["sys_id"] if records else None

    def update_incident(self, sys_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        """
        Update fields on an existing incident record, e.g. to transition its
        state (see incident_monitor_ollama.IN_PROGRESS_INCIDENT_STATE) or
        append a work note referencing a Jira ticket.

        Returns the updated record (raw values) as ServiceNow saved it. Note
        that ServiceNow silently ignores fields the API user isn't allowed
        to write instead of failing, so callers should check the result.
        """
        url = f"{self.base_url}/api/now/table/incident/{sys_id}"

        response = requests.patch(
            url,
            auth=(self.username, self.password),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json=fields,
            timeout=self.timeout,
        )

        if response.status_code not in (200, 201):
            raise ServiceNowAPIError(
                f"ServiceNow API returned {response.status_code}: {response.text[:500]}"
            )

        return response.json().get("result", {})
