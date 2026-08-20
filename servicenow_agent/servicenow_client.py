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
                "number,sys_id,short_description,state,priority,opened_at,"
                "caller_id,assignment_group"
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
