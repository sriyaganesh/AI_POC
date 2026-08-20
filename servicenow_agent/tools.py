"""
LangChain tool(s) exposed to the agent.
"""

from __future__ import annotations

from typing import Optional

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from .incident_monitor import categorize_incident
from .servicenow_client import ServiceNowAPIError, ServiceNowClient, ServiceNowConfigError

# ServiceNow's numeric incident state codes, for the agent's reference.
STATE_CODES = {
    "1": "New",
    "2": "In Progress",
    "3": "On Hold",
    "6": "Resolved",
    "7": "Closed",
    "8": "Canceled",
}


class ListIncidentsInput(BaseModel):
    limit: int = Field(default=10, description="Max number of incidents to return.")
    state: Optional[str] = Field(
        default=None,
        description=(
            "Filter by incident state code: 1=New, 2=In Progress, 3=On Hold, "
            "6=Resolved, 7=Closed, 8=Canceled. Leave empty for all states."
        ),
    )
    query: Optional[str] = Field(
        default=None,
        description=(
            "Optional raw ServiceNow encoded query (sysparm_query) for advanced "
            "filtering, e.g. 'priority=1^active=true'. Leave empty if not needed."
        ),
    )


@tool("list_servicenow_incidents", args_schema=ListIncidentsInput)
def list_servicenow_incidents(
    limit: int = 10,
    state: Optional[str] = None,
    query: Optional[str] = None,
) -> str:
    """
    List incidents created in the connected ServiceNow PDI, newest first.
    Use this whenever the user asks about incidents, tickets, or issues in ServiceNow.
    """
    try:
        client = ServiceNowClient()
        incidents = client.list_incidents(limit=limit, state=state, query=query)
    except ServiceNowConfigError as e:
        return f"Configuration error: {e}"
    except ServiceNowAPIError as e:
        return f"ServiceNow API error: {e}"
    except Exception as e:  # noqa: BLE001 - surface any unexpected failure to the agent
        return f"Unexpected error while contacting ServiceNow: {e}"

    if not incidents:
        return "No incidents found matching the given criteria."

    lines = [f"Found {len(incidents)} incident(s):"]
    for inc in incidents:
        state_label = STATE_CODES.get(inc.state, inc.state or "Unknown")
        category = categorize_incident(inc)
        lines.append(
            f"- {inc.number} | {inc.short_description or '(no description)'} "
            f"| state={state_label} | priority={inc.priority} "
            f"| opened={inc.opened_at} | caller={inc.caller or 'n/a'} "
            f"| assignment_group={inc.assignment_group or 'n/a'} | category={category}"
        )
    return "\n".join(lines)


TOOLS = [list_servicenow_incidents]
