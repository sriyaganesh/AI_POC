"""
Polls the connected ServiceNow PDI for newly created incidents and appends
each new incident found to a local output file (CSV or JSONL).

Which incidents have already been seen is persisted to a small JSON state
file between runs, so this can be invoked repeatedly -- e.g. from cron,
Windows Task Scheduler, or the --watch loop below -- without writing
duplicate entries.

Run once (e.g. from a scheduler):
    python -m servicenow_agent.incident_monitor

Run continuously, polling every 60 seconds:
    python -m servicenow_agent.incident_monitor --watch --interval 60

On the very first run (no state file yet) the currently existing incidents
are recorded as a baseline but NOT written to the output file, so the output
file only ever contains incidents created after monitoring started. Pass
--include-existing to dump the baseline to the output file too.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from .servicenow_client import (
    Incident,
    ServiceNowAPIError,
    ServiceNowClient,
    ServiceNowConfigError,
)

DEFAULT_OUTPUT = Path("new_incidents.csv")
DEFAULT_STATE = Path(".incident_monitor_state.json")

# Assignment group (case/spacing-insensitive) that routes an incident to Jira
# instead of ServiceDesk.
JIRA_ASSIGNMENT_GROUP = "POC_DATA"

CSV_FIELDS = [
    "detected_at",
    "number",
    "sys_id",
    "short_description",
    "state",
    "priority",
    "opened_at",
    "caller",
    "assignment_group",
    "category",
]


def categorize_incident(incident: Incident) -> str:
    """
    Categorize an incident based on its assignment group:
    - Assigned to the POC_DATA group -> "Jira"
    - Anything else -> "ServiceDesk"
    """
    group = (incident.assignment_group or "").strip().lower().replace(" ", "_")
    if group == JIRA_ASSIGNMENT_GROUP.lower():
        return "Jira"
    return "ServiceDesk"


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"seen_sys_ids": [], "initialized": False}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"seen_sys_ids": [], "initialized": False}


def save_state(state_path: Path, state: dict) -> None:
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def append_incidents(output_path: Path, incidents: list[Incident], fmt: str) -> None:
    if not incidents:
        return

    detected_at = datetime.now(timezone.utc).isoformat()

    if fmt == "jsonl":
        with output_path.open("a", encoding="utf-8") as f:
            for inc in incidents:
                row = asdict(inc)
                row["detected_at"] = detected_at
                row["category"] = categorize_incident(inc)
                f.write(json.dumps(row) + "\n")
        return

    # csv
    write_header = not output_path.exists() or output_path.stat().st_size == 0
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        for inc in incidents:
            row = asdict(inc)
            row["detected_at"] = detected_at
            row["category"] = categorize_incident(inc)
            writer.writerow(row)


def poll_once(
    client: ServiceNowClient,
    output_path: Path,
    state: dict,
    limit: int,
    fmt: str,
    include_existing_on_baseline: bool,
) -> list[Incident]:
    """
    Fetch the most recent incidents, diff against previously-seen sys_ids,
    write any new ones to the output file, and update state in place.

    Returns the list of newly detected incidents.
    """

    incidents = client.list_incidents(limit=limit, order_by_newest=True)

    seen: set[str] = set(state.get("seen_sys_ids", []))
    is_baseline_run = not state.get("initialized", False)

    new_incidents = [inc for inc in incidents if inc.sys_id and inc.sys_id not in seen]

    if is_baseline_run:
        # First run: record everything currently there as the baseline.
        # Only write to the output file if explicitly asked to.
        if not include_existing_on_baseline:
            written = []
        else:
            written = new_incidents
        state["initialized"] = True
    else:
        written = new_incidents

    if written:
        append_incidents(output_path, written, fmt)

    # Update seen set, bounded so the state file doesn't grow forever.
    all_ids = list(seen.union(inc.sys_id for inc in incidents if inc.sys_id))
    state["seen_sys_ids"] = all_ids[-2000:]
    state["last_checked"] = datetime.now(timezone.utc).isoformat()

    return written


def build_client() -> ServiceNowClient:
    load_dotenv()
    return ServiceNowClient()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Poll ServiceNow for new incidents and log them to a file."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output file to append new incidents to (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE,
        help=f"State file tracking already-seen incidents (default: {DEFAULT_STATE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="How many of the most recent incidents to inspect per poll (default: 50)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl"],
        default="csv",
        help="Output file format (default: csv)",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep polling in a loop instead of running once.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Seconds between polls when --watch is set (default: 60)",
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="On the first run, also write currently-existing incidents to the output file.",
    )
    args = parser.parse_args()

    try:
        client = build_client()
    except ServiceNowConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    state = load_state(args.state)

    def run_once() -> None:
        try:
            new_incidents = poll_once(
                client,
                args.output,
                state,
                args.limit,
                args.format,
                args.include_existing,
            )
        except ServiceNowAPIError as e:
            print(f"ServiceNow API error: {e}", file=sys.stderr)
            return
        finally:
            save_state(args.state, state)

        if new_incidents:
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"{len(new_incidents)} new incident(s) written to {args.output}:"
            )
            for inc in new_incidents:
                category = categorize_incident(inc)
                print(
                    f"  - {inc.number}: {inc.short_description or '(no description)'} "
                    f"[group={inc.assignment_group or 'n/a'} -> category={category}]"
                )
        else:
            print(f"[{datetime.now(timezone.utc).isoformat()}] No new incidents.")

    if not args.watch:
        run_once()
        return

    print(f"Watching for new incidents every {args.interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            run_once()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
