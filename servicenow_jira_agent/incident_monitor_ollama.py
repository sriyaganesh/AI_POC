"""
Polls the connected ServiceNow PDI for newly created incidents and appends
each new incident found to a local output file (CSV or JSONL).

This is a copy of incident_monitor.py that classifies incidents using a
locally-hosted Ollama model instead of OpenAI. Make sure Ollama is running
(https://ollama.com) and the desired model has been pulled, e.g.:

    ollama pull llama3.1

Which incidents have already been seen is persisted to a small JSON state
file between runs, so this can be invoked repeatedly -- e.g. from cron,
Windows Task Scheduler, or the --watch loop below -- without writing
duplicate entries.

Run once (e.g. from a scheduler):
    python -m servicenow_agent.incident_monitor_ollama

Run continuously, polling every 60 seconds:
    python -m servicenow_agent.incident_monitor_ollama --watch --interval 60

On the very first run (no state file yet) the currently existing incidents
are recorded as a baseline but NOT written to the output file, so the output
file only ever contains incidents created after monitoring started. Pass
--include-existing to dump the baseline to the output file too.

Configuration (via environment variables / .env):
    OLLAMA_MODEL    Name of the Ollama model to use (default: "qwen3:4b").
    OLLAMA_BASE_URL Base URL of the Ollama server (default: "http://localhost:11434").
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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

DEFAULT_OUTPUT = Path("new_incidents_ollama.csv")
DEFAULT_STATE = Path(".incident_monitor_ollama_state.json")

# Assignment group (case/spacing-insensitive) that routes an incident to Jira
# instead of ServiceDesk.
JIRA_ASSIGNMENT_GROUP = "POC_DATA"

CSV_FIELDS = [
    "detected_at",
    "number",
    "sys_id",
    "short_description",
    "description",
    "state",
    "priority",
    "opened_at",
    "caller",
    "assignment_group",
    "category",
]

# System prompt used to ask the LLM whether an incident (that isn't already
# routed to POC_DATA by assignment group) is a data issue and should still
# go to Jira, or is a general issue that belongs on ServiceDesk.

CLASSIFY_SYSTEM_PROMPT = """
You are triaging IT support incidents for routing.

Read the incident's short description and description and decide whether it
describes a DATA issue: e.g. incorrect, missing, duplicate, delayed,
corrupted, or inconsistent data; problems with a data pipeline, ETL job,
report, dashboard, database, or data quality/data feed. If it is a data
issue, respond with JIRA.

If it is a general IT/operational issue instead (hardware, access/password,
software install, network/connectivity, email, printer, application error
unrelated to data, etc.), respond with SERVICEDESK.

Respond with exactly one word, JIRA or SERVICEDESK, and nothing else.
""".strip()


# CLASSIFY_SYSTEM_PROMPT = """
# You are an IT incident triage classifier.

# Your task is to classify an incident as either JIRA or SERVICEDESK based only
# on the incident's short description and description.

# CLASSIFY AS JIRA when the incident is primarily related to DATA, including:
# - Incorrect, missing, duplicate, corrupted, inconsistent, or stale data
# - Data quality problems
# - Data feeds or data synchronization problems
# - ETL or data pipeline failures
# - Database/data processing problems
# - Reports or dashboards showing incorrect, missing, incomplete, or inconsistent data
# - Data transformation, mapping, ingestion, or processing issues
# - Problems where the underlying cause appears to be data-related

# CLASSIFY AS SERVICEDESK when the incident is primarily a general IT,
# operational, infrastructure, or user-support issue, including:
# - Password, account, authentication, or access problems
# - Hardware problems
# - Software installation or configuration
# - Network or connectivity problems
# - Email problems
# - Printer problems
# - General application errors or application availability problems
# - Server or infrastructure problems
# - VPN problems
# - User device problems
# - Other issues that do not primarily involve DATA

# IMPORTANT RULES:
# 1. Classify based on the PRIMARY problem described in the incident.
# 2. Do not classify an incident as JIRA merely because an application,
#    database, report, or dashboard is mentioned.
# 3. An application error is SERVICEDESK unless the incident indicates that
#    incorrect, missing, corrupted, inconsistent, delayed, or otherwise
#    problematic DATA is the primary issue.
# 4. If both an IT problem and a data problem are mentioned, classify as JIRA
#    only when the DATA problem is the primary issue.
# 5. If the description is ambiguous or there is insufficient evidence of a
#    data-related problem, classify as SERVICEDESK.
# 6. Do not infer a data problem that is not stated or reasonably supported
#    by the incident description.

# OUTPUT RULE:
# Return exactly one word:
# JIRA
# or
# SERVICEDESK

# Do not return an explanation, punctuation, JSON, markdown, or any other text.
# """.strip()


# Cache of LLM classification results for this process, keyed by the
# incident text, so the same incident text is never re-sent to the LLM
# twice (e.g. if it reappears across polls before its sys_id is recorded
# as seen, or if categorize_incident() is called directly outside the
# normal batch/write path).
_llm_category_cache: dict[str, str] = {}
_llm_client = None


def _get_llm():
    """
    Lazily build and cache the Ollama chat model used for classification.
    Only imported/instantiated when actually needed, so incidents that are
    already resolved by assignment group never require an Ollama call.
    """
    global _llm_client
    if _llm_client is None:
        from langchain_ollama import ChatOllama

        _llm_client = ChatOllama(
            model=os.getenv("OLLAMA_MODEL", "qwen3:4b"),
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            temperature=0,
            top_p=0.1,
            # Thinking models (e.g. qwen3) emit a hidden chain-of-thought
            # block before the answer. Turning reasoning off does NOT stop
            # the model from reasoning -- it just forces that same
            # chain-of-thought into the visible `content` field instead of
            # a separate one, so a small num_predict truncates the reply
            # before it ever reaches the actual JIRA/SERVICEDESK word and
            # everything silently falls back to ServiceDesk. Keeping
            # reasoning on routes the chain-of-thought to its own field so
            # `content` only ever holds the final one-word answer.
            reasoning=True,
            # Give the model enough budget to finish its chain-of-thought
            # before answering -- qwen3:4b needs several hundred tokens of
            # thinking for even a simple classification. Too low and the
            # response gets cut off mid-thought with no answer at all.
            num_predict=1024,
            # Keep the model resident between polls instead of letting
            # Ollama's default 5-minute idle timeout unload it, which would
            # otherwise cost a reload on the next classification.
            keep_alive="30m",
            # Fail fast instead of hanging the poll loop indefinitely if
            # Ollama is unreachable, still loading, or wedged.
            client_kwargs={"timeout": 30},
        )
    return _llm_client


def _classify_with_llm(short_description: str, description: str) -> str:
    """
    Ask the Ollama LLM whether an incident's short description/description
    describe a data issue ("Jira") or a general issue ("ServiceDesk").

    Never raises: if the LLM call fails for any reason (Ollama not running,
    model not pulled, network error, etc.) this logs a warning and falls
    back to "ServiceDesk" so monitoring keeps running.
    """
    short_description = (short_description or "").strip()
    description = (description or "").strip()

    cache_key = f"{short_description}\x1f{description}"
    if cache_key in _llm_category_cache:
        return _llm_category_cache[cache_key]

    if not short_description and not description:
        _llm_category_cache[cache_key] = "ServiceDesk"
        return "ServiceDesk"

    user_content = f"Short description: {short_description or '(none)'}"
    if description:
        user_content += f"\nDescription: {description}"

    try:
        llm = _get_llm()
        response = llm.invoke(
            [
                {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ]
        )
        answer = (response.content or "").strip().upper()
        category = "Jira" if "JIRA" in answer else "ServiceDesk"
    except Exception as e:  # noqa: BLE001 - never let LLM issues break monitoring
        print(
            f"Warning: LLM classification failed ({e}); defaulting to ServiceDesk. "
            "Is Ollama running and is the model pulled (`ollama pull <model>`)?",
            file=sys.stderr,
        )
        category = "ServiceDesk"

    _llm_category_cache[cache_key] = category
    return category


def categorize_incident(incident: Incident) -> str:
    """
    Categorize an incident:
    1. Assigned to the POC_DATA group -> "Jira".
    2. Otherwise, ask an LLM (Ollama) to read the short description and
       description and decide whether it's a data issue ("Jira") or a
       general issue ("ServiceDesk").
    """
    group = (incident.assignment_group or "").strip().lower().replace(" ", "_")
    if group == JIRA_ASSIGNMENT_GROUP.lower():
        return "Jira"
    return _classify_with_llm(incident.short_description, incident.description)


def categorize_incidents(incidents: list[Incident]) -> dict[str, str]:
    """
    Categorize a batch of incidents, keyed by sys_id.

    Incidents already resolved by assignment group are answered immediately
    without touching the LLM. The rest are classified concurrently (Ollama
    can serve multiple requests in parallel -- see OLLAMA_NUM_PARALLEL)
    instead of one sequential round trip per incident, so a poll that turns
    up several new incidents at once isn't gated on N x (LLM latency).
    """
    results: dict[str, str] = {}
    to_classify: list[Incident] = []
    for inc in incidents:
        group = (inc.assignment_group or "").strip().lower().replace(" ", "_")
        if group == JIRA_ASSIGNMENT_GROUP.lower():
            results[inc.sys_id] = "Jira"
        else:
            to_classify.append(inc)

    if to_classify:
        with ThreadPoolExecutor(max_workers=min(4, len(to_classify))) as pool:
            futures = {
                inc.sys_id: pool.submit(
                    _classify_with_llm, inc.short_description, inc.description
                )
                for inc in to_classify
            }
            for sys_id, fut in futures.items():
                results[sys_id] = fut.result()

    return results


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"seen_sys_ids": [], "initialized": False}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"seen_sys_ids": [], "initialized": False}


def save_state(state_path: Path, state: dict) -> None:
    state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def append_incidents(
    output_path: Path, incidents: list[Incident], fmt: str
) -> dict[str, str]:
    """
    Write new incidents to the output file and return their categories
    (keyed by sys_id), classifying the whole batch once up front so callers
    don't need to re-classify the same incidents again afterwards.
    """
    if not incidents:
        return {}

    categories = categorize_incidents(incidents)
    detected_at = datetime.now(timezone.utc).isoformat()

    if fmt == "jsonl":
        with output_path.open("a", encoding="utf-8") as f:
            for inc in incidents:
                row = asdict(inc)
                row["detected_at"] = detected_at
                row["category"] = categories[inc.sys_id]
                f.write(json.dumps(row) + "\n")
        return categories

    # csv
    write_header = not output_path.exists() or output_path.stat().st_size == 0
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        for inc in incidents:
            row = asdict(inc)
            row["detected_at"] = detected_at
            row["category"] = categories[inc.sys_id]
            writer.writerow(row)

    return categories


def poll_once(
    client: ServiceNowClient,
    output_path: Path,
    state: dict,
    limit: int,
    fmt: str,
    include_existing_on_baseline: bool,
) -> tuple[list[Incident], dict[str, str]]:
    """
    Fetch the most recent incidents, diff against previously-seen sys_ids,
    write any new ones to the output file, and update state in place.

    Returns the list of newly detected incidents, along with their
    categories (keyed by sys_id) so callers don't need to re-classify them.
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

    categories = append_incidents(output_path, written, fmt) if written else {}

    # Update seen set, bounded so the state file doesn't grow forever.
    all_ids = list(seen.union(inc.sys_id for inc in incidents if inc.sys_id))
    state["seen_sys_ids"] = all_ids[-2000:]
    state["last_checked"] = datetime.now(timezone.utc).isoformat()

    return written, categories


def build_client() -> ServiceNowClient:
    load_dotenv()
    return ServiceNowClient()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Poll ServiceNow for new incidents and log them to a file "
        "(classification via a local Ollama model)."
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
            new_incidents, categories = poll_once(
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
                category = categories[inc.sys_id]
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
