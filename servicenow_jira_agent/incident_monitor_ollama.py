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

Only incidents that are still in the ServiceNow "New" state (i.e. newly
created and not yet actioned) are fetched from ServiceNow -- see
NEW_INCIDENT_STATE / --state below. Incidents already picked up in a
previous poll are additionally filtered out locally via the state file, so
none are ever written to the output file twice.

On the very first run (no state file yet) the currently existing incidents
are recorded as a baseline but NOT written to the output file, so the output
file only ever contains incidents created after monitoring started. Pass
--include-existing to dump the baseline to the output file too.

The Jira-vs-ServiceDesk classification for incidents not already routed by
assignment group combines two signals:
  1. Keyword matching against the short/long description, using the
     user-editable keyword list in data_keywords.yaml (see --keywords),
     which includes both general data terms and data/ETL tool names
     (Informatica, Talend, Airflow, Snowflake, Power BI, etc.).
  2. LLM (Ollama) judgement, which is given the keyword matches (if any) as
     supporting evidence and always reads the full incident text to make
     the final call -- if NO keyword matches, the LLM still classifies the
     incident based on its own reading of the description rather than
     defaulting to ServiceDesk.

Incidents that classify as "Jira" (see categorize_incident() above) are
automatically routed to Jira: a Jira issue is created from the incident's
details (via JIRA_URL/JIRA_EMAIL/JIRA_API_TOKEN/JIRA_PROJECT -- see
jira_client.py), and on success the ServiceNow incident's state is
transitioned from New to In Progress (SERVICENOW_IN_PROGRESS_STATE) with a
work note recording the new Jira key. Pass --no-jira to skip this and only
classify/log incidents.

Configuration (via environment variables / .env):
    OLLAMA_MODEL                Name of the Ollama model to use (default: "qwen3:4b").
    OLLAMA_BASE_URL             Base URL of the Ollama server (default: "http://localhost:11434").
    SERVICENOW_NEW_STATE        ServiceNow incident "state" value considered "new" (default: "1").
    SERVICENOW_IN_PROGRESS_STATE ServiceNow incident "state" value to set after a
                                 Jira ticket is created for it (default: "2").
    DATA_KEYWORDS_FILE          Path to the data-related keywords YAML file
                                 (default: data_keywords.yaml next to this script).
    JIRA_URL, JIRA_EMAIL,
    JIRA_API_TOKEN, JIRA_PROJECT Jira Cloud connection settings used to create
                                 tickets for incidents classified as "Jira".
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

import yaml
from dotenv import load_dotenv

from .jira_client import JiraAPIError, JiraClient, JiraConfigError
from .llm_usage_tracker import LLMUsageTracker
from .servicenow_client import (
    Incident,
    ServiceNowAPIError,
    ServiceNowClient,
    ServiceNowConfigError,
)

DEFAULT_OUTPUT = Path("new_incidents_ollama.csv")
DEFAULT_STATE = Path(".incident_monitor_ollama_state.json")

# ServiceNow incident "state" value considered "New" (i.e. just created and
# not yet actioned). Standard ServiceNow incident states are 1=New,
# 2=In Progress, 3=On Hold, 6=Resolved, 7=Closed, 8=Canceled -- override via
# SERVICENOW_NEW_STATE / --state if this instance customizes state values.
NEW_INCIDENT_STATE = os.getenv("SERVICENOW_NEW_STATE", "1")

# ServiceNow incident "state" value to set once an incident has been
# routed to Jira (i.e. a Jira ticket was successfully created for it).
# Standard ServiceNow value is 2=In Progress.
IN_PROGRESS_INCIDENT_STATE = os.getenv("SERVICENOW_IN_PROGRESS_STATE", "2")

# User-editable YAML file listing keywords that indicate a DATA-related
# incident. See data_keywords.yaml for the template/instructions.
DEFAULT_KEYWORDS_FILE = Path(
    os.getenv("DATA_KEYWORDS_FILE", str(Path(__file__).resolve().parent / "data_keywords.yaml"))
)

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
    "matched_keywords",
    "jira_key",
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

You may also be given a "Keyword hint" listing configured data-related
keywords (including names of data/ETL tools and platforms) that were found
in the incident text. Treat a match as supporting evidence only, not as the
final answer: a keyword can appear in a non-data context (e.g. "database
server is unreachable" is an infrastructure/hardware issue, not a data
issue), so always judge based on what the incident is actually about.

If the hint says no configured keywords were found, do NOT default to
SERVICEDESK just because of that. The keyword list is only a helper and does
not cover every way a data issue can be worded or every data tool that
exists. Read the short description and description carefully and use your
own judgment: if the incident is fundamentally about incorrect, missing,
duplicate, corrupted, delayed, or inconsistent data, or a problem with a
data pipeline, ETL/data tool, report, or database, classify it as JIRA even
if no keyword matched. Only classify as SERVICEDESK when the incident is
genuinely a general IT/operational issue unrelated to data.

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


# Cache of loaded keyword files, keyed by resolved path, as
# (mtime, keywords) so data_keywords.yaml is only re-read/re-parsed when it
# actually changes on disk -- edits made while --watch is running take
# effect on the very next poll with no restart needed.
_keywords_cache: dict[Path, tuple[float, list[str]]] = {}


def load_data_keywords(path: Path = DEFAULT_KEYWORDS_FILE) -> list[str]:
    """
    Load the user-editable list of data-related keywords from a YAML file
    (see data_keywords.yaml for the expected format/instructions).

    Never raises: a missing, empty, or malformed file logs a warning and
    yields an empty keyword list, so incidents fall back to being
    classified by LLM judgement alone.
    """
    path = Path(path)
    try:
        mtime = path.stat().st_mtime
    except OSError:
        print(
            f"Warning: keyword file not found at {path}; proceeding with no "
            "keyword hints (LLM-only classification).",
            file=sys.stderr,
        )
        return []

    cached = _keywords_cache.get(path)
    if cached and cached[0] == mtime:
        return cached[1]

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        raw_keywords = data.get("keywords") or []
        keywords = [str(k).strip() for k in raw_keywords if str(k).strip()]
    except (yaml.YAMLError, OSError) as e:
        print(
            f"Warning: could not read keyword file {path} ({e}); proceeding "
            "with no keyword hints (LLM-only classification).",
            file=sys.stderr,
        )
        keywords = []

    _keywords_cache[path] = (mtime, keywords)
    return keywords


def find_matched_keywords(
    short_description: str, description: str, keywords: list[str]
) -> list[str]:
    """
    Return the configured keywords (in configured order, de-duplicated) that
    appear as a substring of the incident's short description or
    description, case-insensitively.
    """
    text = f"{short_description or ''} {description or ''}".lower()
    matched: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower and kw_lower in text and kw_lower not in seen:
            matched.append(kw)
            seen.add(kw_lower)
    return matched


# Cache of LLM classification results for this process, keyed by the
# incident text and matched keywords, so the same incident is never re-sent
# to the LLM twice (e.g. if it reappears across polls before its sys_id is
# recorded as seen, or if categorize_incident() is called directly outside
# the normal batch/write path).
_llm_category_cache: dict[str, str] = {}
_llm_client = None

# Records every LLM call made for classification (tokens used, latency,
# success/failure) to a local JSONL log and this process's in-memory
# session, so usage can be summarized -- see --usage-summary / the
# session summary printed after each run, or run
# `python -m servicenow_jira_agent.llm_usage_tracker` any time.
llm_usage_tracker = LLMUsageTracker()


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


def _classify_with_llm(
    short_description: str, description: str, matched_keywords: list[str] | None = None
) -> str:
    """
    Ask the Ollama LLM whether an incident's short description/description
    describe a data issue ("Jira") or a general issue ("ServiceDesk").

    matched_keywords, if any, are keywords from the configurable
    data_keywords.yaml file that were found in the incident text; they are
    passed to the LLM as supporting evidence, but the LLM still makes the
    final call by reading the full incident text (a keyword can appear in a
    non-data context, e.g. "database server is unreachable" is an
    infrastructure issue, not a data issue).

    Never raises: if the LLM call fails for any reason (Ollama not running,
    model not pulled, network error, etc.) this logs a warning and falls
    back to "ServiceDesk" so monitoring keeps running.
    """
    short_description = (short_description or "").strip()
    description = (description or "").strip()
    matched_keywords = matched_keywords or []

    cache_key = f"{short_description}\x1f{description}\x1f{','.join(matched_keywords)}"
    if cache_key in _llm_category_cache:
        return _llm_category_cache[cache_key]

    if not short_description and not description:
        _llm_category_cache[cache_key] = "ServiceDesk"
        return "ServiceDesk"

    user_content = f"Short description: {short_description or '(none)'}"
    if description:
        user_content += f"\nDescription: {description}"
    if matched_keywords:
        user_content += (
            "\nKeyword hint: the following configured data-related keywords "
            f"were found in the text above: {', '.join(matched_keywords)}. "
            "Treat this as supporting evidence, not as a final answer -- "
            "still decide based on what the incident is actually about."
        )
    else:
        user_content += (
            "\nKeyword hint: none of the configured data-related keywords "
            "were found in the text above. This does not automatically mean "
            "SERVICEDESK -- read the text above yourself and use your own "
            "judgment to decide whether it is nonetheless a data issue."
        )

    try:
        llm = _get_llm()
        model_name = os.getenv("OLLAMA_MODEL", "qwen3:4b")
        with llm_usage_tracker.track(model=model_name, purpose="classify_incident") as call:
            response = llm.invoke(
                [
                    {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ]
            )
            call.set_response(response)
            answer = (response.content or "").strip().upper()
            category = "Jira" if "JIRA" in answer else "ServiceDesk"
            call.result = category
    except Exception as e:  # noqa: BLE001 - never let LLM issues break monitoring
        print(
            f"Warning: LLM classification failed ({e}); defaulting to ServiceDesk. "
            "Is Ollama running and is the model pulled (`ollama pull <model>`)?",
            file=sys.stderr,
        )
        category = "ServiceDesk"

    _llm_category_cache[cache_key] = category
    return category


def categorize_incident(
    incident: Incident, keywords: list[str] | None = None
) -> tuple[str, list[str]]:
    """
    Categorize an incident, returning (category, matched_keywords):
    1. Assigned to the POC_DATA group -> ("Jira", []).
    2. Otherwise, match the short description and description against the
       configurable data-related keyword list, then ask an LLM (Ollama) to
       decide -- using those keyword matches as supporting evidence --
       whether the incident is a data issue ("Jira") or a general issue
       ("ServiceDesk").
    """
    group = (incident.assignment_group or "").strip().lower().replace(" ", "_")
    if group == JIRA_ASSIGNMENT_GROUP.lower():
        return "Jira", []

    keywords = keywords if keywords is not None else load_data_keywords()
    matched = find_matched_keywords(
        incident.short_description, incident.description, keywords
    )
    category = _classify_with_llm(
        incident.short_description, incident.description, matched
    )
    return category, matched


def categorize_incidents(
    incidents: list[Incident], keywords: list[str] | None = None
) -> dict[str, tuple[str, list[str]]]:
    """
    Categorize a batch of incidents, keyed by sys_id, returning
    (category, matched_keywords) per incident.

    Incidents already resolved by assignment group are answered immediately
    without touching the keyword list or the LLM. The rest are matched
    against the configurable data-related keyword list and then classified
    concurrently (Ollama can serve multiple requests in parallel -- see
    OLLAMA_NUM_PARALLEL) instead of one sequential round trip per incident,
    so a poll that turns up several new incidents at once isn't gated on
    N x (LLM latency).
    """
    keywords = keywords if keywords is not None else load_data_keywords()

    results: dict[str, tuple[str, list[str]]] = {}
    to_classify: list[tuple[Incident, list[str]]] = []
    for inc in incidents:
        group = (inc.assignment_group or "").strip().lower().replace(" ", "_")
        if group == JIRA_ASSIGNMENT_GROUP.lower():
            results[inc.sys_id] = ("Jira", [])
        else:
            matched = find_matched_keywords(
                inc.short_description, inc.description, keywords
            )
            to_classify.append((inc, matched))

    if to_classify:
        with ThreadPoolExecutor(max_workers=min(4, len(to_classify))) as pool:
            futures = {
                inc.sys_id: (
                    pool.submit(
                        _classify_with_llm,
                        inc.short_description,
                        inc.description,
                        matched,
                    ),
                    matched,
                )
                for inc, matched in to_classify
            }
            for sys_id, (fut, matched) in futures.items():
                results[sys_id] = (fut.result(), matched)

    return results


# Lazily built/cached Jira client -- see _get_jira_client().
_jira_client: JiraClient | None = None


def _get_jira_client() -> JiraClient:
    """
    Lazily build and cache the Jira client used to create tickets. Only
    instantiated when an incident actually needs to be routed to Jira, so
    ServiceDesk-only runs never require Jira credentials to be configured.
    """
    global _jira_client
    if _jira_client is None:
        _jira_client = JiraClient()
    return _jira_client


def _build_jira_fields(incident: Incident) -> tuple[str, str]:
    """
    Build the (summary, description) to use for the Jira ticket created
    from a ServiceNow incident, carrying over its key details.
    """
    summary = f"[{incident.number}] {incident.short_description or 'ServiceNow incident'}"

    lines = [
        f"ServiceNow incident: {incident.number}",
        f"Priority: {incident.priority or 'n/a'}",
        f"Caller: {incident.caller or 'n/a'}",
        f"Assignment group: {incident.assignment_group or 'n/a'}",
        f"Opened at: {incident.opened_at or 'n/a'}",
        "",
        "Description:",
        incident.description or incident.short_description or "(no description provided)",
    ]
    description = "\n".join(lines)
    return summary, description


def create_jira_ticket(incident: Incident) -> str | None:
    """
    Create a Jira ticket from a ServiceNow incident's details and return its
    key (e.g. "SCRUM-42"), or None if the ticket could not be created.

    Never raises: if Jira isn't configured or the API call fails for any
    reason, this logs a warning and returns None so the poll loop keeps
    running (the incident is left as-is in ServiceNow for a later retry).
    """
    summary, description = _build_jira_fields(incident)
    try:
        jira = _get_jira_client()
        issue = jira.create_issue(summary=summary, description=description, issue_type="Task")
        return issue.key
    except JiraConfigError as e:
        print(f"Warning: Jira is not configured ({e}); skipping ticket creation.", file=sys.stderr)
    except JiraAPIError as e:
        print(
            f"Warning: failed to create Jira ticket for {incident.number} ({e}).",
            file=sys.stderr,
        )
    except Exception as e:  # noqa: BLE001 - never let Jira issues break monitoring
        print(
            f"Warning: unexpected error creating Jira ticket for {incident.number} ({e}).",
            file=sys.stderr,
        )
    return None


def route_incidents_to_jira(
    client: ServiceNowClient,
    incidents: list[Incident],
    categories: dict[str, tuple[str, list[str]]],
) -> dict[str, str]:
    """
    For every incident classified as "Jira", create a Jira ticket from its
    details and, on success, transition the ServiceNow incident from New to
    In Progress (IN_PROGRESS_INCIDENT_STATE) with a work note recording the
    new Jira key.

    Returns the created Jira key per incident (keyed by sys_id); incidents
    that aren't classified as "Jira", or for which ticket creation/the
    ServiceNow update failed, are simply absent from the result.
    """
    jira_keys: dict[str, str] = {}
    for inc in incidents:
        category, _matched = categories.get(inc.sys_id, ("ServiceDesk", []))
        if category != "Jira":
            continue

        jira_key = create_jira_ticket(inc)
        if not jira_key:
            continue
        jira_keys[inc.sys_id] = jira_key

        try:
            client.update_incident(
                inc.sys_id,
                {
                    "state": IN_PROGRESS_INCIDENT_STATE,
                    "work_notes": f"Routed to Jira ticket {jira_key} by incident_monitor_ollama.",
                },
            )
        except ServiceNowAPIError as e:
            print(
                f"Warning: created Jira ticket {jira_key} for {inc.number}, but failed to "
                f"update its ServiceNow state to In Progress ({e}).",
                file=sys.stderr,
            )
        except Exception as e:  # noqa: BLE001 - never let ServiceNow issues break monitoring
            print(
                f"Warning: created Jira ticket {jira_key} for {inc.number}, but an unexpected "
                f"error occurred updating its ServiceNow state ({e}).",
                file=sys.stderr,
            )

    return jira_keys


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
    output_path: Path,
    incidents: list[Incident],
    fmt: str,
    keywords: list[str] | None = None,
    categories: dict[str, tuple[str, list[str]]] | None = None,
    jira_keys: dict[str, str] | None = None,
) -> dict[str, tuple[str, list[str]]]:
    """
    Write new incidents to the output file and return their
    (category, matched_keywords) (keyed by sys_id).

    If categories is not already provided (e.g. by a caller that also needs
    it to route incidents to Jira before writing), the whole batch is
    classified here. jira_keys, if provided, is the Jira ticket key created
    per sys_id (see route_incidents_to_jira), recorded in the "jira_key"
    output column.
    """
    if not incidents:
        return {}

    categories = categories if categories is not None else categorize_incidents(incidents, keywords)
    jira_keys = jira_keys or {}
    detected_at = datetime.now(timezone.utc).isoformat()

    if fmt == "jsonl":
        with output_path.open("a", encoding="utf-8") as f:
            for inc in incidents:
                category, matched = categories[inc.sys_id]
                row = asdict(inc)
                row["detected_at"] = detected_at
                row["category"] = category
                row["matched_keywords"] = matched
                row["jira_key"] = jira_keys.get(inc.sys_id, "")
                f.write(json.dumps(row) + "\n")
        return categories

    # csv
    write_header = not output_path.exists() or output_path.stat().st_size == 0
    with output_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        for inc in incidents:
            category, matched = categories[inc.sys_id]
            row = asdict(inc)
            row["detected_at"] = detected_at
            row["category"] = category
            row["matched_keywords"] = "; ".join(matched)
            row["jira_key"] = jira_keys.get(inc.sys_id, "")
            writer.writerow(row)

    return categories


def poll_once(
    client: ServiceNowClient,
    output_path: Path,
    state: dict,
    limit: int,
    fmt: str,
    include_existing_on_baseline: bool,
    new_state: str | None = NEW_INCIDENT_STATE,
    keywords_path: Path = DEFAULT_KEYWORDS_FILE,
    route_to_jira: bool = True,
) -> tuple[list[Incident], dict[str, tuple[str, list[str]]], dict[str, str]]:
    """
    Fetch only newly-created incidents (ServiceNow state == new_state, e.g.
    "New"), diff against previously-seen sys_ids, classify them, route any
    "Jira" incidents to Jira (creating a ticket and moving the incident to
    In Progress -- see route_incidents_to_jira), write any new ones to the
    output file, and update state in place.

    Returns the list of newly detected incidents, their
    (category, matched_keywords) (keyed by sys_id), and the Jira key created
    per sys_id (only present for incidents successfully routed to Jira), so
    callers don't need to re-classify or re-route them.
    """

    incidents = client.list_incidents(
        limit=limit, order_by_newest=True, state=new_state
    )

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

    keywords = load_data_keywords(keywords_path)

    jira_keys: dict[str, str] = {}
    if written:
        categories = categorize_incidents(written, keywords)
        if route_to_jira:
            jira_keys = route_incidents_to_jira(client, written, categories)
        append_incidents(output_path, written, fmt, keywords, categories, jira_keys)
    else:
        categories = {}

    # Update seen set, bounded so the state file doesn't grow forever.
    all_ids = list(seen.union(inc.sys_id for inc in incidents if inc.sys_id))
    state["seen_sys_ids"] = all_ids[-2000:]
    state["last_checked"] = datetime.now(timezone.utc).isoformat()

    return written, categories, jira_keys


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
    parser.add_argument(
        "--keywords",
        type=Path,
        default=DEFAULT_KEYWORDS_FILE,
        help="Path to the data-related keywords YAML file "
        f"(default: {DEFAULT_KEYWORDS_FILE})",
    )
    parser.add_argument(
        "--incident-state",
        default=NEW_INCIDENT_STATE,
        help="ServiceNow incident 'state' value to restrict polling to "
        f'(default: "{NEW_INCIDENT_STATE}", i.e. New). Use --all-states to '
        "disable this filter.",
    )
    parser.add_argument(
        "--all-states",
        action="store_true",
        help="Poll incidents in any state instead of only newly-created ones.",
    )
    parser.add_argument(
        "--usage-summary",
        action="store_true",
        help="Print a summary of all LLM calls logged so far (tokens used, "
        "latency, success/failure) and exit without polling ServiceNow.",
    )
    parser.add_argument(
        "--no-jira",
        action="store_true",
        help="Classify and log incidents but skip creating Jira tickets / "
        "transitioning ServiceNow incidents to In Progress.",
    )
    args = parser.parse_args()

    if args.usage_summary:
        from .llm_usage_tracker import load_records, print_summary, summarize

        records = load_records(llm_usage_tracker.log_path)
        print_summary(summarize(records), title=f"LLM usage summary ({llm_usage_tracker.log_path})")
        return

    new_state = None if args.all_states else args.incident_state

    try:
        client = build_client()
    except ServiceNowConfigError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    state = load_state(args.state)

    def run_once() -> None:
        try:
            new_incidents, categories, jira_keys = poll_once(
                client,
                args.output,
                state,
                args.limit,
                args.format,
                args.include_existing,
                new_state,
                args.keywords,
                not args.no_jira,
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
                category, matched = categories[inc.sys_id]
                keyword_note = f", keywords={matched}" if matched else ""
                jira_key = jira_keys.get(inc.sys_id)
                jira_note = (
                    f" -> Jira {jira_key} created, incident set to In Progress"
                    if jira_key
                    else ""
                )
                print(
                    f"  - {inc.number}: {inc.short_description or '(no description)'} "
                    f"[group={inc.assignment_group or 'n/a'} -> category={category}{keyword_note}]"
                    f"{jira_note}"
                )
        else:
            print(f"[{datetime.now(timezone.utc).isoformat()}] No new incidents.")

    if not args.watch:
        run_once()
        llm_usage_tracker.print_summary()
        return

    print(f"Watching for new incidents every {args.interval}s. Press Ctrl+C to stop.")
    try:
        while True:
            run_once()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        llm_usage_tracker.print_summary()


if __name__ == "__main__":
    main()
