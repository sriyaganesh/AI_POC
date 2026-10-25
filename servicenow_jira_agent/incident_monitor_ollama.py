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

Polling is incremental: after the baseline poll, each poll asks ServiceNow
only for incidents opened at/after the cursor recorded in the state file
(state["last_opened_at"]), rather than re-fetching and re-diffing the same
--limit newest incidents every time. Every --full-sweep-interval-th poll
instead does a full re-fetch, which is what catches an incident whose state
was reset back to New after already being processed (see
reconcile_reverted_incidents() below) -- see --full-sweep-interval.

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

If an incident's ServiceNow state is later changed back to New by mistake
(e.g. accidentally reset by a user) after already being routed, the next
poll detects it re-appearing among "New" incidents despite already being
recorded as seen, confirms a Jira ticket already exists for it (without
creating a duplicate), and restores its ServiceNow state to In Progress
with an explanatory work note -- see reconcile_reverted_incidents().

Configuration (via environment variables / .env):
    OLLAMA_MODEL                Name of the Ollama model to use (default: "qwen3:4b").
    OLLAMA_BASE_URL             Base URL of the Ollama server (default: "http://localhost:11434").
    SERVICENOW_NEW_STATE        ServiceNow incident "state" value considered "new" (default: "1").
    SERVICENOW_IN_PROGRESS_STATE ServiceNow incident "state" value to set after a
                                 Jira ticket is created for it (default: "2").
    DATA_KEYWORDS_FILE          Path to the data-related keywords YAML file
                                 (default: data_keywords.yaml next to this script).
    OLLAMA_TIMEOUT_SECONDS       Seconds to wait for a single classification call
                                 before giving up and defaulting to ServiceDesk
                                 (default: "180"; qwen3:4b's own successful calls
                                 routinely take 30-150s, see llm_usage_log.jsonl).
    OLLAMA_NUM_PREDICT           Max tokens (reasoning + answer) per classification
                                 call; too low truncates the model before it emits
                                 JIRA/SERVICEDESK, which also defaults to
                                 ServiceDesk (default: "2048").
    OLLAMA_MAX_CONCURRENT_CALLS  Max incidents classified concurrently against
                                 Ollama; higher values speed up large batches but
                                 can starve a single local model instance into
                                 timing out more often (default: "2").
    JIRA_URL, JIRA_EMAIL,
    JIRA_API_TOKEN, JIRA_PROJECT Jira Cloud connection settings used to create
                                 tickets for incidents classified as "Jira".
    SERVICENOW_DATA_GROUP_SYS_ID,
    SERVICENOW_SERVICEDESK_GROUP_SYS_ID
                                 Optional sys_ids of the POC_DATA /
                                 ServiceDesk groups, for when the
                                 API user can't look groups up by name.
    JIRA_ASSIGNEE               Jira user newly created tickets are assigned to
                                 (default: "sglearn"; empty to leave unassigned).
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

# Assignment group that incidents classified as "ServiceDesk" are assigned to
# in ServiceNow (see route_incidents_to_servicedesk()).
SERVICE_DESK_ASSIGNMENT_GROUP = "ServiceDesk"

# Jira user (username, display name or email) that newly created Jira
# tickets are assigned to -- see _assign_jira_ticket(). Set JIRA_ASSIGNEE to
# an empty string to leave tickets unassigned.
JIRA_ASSIGNEE = os.getenv("JIRA_ASSIGNEE", "sglearn")

# Maps a ServiceNow incident priority's leading number (1=Critical..5=Planning,
# the standard out-of-the-box scale) to the equivalent Jira priority name, so
# a created Jira ticket's own Priority field reflects how urgent the
# ServiceNow incident actually was instead of silently defaulting to
# whatever Jira picks on its own (usually "Medium") -- see
# _map_servicenow_priority() / create_jira_ticket().
SERVICENOW_TO_JIRA_PRIORITY = {
    "1": "Highest",
    "2": "High",
    "3": "Medium",
    "4": "Low",
    "5": "Lowest",
}

# How long to wait for a single Ollama classification call before giving up
# and falling back to ServiceDesk (see _classify_with_llm's except-block).
# qwen3:4b's chain-of-thought reasoning routinely takes 30-150s on modest
# hardware (see llm_usage_log.jsonl) -- the old 30s default was shorter than
# most *successful* calls, so calls were timing out -- and silently
# defaulting to ServiceDesk -- far more often than the model was actually
# choosing ServiceDesk on the merits. Override via OLLAMA_TIMEOUT_SECONDS.
OLLAMA_TIMEOUT_SECONDS = int(os.getenv("OLLAMA_TIMEOUT_SECONDS", "180"))

# Max tokens of chain-of-thought + answer the model may generate per
# classification call. Too low and a genuinely tricky case can get cut off
# mid-thought before ever emitting the JIRA/SERVICEDESK word, which (per
# categorize_incident's "JIRA" in answer check) also falls back to
# ServiceDesk -- see the reasoning note in _get_llm() below. Override via
# OLLAMA_NUM_PREDICT.
OLLAMA_NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT", "2048"))

# Max number of incidents classified concurrently against the local Ollama
# server. Firing several classification requests at once at a single small
# local model instance makes each one slower and more likely to time out
# (see llm_usage_log.jsonl, where batches of concurrent calls show far more
# timeouts than isolated ones) -- lowering this trades poll-batch latency
# for reliability. Override via OLLAMA_MAX_CONCURRENT_CALLS.
OLLAMA_MAX_CONCURRENT_CALLS = int(os.getenv("OLLAMA_MAX_CONCURRENT_CALLS", "2"))

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
            num_predict=OLLAMA_NUM_PREDICT,
            # Keep the model resident between polls instead of letting
            # Ollama's default 5-minute idle timeout unload it, which would
            # otherwise cost a reload on the next classification.
            keep_alive="30m",
            # Fail fast if Ollama is unreachable or well and truly wedged,
            # but give it enough rope to actually finish first -- qwen3:4b's
            # own successful calls routinely take 30-150s (see
            # llm_usage_log.jsonl), so a short timeout here doesn't catch
            # only genuine hangs, it also aborts normal slow calls and
            # silently defaults them to ServiceDesk.
            client_kwargs={"timeout": OLLAMA_TIMEOUT_SECONDS},
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
        with ThreadPoolExecutor(
            max_workers=min(OLLAMA_MAX_CONCURRENT_CALLS, len(to_classify))
        ) as pool:
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

# Cached accountId of JIRA_ASSIGNEE -- see _assign_jira_ticket().
_jira_assignee_account_id: str | None = None


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


def _map_servicenow_priority(priority: str) -> str | None:
    """
    Map a ServiceNow incident priority display value (e.g. "1 - Critical",
    "4 - Low") to the equivalent Jira priority name (see
    SERVICENOW_TO_JIRA_PRIORITY).

    Returns None if the priority is blank or unrecognized, so the caller can
    create the ticket without touching Jira's Priority field rather than
    guessing at a value -- better to leave it at Jira's own default than to
    set it to something wrong.
    """
    priority = (priority or "").strip()
    if not priority:
        return None

    # ServiceNow's display value is typically "<n> - <label>" -- the leading
    # digit is the reliable, locale-independent part.
    leading_digit = priority.split(" ", 1)[0].strip()
    if leading_digit in SERVICENOW_TO_JIRA_PRIORITY:
        return SERVICENOW_TO_JIRA_PRIORITY[leading_digit]

    # Fall back to matching the label text, in case priority came through as
    # something else (e.g. just "Critical", or display values weren't used).
    label = priority.lower()
    if "critical" in label or "highest" in label:
        return "Highest"
    if "moderate" in label or "medium" in label:
        return "Medium"
    if "lowest" in label or "planning" in label:
        return "Lowest"
    if "low" in label:
        return "Low"
    if "high" in label:
        return "High"
    return None


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


def _assign_jira_ticket(jira: JiraClient, issue_key: str, incident_number: str) -> None:
    """
    Assign a newly created Jira ticket to JIRA_ASSIGNEE. Never raises: a
    failed assignment only logs a warning, since the ticket itself was
    created and the incident should still move to In Progress.
    """
    global _jira_assignee_account_id
    if not JIRA_ASSIGNEE:
        return
    try:
        if _jira_assignee_account_id is None:
            _jira_assignee_account_id = jira.find_user_account_id(JIRA_ASSIGNEE)
        if not _jira_assignee_account_id:
            print(
                f"Warning: no Jira user found matching '{JIRA_ASSIGNEE}'; "
                f"leaving {issue_key} ({incident_number}) unassigned.",
                file=sys.stderr,
            )
            return
        jira.assign_issue(issue_key, _jira_assignee_account_id)
    except Exception as e:  # noqa: BLE001 - never let assignment break ticket routing
        print(
            f"Warning: created Jira ticket {issue_key} for {incident_number}, but failed to "
            f"assign it to '{JIRA_ASSIGNEE}' ({e}).",
            file=sys.stderr,
        )


def create_jira_ticket(incident: Incident) -> str | None:
    """
    Create a Jira ticket from a ServiceNow incident's details and return its
    key (e.g. "SCRUM-42"), or None if the ticket could not be created.

    Before creating, checks whether a Jira issue already exists for this
    incident (summary containing its ServiceNow number, e.g. "INC0012345")
    and reuses that key instead of creating a duplicate -- this guards
    against re-routing the same incident twice, e.g. if the local state file
    was reset/lost or the incident was polled again before its ServiceNow
    state could be updated to In Progress.

    Never raises: if Jira isn't configured or the API call fails for any
    reason, this logs a warning and returns None so the poll loop keeps
    running (the incident is left as-is in ServiceNow for a later retry).
    """
    summary, description = _build_jira_fields(incident)
    jira_priority = _map_servicenow_priority(incident.priority)
    try:
        jira = _get_jira_client()

        try:
            existing = jira.find_issue_by_summary_text(incident.number)
        except JiraAPIError as e:
            existing = None
            print(
                f"Warning: could not check for an existing Jira ticket for {incident.number} "
                f"({e}); proceeding to create a new one.",
                file=sys.stderr,
            )
        if existing:
            print(
                f"Jira ticket {existing.key} already exists for {incident.number}; "
                "skipping creation.",
                file=sys.stderr,
            )
            return existing.key

        extra_fields = {"priority": {"name": jira_priority}} if jira_priority else None
        try:
            issue = jira.create_issue(
                summary=summary,
                description=description,
                issue_type="Task",
                extra_fields=extra_fields,
            )
        except JiraAPIError as e:
            if not extra_fields:
                raise
            # The project's priority scheme may not have a "<jira_priority>"
            # option (custom schemes vary per project), or the create screen
            # may not expose Priority at all -- don't let that block the
            # incident from being tracked in Jira; retry once without it.
            print(
                f"Warning: failed to create Jira ticket for {incident.number} with priority "
                f"'{jira_priority}' ({e}); retrying without setting priority.",
                file=sys.stderr,
            )
            issue = jira.create_issue(summary=summary, description=description, issue_type="Task")
        _assign_jira_ticket(jira, issue.key, incident.number)
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


# Optional sys_id overrides for the assignment groups above, for when the
# ServiceNow API user can't read sys_user_group to look them up by name (see
# _resolve_group_sys_id()). Find a group's sys_id in ServiceNow via
# User Administration > Groups > right-click the header > Copy sys_id.
_GROUP_SYS_ID_OVERRIDES = {
    JIRA_ASSIGNMENT_GROUP: os.getenv("SERVICENOW_DATA_GROUP_SYS_ID", ""),
    SERVICE_DESK_ASSIGNMENT_GROUP: os.getenv("SERVICENOW_SERVICEDESK_GROUP_SYS_ID", ""),
}

# Cache of assignment group name -> sys_id (None if not found).
_group_sys_ids: dict[str, str | None] = {}


def _resolve_group_sys_id(client: ServiceNowClient, name: str) -> str | None:
    """
    Return the sys_id of the ServiceNow assignment group `name`, from its
    SERVICENOW_*_GROUP_SYS_ID override if set, else by looking it up.

    The sys_id is needed because writing a group *name* into the incident's
    assignment_group reference field only works if the API user can read
    sys_user_group; otherwise ServiceNow stores the raw name as a broken
    reference and the group shows up blank on the incident.
    """
    override = (_GROUP_SYS_ID_OVERRIDES.get(name) or "").strip()
    if override:
        return override
    if name not in _group_sys_ids:
        _group_sys_ids[name] = client.find_group_sys_id(name)
        if not _group_sys_ids[name]:
            print(
                f"Warning: ServiceNow assignment group '{name}' not found (or the API user "
                "lacks read access to sys_user_group). Grant the user the itil role, or set "
                "its sys_id via SERVICENOW_DATA_GROUP_SYS_ID / "
                "SERVICENOW_SERVICEDESK_GROUP_SYS_ID.",
                file=sys.stderr,
            )
    return _group_sys_ids[name]


def _update_incident_checked(
    client: ServiceNowClient, incident: Incident, fields: dict[str, str], group_name: str
) -> bool:
    """
    Update an incident with `fields` plus assignment_group = `group_name`
    (resolved to its sys_id), then verify ServiceNow actually saved the
    state/assignment_group -- it silently ignores fields the API user isn't
    allowed to write instead of returning an error.

    Returns True if everything requested was applied; logs a warning for
    each field that wasn't and returns False. ServiceNowAPIError propagates.
    """
    fields = dict(fields)
    group_sys_id = _resolve_group_sys_id(client, group_name)
    if group_sys_id:
        fields["assignment_group"] = group_sys_id

    saved = client.update_incident(incident.sys_id, fields)

    ok = bool(group_sys_id)
    for field in ("state", "assignment_group"):
        if field not in fields:
            continue
        value = saved.get(field)
        if isinstance(value, dict):
            value = value.get("value")
        if value != fields[field]:
            ok = False
            print(
                f"Warning: ServiceNow did not apply {field}={fields[field]!r} to "
                f"{incident.number} (saved value: {value!r}); the API user likely lacks "
                "write access to this field (needs the itil role).",
                file=sys.stderr,
            )
    return ok


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
            _update_incident_checked(
                client,
                inc,
                {
                    "state": IN_PROGRESS_INCIDENT_STATE,
                    "work_notes": f"Routed to Jira ticket {jira_key} by incident_monitor_ollama.",
                },
                JIRA_ASSIGNMENT_GROUP,
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


def route_incidents_to_servicedesk(
    client: ServiceNowClient,
    incidents: list[Incident],
    categories: dict[str, tuple[str, list[str]]],
) -> list[str]:
    """
    For every incident classified as "ServiceDesk", assign the ServiceNow
    incident to SERVICE_DESK_ASSIGNMENT_GROUP (ServiceDesk) with
    a work note recording the automatic classification.

    Returns the sys_ids of incidents successfully updated; incidents that
    aren't classified as "ServiceDesk", or for which the ServiceNow update
    failed, are simply absent from the result.

    Never raises: ServiceNow errors are logged and skipped so the poll loop
    keeps running.
    """
    updated: list[str] = []
    for inc in incidents:
        category, _matched = categories.get(inc.sys_id, ("ServiceDesk", []))
        if category != "ServiceDesk":
            continue

        try:
            if _update_incident_checked(
                client,
                inc,
                {
                    "work_notes": (
                        "Classified as ServiceDesk by incident_monitor_ollama; "
                        f"assigned to {SERVICE_DESK_ASSIGNMENT_GROUP}."
                    ),
                },
                SERVICE_DESK_ASSIGNMENT_GROUP,
            ):
                updated.append(inc.sys_id)
        except ServiceNowAPIError as e:
            print(
                f"Warning: failed to assign {inc.number} to "
                f"{SERVICE_DESK_ASSIGNMENT_GROUP} ({e}).",
                file=sys.stderr,
            )
        except Exception as e:  # noqa: BLE001 - never let ServiceNow issues break monitoring
            print(
                f"Warning: unexpected error assigning {inc.number} to "
                f"{SERVICE_DESK_ASSIGNMENT_GROUP} ({e}).",
                file=sys.stderr,
            )

    return updated


def reconcile_reverted_incidents(
    client: ServiceNowClient, incidents: list[Incident]
) -> dict[str, tuple[str, str]]:
    """
    Handle incidents that were already seen in a previous poll (i.e. already
    processed once) but have come back in this poll's "New" results anyway --
    this means their ServiceNow state must have been changed back to New by
    mistake sometime after processing (e.g. a user accidentally reset it),
    since an incident correctly left In Progress would no longer match a
    state=New query.

    For each such incident, confirm a Jira ticket already exists for it
    (matched via its ServiceNow number, same as create_jira_ticket's
    duplicate check) before touching anything -- no ticket is ever created
    here. If one exists, the ServiceNow state is restored to In Progress
    with a work note explaining why; no action is taken otherwise (e.g. the
    incident was previously classified ServiceDesk and never routed to
    Jira, so there is nothing to restore).

    Returns the confirmed (incident number, Jira key) per incident (keyed by
    sys_id) for those whose state was restored; incidents left untouched are
    absent from the result.

    Never raises: Jira/ServiceNow errors are logged and skipped so the poll
    loop keeps running.
    """
    fixed: dict[str, tuple[str, str]] = {}
    if not incidents:
        return fixed

    try:
        jira = _get_jira_client()
    except JiraConfigError as e:
        print(
            f"Warning: Jira is not configured ({e}); skipping reconciliation of "
            f"{len(incidents)} incident(s) whose state reverted to New.",
            file=sys.stderr,
        )
        return fixed

    for inc in incidents:
        try:
            existing = jira.find_issue_by_summary_text(inc.number)
        except JiraAPIError as e:
            print(
                f"Warning: could not check for an existing Jira ticket for {inc.number} "
                f"while reconciling its state ({e}); leaving it as-is.",
                file=sys.stderr,
            )
            continue

        if not existing:
            # No Jira ticket on record for this incident -- it wasn't
            # previously routed to Jira, so a state of New isn't a mistake.
            continue

        try:
            _update_incident_checked(
                client,
                inc,
                {
                    "state": IN_PROGRESS_INCIDENT_STATE,
                    "work_notes": (
                        f"State was found reset to New, but Jira ticket {existing.key} "
                        "already exists for this incident; restoring state to In Progress."
                    ),
                },
                JIRA_ASSIGNMENT_GROUP,
            )
            fixed[inc.sys_id] = (inc.number, existing.key)
            print(
                f"Incident {inc.number} was showing as New but Jira ticket {existing.key} "
                "already exists for it; restored ServiceNow state to In Progress.",
                file=sys.stderr,
            )
        except ServiceNowAPIError as e:
            print(
                f"Warning: found existing Jira ticket {existing.key} for {inc.number}, but "
                f"failed to restore its ServiceNow state to In Progress ({e}).",
                file=sys.stderr,
            )
        except Exception as e:  # noqa: BLE001 - never let ServiceNow issues break monitoring
            print(
                f"Warning: found existing Jira ticket {existing.key} for {inc.number}, but an "
                f"unexpected error occurred restoring its ServiceNow state ({e}).",
                file=sys.stderr,
            )

    return fixed


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


# ServiceNow "opened_at" display-value format (matches sysparm_display_value
# output, e.g. "2024-01-15 10:30:00"), used to parse the incremental-poll
# cursor for comparison. If a value doesn't match (unexpected instance
# format), _max_opened_at() falls back to plain string comparison, which
# still works correctly as long as the format is consistently zero-padded
# and left-to-right significant (as ServiceNow's default format is).
_OPENED_AT_FORMAT = "%Y-%m-%d %H:%M:%S"


def _max_opened_at(current: str | None, incidents: list[Incident]) -> str | None:
    """
    Return the latest "opened_at" value among current and every incident in
    incidents, used to advance the incremental-poll cursor (state's
    "last_opened_at") to the furthest point the New-state timeline has been
    swept up to.
    """

    def _key(value: str):
        try:
            return (0, datetime.strptime(value, _OPENED_AT_FORMAT))
        except ValueError:
            return (1, value)

    best = current
    for inc in incidents:
        if inc.opened_at and (best is None or _key(inc.opened_at) > _key(best)):
            best = inc.opened_at
    return best


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
    full_sweep_interval: int = 10,
) -> tuple[
    list[Incident],
    dict[str, tuple[str, list[str]]],
    dict[str, str],
    dict[str, tuple[str, str]],
    list[str],
]:
    """
    Fetch newly-created incidents (ServiceNow state == new_state, e.g.
    "New"), diff against previously-seen sys_ids, classify them, route any
    "Jira" incidents to Jira (creating a ticket and moving the incident to
    In Progress -- see route_incidents_to_jira), assign any "ServiceDesk"
    incidents to SERVICE_DESK_ASSIGNMENT_GROUP (see
    route_incidents_to_servicedesk), write any new ones to the output file,
    and update state in place.

    Polling is incremental: once a baseline has been established, each poll
    queries ServiceNow only for incidents opened at/after the cursor
    recorded in state["last_opened_at"] (the furthest point already swept),
    instead of re-fetching and re-diffing the same top `limit` newest
    incidents every time. This means a poll's cost no longer depends on how
    many incidents have already been seen, and a burst of more than `limit`
    new incidents between polls is never silently dropped -- the cursor only
    advances past what was actually fetched.

    Because an incremental poll only looks at incidents opened after the
    cursor, it cannot see an older incident whose state was reset back to
    new_state after already being processed (see
    reconcile_reverted_incidents() below). To still catch that, every
    full_sweep_interval-th poll (and always the first, baseline poll) instead
    does a full, non-incremental fetch of every current new_state incident
    (as before), which both re-establishes the cursor and re-surfaces any
    reverted incidents for reconciliation. Pass full_sweep_interval=1 to do a
    full sweep on every poll (disables the incremental optimization); pass 0
    to never sweep again after the baseline (fastest, but reverted incidents
    older than the incremental window would no longer be caught).

    Returns the list of newly detected incidents, their
    (category, matched_keywords) (keyed by sys_id), the Jira key created per
    sys_id (only present for incidents successfully routed to Jira), the
    confirmed Jira key per sys_id for incidents whose reverted state was
    restored to In Progress, and the sys_ids of incidents assigned to
    SERVICE_DESK_ASSIGNMENT_GROUP -- so callers don't need to re-classify or
    re-route them.
    """

    cursor = state.get("last_opened_at")
    is_baseline_run = not state.get("initialized", False)
    polls_since_sweep = state.get("polls_since_full_sweep", 0)
    do_full_sweep = (
        is_baseline_run
        or cursor is None
        or full_sweep_interval == 1
        or (full_sweep_interval > 1 and polls_since_sweep + 1 >= full_sweep_interval)
    )

    incidents = client.list_incidents(
        limit=limit,
        order_by_newest=True,
        state=new_state,
        query=None if do_full_sweep else f"opened_at>={cursor}",
    )

    state["polls_since_full_sweep"] = 0 if do_full_sweep else polls_since_sweep + 1
    state["last_opened_at"] = _max_opened_at(cursor, incidents)

    seen: set[str] = set(state.get("seen_sys_ids", []))

    new_incidents = [inc for inc in incidents if inc.sys_id and inc.sys_id not in seen]
    reverted_incidents = [inc for inc in incidents if inc.sys_id and inc.sys_id in seen]

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
    servicedesk_assigned: list[str] = []
    if written:
        categories = categorize_incidents(written, keywords)
        if route_to_jira:
            jira_keys = route_incidents_to_jira(client, written, categories)
            servicedesk_assigned = route_incidents_to_servicedesk(client, written, categories)
        append_incidents(output_path, written, fmt, keywords, categories, jira_keys)
    else:
        categories = {}

    reconciled_keys: dict[str, tuple[str, str]] = {}
    if reverted_incidents and route_to_jira:
        reconciled_keys = reconcile_reverted_incidents(client, reverted_incidents)

    # Update seen set, bounded so the state file doesn't grow forever.
    all_ids = list(seen.union(inc.sys_id for inc in incidents if inc.sys_id))
    state["seen_sys_ids"] = all_ids[-2000:]
    state["last_checked"] = datetime.now(timezone.utc).isoformat()

    return written, categories, jira_keys, reconciled_keys, servicedesk_assigned


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
        "--full-sweep-interval",
        type=int,
        default=10,
        help="After the baseline poll, do a full (non-incremental) re-fetch of every "
        "current incident -- needed to catch incidents reverted back to New after "
        "already being processed, see reconcile_reverted_incidents() -- every Nth "
        "poll; other polls fetch incrementally (only incidents opened since the "
        "last poll). Use 1 to full-sweep every poll (disables the incremental "
        "optimization), or 0 to never sweep again after the baseline (default: 10).",
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
            new_incidents, categories, jira_keys, reconciled_keys, servicedesk_assigned = poll_once(
                client,
                args.output,
                state,
                args.limit,
                args.format,
                args.include_existing,
                new_state,
                args.keywords,
                not args.no_jira,
                args.full_sweep_interval,
            )
        except ServiceNowAPIError as e:
            print(f"ServiceNow API error: {e}", file=sys.stderr)
            return
        finally:
            save_state(args.state, state)

        if reconciled_keys:
            print(
                f"[{datetime.now(timezone.utc).isoformat()}] "
                f"{len(reconciled_keys)} incident(s) had reverted to New by mistake and were "
                "restored to In Progress (existing Jira ticket confirmed):"
            )
            for number, jira_key in reconciled_keys.values():
                print(f"  - {number} -> Jira {jira_key}")

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
                servicedesk_note = (
                    f" -> assigned to {SERVICE_DESK_ASSIGNMENT_GROUP}"
                    if inc.sys_id in servicedesk_assigned
                    else ""
                )
                print(
                    f"  - {inc.number}: {inc.short_description or '(no description)'} "
                    f"[group={inc.assignment_group or 'n/a'} -> category={category}{keyword_note}]"
                    f"{jira_note}{servicedesk_note}"
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
