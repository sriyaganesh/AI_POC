# ServiceNow ↔ Jira Incident Agent

Two complementary tools built on top of a shared ServiceNow client:

1. **`agent.py`** — a LangChain + OpenAI conversational agent you can ask
   natural-language questions about incidents in a ServiceNow PDI
   (Personal Developer Instance).
2. **`incident_monitor_ollama.py`** — an unattended poller that watches
   ServiceNow for newly created incidents, classifies each one as a
   **data issue** or a **general IT issue** using a locally-hosted Ollama
   model, and automatically **creates a Jira ticket** for the data issues
   (transitioning the ServiceNow incident to "In Progress" once routed).

   An older, simpler variant, **`incident_monitor.py`**, does the same
   polling/logging but classifies with OpenAI instead of Ollama and does
   not create Jira tickets — kept for reference/comparison.

Everything lives in the `servicenow_jira_agent` package.

## Architecture

```
servicenow_jira_agent/
  servicenow_client.py       # REST client for the ServiceNow Table API (incident table)
  jira_client.py             # REST client for the Jira Cloud API (issue creation)
  tools.py                   # LangChain @tool wrapping the ServiceNow client, for agent.py
  agent.py                   # Interactive/one-shot Q&A agent (OpenAI LLM + tool)
  incident_monitor.py        # Poll -> classify (OpenAI) -> log to CSV/JSONL
  incident_monitor_ollama.py # Poll -> classify (Ollama) -> route to Jira -> log
  data_keywords.yaml         # Editable keyword list used as classification hints
  llm_usage_tracker.py       # Logs tokens/latency/outcome for every LLM call
```

### 1. The Q&A agent (`agent.py`)

```
user instruction
   -> OpenAI model (LangChain create_agent) decides whether to call the
      "list_servicenow_incidents" tool
   -> tool hits GET /api/now/table/incident on your PDI (servicenow_client.py)
   -> each returned incident is also tagged Jira/ServiceDesk for context
      (categorize_incident() from incident_monitor.py)
   -> results go back to the model
   -> model writes a natural-language summary (table/bullets) for the user
```

Run it interactively or with a single instruction (see [Usage](#usage) below).
It only reads data — it never modifies ServiceNow or creates Jira tickets.

### 2. The incident monitor / auto-router (`incident_monitor_ollama.py`)

This is the main pipeline. On every poll it:

1. **Fetches** incidents from ServiceNow that are still in the "New" state
   (`SERVICENOW_NEW_STATE`, default `1`) — i.e. not yet actioned by anyone.
2. **Diffs** them against a local state file so nothing is processed twice
   (`.incident_monitor_ollama_state.json`, gitignored). The very first run
   just records a baseline and writes nothing (unless `--include-existing`),
   so the output file only ever contains incidents opened *after* monitoring
   started.
3. **Classifies** each new incident as `Jira` (data issue) or `ServiceDesk`
   (general issue), in this order:
   - **Assignment group short-circuit**: if the incident's assignment group
     is `POC_DATA`, it's `Jira` immediately — no LLM call needed.
   - **Keyword hint**: otherwise, the short description + description are
     checked against the editable keyword list in `data_keywords.yaml`
     (general data terms plus ETL/BI/warehouse tool names — Informatica,
     Airflow, Snowflake, Power BI, dbt, etc.). This is a hint, not a
     verdict.
   - **LLM judgement**: the full incident text, plus the keyword hint (if
     any), is sent to a local Ollama model (`OLLAMA_MODEL`, default
     `qwen3:4b`) with a system prompt instructing it to always read the
     actual text rather than defaulting off the keyword match (e.g. "the
     database server is unreachable" is infrastructure, not a data issue,
     even though "database" matched). The model answers exactly `JIRA` or
     `SERVICEDESK`.
   - Multiple incidents from the same poll are classified **concurrently**
     (up to 4 parallel Ollama calls) instead of one at a time.
4. **Routes** every incident classified `Jira`:
   - Before creating anything, searches the Jira project for an existing
     issue whose summary already contains the incident number
     (`JiraClient.find_issue_by_summary_text()`) and reuses that key instead
     of creating a duplicate.
   - Otherwise creates a Jira issue (`jira_client.py`, Jira Cloud REST API
     v3) carrying over the incident number, priority, caller, assignment
     group, and description.
   - Either way, PATCHes the ServiceNow incident: `state` →
     `SERVICENOW_IN_PROGRESS_STATE` (default `2`, In Progress) and adds a
     work note recording the Jira key.
   - Pass `--no-jira` to classify/log only and skip this step entirely.
5. **Reconciles reverted incidents**: incidents that reappear among "New"
   results despite already being recorded as seen (e.g. someone accidentally
   reset a routed incident's state back to New) are checked for an existing
   Jira ticket via the same summary-text search; if one is found, the
   ServiceNow state is restored to In Progress with an explanatory work
   note and no duplicate is created (`reconcile_reverted_incidents()`). This
   runs whenever `--no-jira` is not set.
6. **Logs** every newly-seen incident (category, matched keywords, Jira key
   if any) to the output file — `new_incidents_ollama.csv` by default, or
   JSONL with `--format jsonl`.
7. **Records** every LLM call (model, tokens, latency, success/failure) to
   `llm_usage_log.jsonl` and prints a usage summary at the end of each run
   (or on demand via `--usage-summary`).
8. Optionally **loops forever** on an interval with `--watch --interval N`
   (e.g. for cron / Windows Task Scheduler you'd instead just run it once
   per invocation).

Any failure in a single step (Ollama unreachable, Jira misconfigured, a
ServiceNow update failing) is caught, logged as a warning, and does **not**
stop the poll loop — classification falls back to `ServiceDesk` and routing
is simply skipped for that incident so it can be retried on a later poll.

## 1. Get your PDI details

1. Sign up / log in at https://developer.servicenow.com and request a PDI if
   you don't already have one.
2. Note the instance name from the URL, e.g. `https://dev12345.service-now.com`
   → instance is `dev12345`.
3. Use your PDI admin username/password (or create a dedicated integration
   user with the `itil` role, which can read/update the incident table).

## 2. Get your Jira details (only needed for `incident_monitor_ollama.py`)

1. In Jira Cloud, create (or pick) the project data incidents should land in
   and note its **project key** (e.g. `SCRUM`).
2. Create an API token at https://id.atlassian.com/manage-profile/security/api-tokens.
3. Your Jira site URL is `https://<your-site>.atlassian.net`.

## 3. Set up Ollama (only needed for `incident_monitor_ollama.py`)

1. Install Ollama from https://ollama.com and make sure it's running.
2. Pull the model you want to classify with, e.g.:
   ```bash
   ollama pull qwen3:4b
   ```

## 4. Install dependencies

```bash
pip install -r requirements.txt
```

## 5. Configure credentials

Create a `.env` file in the project root (never commit it — it's already in
`.gitignore`):

```
# ServiceNow
SERVICENOW_INSTANCE=dev12345
SERVICENOW_USERNAME=admin
SERVICENOW_PASSWORD=your_pdi_password
SERVICENOW_NEW_STATE=1
SERVICENOW_IN_PROGRESS_STATE=2

# OpenAI (used by agent.py and incident_monitor.py)
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini

# Ollama (used by incident_monitor_ollama.py)
OLLAMA_MODEL=qwen3:4b
OLLAMA_BASE_URL=http://localhost:11434

# Jira (used by incident_monitor_ollama.py for auto-routing)
JIRA_URL=https://your-site.atlassian.net
JIRA_EMAIL=you@example.com
JIRA_API_TOKEN=your_api_token
JIRA_PROJECT=SCRUM

# Optional: override the keyword file location
DATA_KEYWORDS_FILE=servicenow_jira_agent/data_keywords.yaml
```

You only need the section(s) relevant to the tool you're running — e.g. the
Q&A agent only needs the ServiceNow + OpenAI settings.

## Usage

### Q&A agent

Interactive chat:

```bash
python -m servicenow_jira_agent.agent
```

Single instruction:

```bash
python -m servicenow_jira_agent.agent "List the 5 most recent incidents"
python -m servicenow_jira_agent.agent "Show me all open (New) incidents"
python -m servicenow_jira_agent.agent "Any priority 1 incidents right now?"
```

### Incident monitor + Jira auto-router (Ollama)

Run once (e.g. from a scheduler):

```bash
python -m servicenow_jira_agent.incident_monitor_ollama
```

Run continuously, polling every 60 seconds:

```bash
python -m servicenow_jira_agent.incident_monitor_ollama --watch --interval 60
```

Classify and log but don't touch Jira/ServiceNow state:

```bash
python -m servicenow_jira_agent.incident_monitor_ollama --no-jira
```

Poll incidents in any state, not just "New", and write JSONL instead of CSV:

```bash
python -m servicenow_jira_agent.incident_monitor_ollama --all-states --format jsonl
```

Print a summary of every LLM call logged so far (tokens, latency, outcomes)
without polling ServiceNow:

```bash
python -m servicenow_jira_agent.incident_monitor_ollama --usage-summary
```

Other flags: `--output`, `--state`, `--limit`, `--keywords`,
`--incident-state`, `--include-existing` — run with `--help` for the full
list.

### Incident monitor (OpenAI, no Jira routing)

```bash
python -m servicenow_jira_agent.incident_monitor
python -m servicenow_jira_agent.incident_monitor --watch --interval 60
```

## Customizing data-vs-general classification

Edit `servicenow_jira_agent/data_keywords.yaml` — one keyword/phrase per
line, no code changes or restart required (the file is re-read whenever its
modified time changes, even while `--watch` is running). Matching is
case-insensitive substring matching against the short description +
description. These keywords are only a *hint*: the LLM still reads the full
incident text and can classify an incident as `Jira` even with zero keyword
matches, or as `ServiceDesk` despite a keyword match (e.g. "database server
is unreachable" is infrastructure, not a data problem).

Incidents assigned to the **`POC_DATA`** group always route to Jira
immediately, bypassing keyword/LLM classification entirely.

## Notes

- ServiceNow `state` uses numeric codes: 1=New, 2=In Progress, 3=On Hold,
  6=Resolved, 7=Closed, 8=Canceled. Override `SERVICENOW_NEW_STATE` /
  `SERVICENOW_IN_PROGRESS_STATE` if your instance customizes these.
- State files (`.incident_monitor_state.json`,
  `.incident_monitor_ollama_state.json`) and output files
  (`new_incidents.csv`, `new_incidents_ollama.csv`) are gitignored — they're
  local run artifacts, not source.
- `llm_usage_log.jsonl` accumulates across runs/processes and can be
  summarized any time with `--usage-summary`, independent of whether
  `--watch` is still running elsewhere.
- The Q&A agent's tool queries live data from your PDI on every call —
  nothing is cached or simulated. For advanced filtering it can pass a raw
  `sysparm_query` string (e.g. `priority=1^active=true`).
- Both ServiceNow and Jira clients use basic auth (PDI username/password,
  and Jira email + API token respectively), which is what a PDI and Jira
  Cloud support out of the box.
- `incident_monitor_ollama.py` never creates a duplicate Jira ticket for the
  same incident: before creating one it searches Jira for an existing issue
  whose summary contains the incident number, and reuses that key if found.
  This also covers an incident whose ServiceNow state gets reset back to
  New after already being routed — the next poll detects it, confirms the
  existing ticket, and restores the state to In Progress instead of
  re-routing it.
