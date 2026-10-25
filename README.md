# ServiceNow ↔ Jira Incident Agent

This project watches a ServiceNow PDI (Personal Developer Instance) for new
incidents. For each one it decides whether it is a **data issue** or a
**general IT issue**, then routes it automatically:

| Classification | What happens in Jira | What happens in ServiceNow |
|---|---|---|
| **Jira** (data issue) | A Jira **Task** is created and **assigned to `sglearn`** | State → **In Progress**, assignment group → `POC_DATA`, work note with the Jira key |
| **ServiceDesk** (general issue) | Nothing | Assignment group → `ServiceDesk`, work note |

Everything lives in the `servicenow_jira_agent` Python package. It contains
three runnable tools:

| Tool | Purpose | LLM used |
|---|---|---|
| `incident_monitor_ollama.py` | **Main pipeline**: poll → classify → route to Jira / ServiceDesk → log | Local Ollama (`qwen3:4b` by default) |
| `agent.py` | Ask questions about incidents in plain English (read-only) | OpenAI (`gpt-4o-mini` by default) |
| `incident_monitor.py` | Older version of the monitor: poll → classify → log only, with no Jira or ServiceNow updates. Kept for reference | OpenAI |

---

## Part A — How an incident is processed (step by step)

This is what `incident_monitor_ollama.py` does on **every poll**.

### Step 1 — Fetch "New" incidents from ServiceNow
- Calls `GET https://<instance>.service-now.com/api/now/table/incident`
  with `state=1` (New, configurable through `SERVICENOW_NEW_STATE`), newest first,
  up to `--limit` records (default `50`).
- **Polling is incremental.** After the first run, each poll fetches only incidents
  whose `opened_at` is at or after the cursor saved in the state file
  (`last_opened_at`).
- **Full sweep:** every 10th poll (`--full-sweep-interval`, default `10`)
  re-fetches every incident that is currently New. This is how incidents that
  were reset back to New get caught (see Step 7).

### Step 2 — Skip incidents that were already processed
- The sys_ids already seen are stored in `.incident_monitor_ollama_state.json`.
  The file keeps the last 2,000 sys_ids.
- **First run only:** the incidents that exist when you first start are
  recorded as a *baseline* and are **not** processed. Only incidents created
  after that point are routed. To process the baseline incidents too, pass
  `--include-existing`.

### Step 3 — Classify each new incident as `Jira` or `ServiceDesk`
**Every** new incident is classified on its content. The assignment group it
was raised with is ignored: an incident assigned straight to `POC_DATA` is
still checked, and is moved to `ServiceDesk` if it isn't a data issue. The
checks below run in this order:
1. **Keyword hint.** The short description and description are compared
   against `servicenow_jira_agent/data_keywords.yaml`. Matching is a
   case-insensitive substring match. The keywords cover general data terms
   and ETL/BI tool names such as Informatica, Airflow, Snowflake and Power BI.
2. **LLM decision.** The incident text and the keyword hint are sent to the
   local Ollama model, which answers exactly `JIRA` or `SERVICEDESK`. The
   keyword hint is supporting evidence only:
   - "database server is unreachable" can still be classified ServiceDesk
     (it is an infrastructure problem).
   - A data issue that matches no keyword can still be classified Jira.
- Several incidents are classified in parallel, up to
  `OLLAMA_MAX_CONCURRENT_CALLS` at a time (default `2`).
- If the LLM call fails or times out (default `180` seconds), the incident
  defaults to **ServiceDesk**.

### Step 4 — Route "Jira" incidents
For each incident classified **Jira**:
1. **Duplicate check.** Jira is searched with
   `project = "<JIRA_PROJECT>" AND summary ~ "<INC number>"`. If an issue is
   found, its key is reused and **no new ticket is created**.
2. **Create the Jira ticket.** The ticket is created as a **Task** in `JIRA_PROJECT`:
   - **Summary:** `[INC0012345] <short description>`
   - **Description:** ServiceNow number, priority, caller, assignment group,
     opened-at time and the full description.
   - **Priority:** mapped from ServiceNow priority:

     | ServiceNow | Jira |
     |---|---|
     | 1 - Critical | Highest |
     | 2 - High | High |
     | 3 - Moderate | Medium |
     | 4 - Low | Low |
     | 5 - Planning | Lowest |

     If Jira rejects the priority value, the ticket is created again
     once without a priority.
3. **Assign the ticket to `sglearn`.**
   - Jira Cloud identifies users by account ID, not username. The monitor
     looks up the account ID of `JIRA_ASSIGNEE` (default `sglearn`) once,
     using `GET /rest/api/3/user/search`, and caches it.
   - It then calls `PUT /rest/api/3/issue/<KEY>/assignee`.
   - If the assignment fails, a warning is logged and the ticket stays
     unassigned. The incident still moves on to the next step.
   - Only **newly created** tickets are assigned. A reused existing ticket
     keeps its current assignee.
4. **Move the ServiceNow incident to In Progress.** This is
   `PATCH /api/now/table/incident/<sys_id>` with:
   - `state` = `2` (In Progress, configurable through `SERVICENOW_IN_PROGRESS_STATE`)
   - `assignment_group` = `POC_DATA`
   - `work_notes` = `Routed to Jira ticket <KEY> by incident_monitor_ollama.`

If the Jira ticket can't be created, the incident is **left as New** and
nothing else happens to it.

### Step 5 — Route "ServiceDesk" incidents
Every incident that is **not** classified Jira is classified **ServiceDesk**.
This includes incidents where the LLM call failed, and incidents that were
raised with `POC_DATA` as their group but turned out not to be data issues.
Each one is updated in ServiceNow with:
- `assignment_group` = `ServiceDesk`
- a work note recording the classification.

Its **state is not changed**.

> **How groups are set (Steps 4 and 5):** the group name is first converted
> to the group's sys_id, either looked up in `sys_user_group` or taken from
> the `SERVICENOW_*_GROUP_SYS_ID` override. After each update, the monitor
> reads back the saved `state` and `assignment_group` and prints a warning
> if ServiceNow didn't apply them.

### Step 6 — Write each incident to the output file
Every processed incident is appended to `new_incidents_ollama.csv`, or to a
JSONL file with `--format jsonl`. The columns are:
`detected_at, number, sys_id, short_description, description, state,
priority, opened_at, caller, assignment_group, category, matched_keywords,
jira_key`.

### Step 7 — Fix incidents that were reset to New by mistake
- An incident that was already processed but shows up as New again was most
  likely reset by someone.
- The monitor looks for its existing Jira ticket (same summary search as Step 4).
- If a ticket is found, the incident is set back to **In Progress** and
  `POC_DATA`, with an explanatory work note. **No duplicate ticket is created.**
- If no ticket is found, the incident is left alone.

### Step 8 — Save state and log LLM usage
- The state file is saved after every poll, including polls that fail.
- Every LLM call (model, tokens, latency, result) is appended to
  `servicenow_jira_agent/llm_usage_log.jsonl`.
- A usage summary is printed when the run ends.

> **Errors never stop the monitor.** If Ollama is down, Jira is
> misconfigured or a ServiceNow update fails, the problem is printed as a
> warning on stderr and the poll continues.

---

## Part B — Setup (step by step)

### Step 1 — Install prerequisites
- **Python 3.10 or newer.** The code uses `str | None` type hints.
- **Git.**
- **Ollama** from https://ollama.com. You need it only for the monitor.

### Step 2 — Get the code and install dependencies
From the project root (`AI_POC`):

```bash
python -m venv .venv
# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

This installs `langchain`, `langchain-openai`, `langchain-ollama`,
`langchain-core`, `python-dotenv`, `requests` and `PyYAML`.

### Step 3 — Prepare ServiceNow
1. Log in at https://developer.servicenow.com and start or request your PDI.
2. Note the instance URL, for example `https://dev12345.service-now.com`.
3. Use the PDI `admin` account, or an integration user with the **`itil`** role.
   The user must be able to:
   - **read and update** the `incident` table, including the `state` field;
   - **read** the `sys_user_group` table, which is used to find the assignment groups.

   > ⚠️ Without these permissions ServiceNow **does not return an error**.
   > It silently ignores the `state` change and stores the group name as an
   > invalid reference, so the group shows up blank. The monitor now detects
   > this and prints a warning (see Part F).
4. Make sure both assignment groups exist (*User Administration → Groups*):
   - `POC_DATA` (data incidents / Jira)
   - `ServiceDesk` (general incidents)
5. *(Only needed if the user can't read groups)* Copy each group's sys_id
   into `.env` as `SERVICENOW_DATA_GROUP_SYS_ID` and
   `SERVICENOW_SERVICEDESK_GROUP_SYS_ID`. To find a sys_id, open the group,
   right-click the form header and choose **Copy sys_id**.

### Step 4 — Prepare Jira Cloud
1. Note your site URL, for example `https://your-site.atlassian.net`.
2. Note the **project key** that tickets should be created in, for example `SCRUM`.
   The project must have the **Task** issue type.
3. Create an API token at
   https://id.atlassian.com/manage-profile/security/api-tokens.
4. Make sure the user **`sglearn`** exists in this Jira site and is
   **assignable** in the project. Assignable means the user has the
   *Assignable User* permission, which project members usually have.

### Step 5 — Prepare Ollama
```bash
ollama pull qwen3:4b
ollama list          # confirm the model is listed
```
Ollama must be running at `http://localhost:11434`, or at whatever URL
`OLLAMA_BASE_URL` is set to.

### Step 6 — Create the `.env` file
Create `.env` in the project root. It is already in `.gitignore`, so **never
commit it**.

```dotenv
# --- ServiceNow (required) ---
SERVICENOW_INSTANCE=https://dev12345.service-now.com
SERVICENOW_USERNAME=admin
SERVICENOW_PASSWORD=your_pdi_password

# --- Jira (required for routing) ---
JIRA_URL=https://your-site.atlassian.net
JIRA_EMAIL=you@example.com
JIRA_API_TOKEN=your_api_token
JIRA_PROJECT=SCRUM
JIRA_ASSIGNEE=sglearn

# --- Ollama (required for the monitor) ---
OLLAMA_MODEL=qwen3:4b
OLLAMA_BASE_URL=http://localhost:11434

# --- OpenAI (only for agent.py / incident_monitor.py) ---
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
```

> `SERVICENOW_INSTANCE` accepts either `dev12345` or the full URL. Use the
> **full URL**, because `test/test_servicenow.py` requires it.

### Step 7 — Test the connections
Run these from the project root:

```bash
python servicenow_jira_agent/test/test_servicenow.py   # expect "HTTP Status: 200"
python servicenow_jira_agent/test/test_jira.py         # tests the Jira connection
```

To check that the assignee resolves to a Jira account, run this. It only
reads data and does not create anything:

```bash
python -c "from dotenv import load_dotenv; load_dotenv(); from servicenow_jira_agent.jira_client import JiraClient; print(JiraClient().find_user_account_id('sglearn'))"
```
It should print an account ID such as `712020:...`. If it prints `None`,
no Jira user matches `sglearn`.

### Step 8 — Run the monitor
Always run from the **project root**. The state and output files are created
in the current folder.

**First run** (records the baseline only):
```bash
python -m servicenow_jira_agent.incident_monitor_ollama
```

**Continuous monitoring** (polls every 60 seconds; press Ctrl+C to stop):
```bash
python -m servicenow_jira_agent.incident_monitor_ollama --watch --interval 60
```

### Step 9 — Test it end to end
1. With the monitor running, create a new incident in ServiceNow that has a
   data-related description, for example *"Daily sales report shows
   duplicate rows after last night's ETL load"*.
2. Wait for the next poll. The console shows a line like this:
   ```
   - INC0010042: Daily sales report shows duplicate rows ... [group=n/a -> category=Jira, keywords=['report', 'ETL']] -> Jira SCRUM-15 created, incident set to In Progress
   ```
3. Check Jira. **SCRUM-15** exists, has the matching priority and is **assigned to sglearn**.
4. Check ServiceNow. The incident's state is **In Progress**, its group is
   **POC_DATA**, and its work notes mention **SCRUM-15**.
5. Create a general incident, for example *"Printer on floor 3 not working"*.
   It is assigned to **ServiceDesk** and stays **New**.
6. Create another general incident, but set its assignment group to
   **POC_DATA** when you raise it. It is still classified on its content, so
   no Jira ticket is created and its group changes to **ServiceDesk**.

---

## Part C — Command reference

### `incident_monitor_ollama.py` flags

| Flag | Default | Meaning |
|---|---|---|
| `--watch` | off | Keep polling in a loop |
| `--interval N` | `60` | Seconds between polls (with `--watch`) |
| `--limit N` | `50` | Maximum incidents fetched per poll |
| `--format csv\|jsonl` | `csv` | Output file format |
| `--output PATH` | `new_incidents_ollama.csv` | Output file |
| `--state PATH` | `.incident_monitor_ollama_state.json` | State file |
| `--keywords PATH` | `servicenow_jira_agent/data_keywords.yaml` | Keyword file |
| `--incident-state N` | `1` | ServiceNow state to poll |
| `--all-states` | off | Poll incidents in any state |
| `--include-existing` | off | On the first run, also process the existing incidents |
| `--full-sweep-interval N` | `10` | Full re-fetch every Nth poll (`1` = every poll, `0` = never after the first run) |
| `--no-jira` | off | Classify and log only: no Jira tickets and **no ServiceNow updates** |
| `--usage-summary` | — | Print LLM usage statistics and exit |

Examples:
```bash
python -m servicenow_jira_agent.incident_monitor_ollama --no-jira          # dry run, no changes in Jira or ServiceNow
python -m servicenow_jira_agent.incident_monitor_ollama --usage-summary    # LLM token and latency report
python -m servicenow_jira_agent.incident_monitor_ollama --help
```

### Environment variables

| Variable | Default | Used for |
|---|---|---|
| `SERVICENOW_INSTANCE` / `_USERNAME` / `_PASSWORD` | — (required) | ServiceNow connection |
| `SERVICENOW_NEW_STATE` | `1` | Incident state treated as "New" |
| `SERVICENOW_IN_PROGRESS_STATE` | `2` | State set after a Jira ticket is created |
| `SERVICENOW_DATA_GROUP_SYS_ID` | — (looked up by name) | sys_id of `POC_DATA`, if the user can't read groups |
| `SERVICENOW_SERVICEDESK_GROUP_SYS_ID` | — (looked up by name) | sys_id of `ServiceDesk`, if the user can't read groups |
| `JIRA_URL` / `JIRA_EMAIL` / `JIRA_API_TOKEN` / `JIRA_PROJECT` | — (required for routing) | Jira connection |
| `JIRA_ASSIGNEE` | `sglearn` | Jira user that new tickets are assigned to. An empty value leaves tickets unassigned |
| `OLLAMA_MODEL` | `qwen3:4b` | Classification model |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server |
| `OLLAMA_TIMEOUT_SECONDS` | `180` | Timeout per classification call |
| `OLLAMA_NUM_PREDICT` | `2048` | Maximum tokens per classification call |
| `OLLAMA_MAX_CONCURRENT_CALLS` | `2` | Number of parallel classification calls |
| `DATA_KEYWORDS_FILE` | `servicenow_jira_agent/data_keywords.yaml` | Keyword file location |
| `OPENAI_API_KEY` / `OPENAI_MODEL` | — / `gpt-4o-mini` | `agent.py` and `incident_monitor.py` only |

### Q&A agent (`agent.py`, read-only)
```bash
python -m servicenow_jira_agent.agent                                     # interactive; type 'exit' to quit
python -m servicenow_jira_agent.agent "List the 5 most recent incidents"  # single question
python -m servicenow_jira_agent.agent "Any priority 1 incidents right now?"
```
The agent uses the `list_servicenow_incidents` tool (`tools.py`) to fetch live
data from your PDI. It never changes ServiceNow and never creates Jira tickets.

### Legacy monitor (`incident_monitor.py`, OpenAI, no routing)
```bash
python -m servicenow_jira_agent.incident_monitor --watch --interval 60
```

---

## Part D — Customizing the classification

- **Keywords:** edit `servicenow_jira_agent/data_keywords.yaml`, with one entry
  per line (`- keyword`). Changes take effect on the **next poll**, even while
  `--watch` is running. No restart is needed.
- **Assignment group doesn't decide routing:** assigning an incident to
  `POC_DATA` when it is raised does **not** send it to Jira. Every new
  incident goes through keyword and LLM classification. To steer
  borderline cases towards Jira, add keywords instead.
- **Different assignee:** set `JIRA_ASSIGNEE` in `.env` to a username,
  display name or email, then restart the monitor.

---

## Part E — Files and project layout

```
AI_POC/
  .env                                  # your credentials (git-ignored)
  requirements.txt
  new_incidents_ollama.csv              # monitor output (git-ignored)
  .incident_monitor_ollama_state.json   # monitor state (git-ignored)
  servicenow_jira_agent/
    servicenow_client.py         # ServiceNow Table API: list_incidents(), update_incident()
    jira_client.py               # Jira REST v3: create_issue(), find_issue_by_summary_text(),
                                 #   find_user_account_id(), assign_issue()
    incident_monitor_ollama.py   # main pipeline (Part A)
    incident_monitor.py          # legacy OpenAI monitor
    agent.py / tools.py          # Q&A agent and its LangChain tool
    data_keywords.yaml           # editable data keywords
    llm_usage_tracker.py         # LLM usage logging -> llm_usage_log.jsonl
    test/test_servicenow.py      # ServiceNow connection check
    test/test_jira.py            # Jira connection check
```

> **Reset the monitor:** delete `.incident_monitor_ollama_state.json`. The
> next run then records a new baseline. Jira's duplicate check stops tickets
> from being created twice.

---

## Part F — Troubleshooting

| Message / symptom | Cause | Fix |
|---|---|---|
| `Configuration error: Missing ServiceNow connection settings` | `SERVICENOW_*` values are missing from `.env` | Complete Step 6 and run from the project root |
| `Warning: Jira is not configured` | `JIRA_*` values are missing | Add all four `JIRA_*` values |
| `Warning: no Jira user found matching 'sglearn'` | The user doesn't exist or isn't visible to the API user | Check the user in Jira, or set `JIRA_ASSIGNEE` to their email |
| `... failed to assign it to 'sglearn' (Jira API returned 400 ...)` | The user isn't assignable in the project | Give the user the *Assignable User* permission in the project |
| `Warning: LLM classification failed ...` | Ollama isn't running, the model isn't pulled, or the call timed out | Run `ollama list`, pull the model, or increase `OLLAMA_TIMEOUT_SECONDS` |
| Everything is classified ServiceDesk | LLM timeouts (each failure defaults to ServiceDesk) | Check `--usage-summary` for failures and reduce `OLLAMA_MAX_CONCURRENT_CALLS` |
| `... failed to update its ServiceNow state to In Progress (... 403 ...)` | The ServiceNow user can't update incidents | Give the user the `itil` role, or use `admin` |
| `ServiceNow assignment group '...' not found (or the API user lacks read access ...)` | The group doesn't exist, or the user can't read `sys_user_group` | Give the user the `itil` role, or set `SERVICENOW_*_GROUP_SYS_ID` (Part B, Step 3.5) |
| `ServiceNow did not apply state='2' ...` / `... assignment_group=...` | ServiceNow silently ignored a field the user isn't allowed to write | Give the user the `itil` role |
| Incident has a Jira key but stays **New**, or its group is blank | Same permissions problem, from a run before these checks were added | Fix the permissions, then correct those incidents by hand |
| Incident raised in `POC_DATA` was moved to `ServiceDesk` | Expected: the assignment group doesn't decide routing, and the LLM judged it not to be a data issue | If it really is a data issue, add matching keywords to `data_keywords.yaml` (Part D) |
| First run shows "No new incidents." | Expected: the first run only records the baseline | Create a new incident, or use `--include-existing` |

### ServiceNow state codes
`1` New · `2` In Progress · `3` On Hold · `6` Resolved · `7` Closed · `8` Canceled
