# ServiceNow Incident Agent (LangChain + OpenAI)

An agentic AI that connects to a ServiceNow PDI (Personal Developer Instance)
and answers questions about incidents using natural language. Built with
LangChain's tool-calling agent, backed by an OpenAI model, with a custom tool
that talks to the ServiceNow Table API.

## Architecture

```
servicenow_agent/
  servicenow_client.py   # REST client for the ServiceNow Table API (incident table)
  tools.py                # LangChain @tool wrapping the client, exposed to the agent
  agent.py                # Agent + AgentExecutor wiring (OpenAI LLM + tool)
```

Flow: user instruction -> OpenAI model decides to call `list_servicenow_incidents`
-> tool hits `GET /api/now/table/incident` on your PDI -> results are returned to
the model -> model formats a natural-language summary for the user.

## 1. Get your PDI details

1. Sign up / log in at https://developer.servicenow.com and request a PDI if you
   don't already have one.
2. Note the instance name from the URL, e.g. `https://dev12345.service-now.com`
   -> instance is `dev12345`.
3. Use your PDI admin username/password (or create a dedicated integration user
   with the `itil` role, which can read the incident table).

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

## 3. Configure credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```
SERVICENOW_INSTANCE=dev12345
SERVICENOW_USERNAME=admin
SERVICENOW_PASSWORD=your_pdi_password
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini
```

Never commit `.env` to source control.

## 4. Run the agent

Interactive chat:

```bash
python -m servicenow_agent.agent
```

Single instruction:

```bash
python -m servicenow_agent.agent "List the 5 most recent incidents"
python -m servicenow_agent.agent "Show me all open (New) incidents"
python -m servicenow_agent.agent "Any priority 1 incidents right now?"
```

## Notes

- The tool queries live data from your PDI every call — nothing is cached or
  simulated.
- `state` filter uses ServiceNow's numeric codes (1=New, 2=In Progress,
  3=On Hold, 6=Resolved, 7=Closed, 8=Canceled); the agent knows this mapping.
- For advanced filtering, the agent can pass a raw `sysparm_query` string
  (e.g. `priority=1^active=true`) via the `query` parameter.
- Swap `ServiceNowClient()` construction args or env vars if you want to point
  at a full ServiceNow instance instead of a PDI — the Table API is the same.
