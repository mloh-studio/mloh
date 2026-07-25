# Databricks Pipeline Remediation Agent

An agentic FastAPI application deployed as a Databricks App. It receives webhook notifications
from Delta Live Tables (DLT) pipelines, classifies the error, uses an LLM to explain what went
wrong and suggest fixes, then raises a rich notification to a Microsoft Teams channel for
non-transient failures.

---

## How it works

```
DLT Pipeline failure
        │
        │  POST /webhook  (JSON event)
        ▼
┌─────────────────────────────────────────────┐
│           FastAPI App (Databricks Apps)      │
│                                             │
│  1. classifier.py                           │
│     TRANSIENT? ──► suppress, return         │
│     NON_TRANSIENT? ──► continue             │
│                                             │
│  2. reasoner.py  (agentic layer)            │
│     ──► Databricks Foundation Model API     │
│         (databricks-meta-llama-3-3-70b)     │
│     ◄── summary + root cause + suggestions  │
│                                             │
│  3. notifier.py                             │
│     ──► Delta table  (audit log)            │
│     ──► Teams Adaptive Card  (if URL set)   │
└─────────────────────────────────────────────┘
```

### Error classification

| Error type | Classification |
|---|---|
| `RATE_LIMIT`, `REQUEST_TIMEOUT`, `TEMPORARILY_UNAVAILABLE` | Transient — suppressed |
| `SCHEMA_MISMATCH`, `EXPECTATION_FAILURE`, `CONFIGURATION_ERROR` | Non-transient — notified |
| `RESOURCE_DOES_NOT_EXIST`, `PARSE_EXCEPTION`, `INTERNAL_ERROR` | Non-transient — notified |
| Unknown | Non-transient — notified (fail-safe) |

### Webhook response

For non-transient errors the agent returns a structured JSON response including the LLM reasoning:

```json
{
  "pipeline_id": "pipe-001",
  "update_id": "upd-abc",
  "classification": "NON_TRANSIENT",
  "notification_sent": true,
  "reasoning": {
    "summary": "The pipeline failed because column 'id' is missing from the schema.",
    "root_cause": "A recent schema change removed the 'id' column from the source table.",
    "suggestions": [
      "Restore the 'id' column in the source table.",
      "Update DLT pipeline expectations to match the new schema.",
      "Validate schema compatibility before deploying pipeline changes."
    ]
  }
}
```

### Delta table schema (`main.default.pipeline_notifications`)

| Column | Description |
|---|---|
| `event_time` | UTC timestamp of the notification |
| `pipeline_id` | DLT pipeline ID |
| `pipeline_name` | Human-readable pipeline name |
| `update_id` | Pipeline update/run ID |
| `error_type` | Raw error type from the webhook |
| `error_message` | Raw error message from the webhook |
| `classification` | `TRANSIENT` or `NON_TRANSIENT` |
| `llm_summary` | Plain-English explanation from the LLM |
| `llm_root_cause` | Most likely root cause identified by the LLM |
| `llm_suggestions` | Pipe-separated list of fix suggestions |
| `notification_sent` | Whether a Teams notification was raised |

---

## Project structure

```
databricks-pipeline-remediation-agent/
├── databricks.yml                  # DAB root bundle
├── pyproject.toml                  # Python project + deps (managed by uv)
├── resources/
│   ├── pipeline.yml                # DLT pipeline resource (serverless)
│   └── app.yml                     # Databricks App resource
├── src/
│   ├── agent/
│   │   ├── app.py                  # FastAPI app — POST /webhook, GET /health
│   │   ├── classifier.py           # Rule-based error classifier
│   │   ├── reasoner.py             # Agentic layer — LLM via MLflow
│   │   └── notifier.py             # Delta write + Teams Adaptive Card
│   └── pipeline/
│       ├── bronze_ingest.py        # Mock DLT Bronze notebook
│       └── silver_transform.py     # Mock DLT Silver (deliberately fails)
├── tests/
│   ├── test_classifier.py
│   ├── test_notifier.py
│   └── test_app.py
└── scripts/
    └── trigger_test_webhook.sh     # Local test helper
```

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — `brew install uv`
- [Databricks CLI v0.200+](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) — `brew install databricks`
- A Databricks workspace (free edition) with:
  - Unity Catalog enabled (`main` catalog must exist)
  - Databricks Apps enabled
  - Foundation Model APIs enabled (for the LLM reasoning step)

---

## Local development

```bash
# Install all dependencies
uv sync

# Run the test suite (no Databricks connection needed)
uv run pytest tests/ -v

# Start the agent locally
uv run uvicorn src.agent.app:app --reload

# Send test webhook payloads to the local server
./scripts/trigger_test_webhook.sh
```

The test suite mocks both the LLM call and the Delta write — it runs entirely offline.

---

## Deployment with Databricks Asset Bundles (DAB)

The bundle deploys two resources: the DLT pipeline and the FastAPI app.
Because the app URL is only known after first deploy, wiring the webhook is a two-step process.

### Step 1 — Configure authentication

```bash
databricks configure --token
```

### Step 2 — First deploy (pipeline + app, webhook not yet wired)

```bash
databricks bundle deploy --target dev
```

### Step 3 — Get the deployed app URL

```bash
databricks apps get pipeline-remediation-agent-dev --query 'url'
```

Or find it in the Databricks UI under **Compute → Apps**.

### Step 4 — Wire the webhook via the REST API

DAB does not support `webhook_notifications` for pipelines. Wire the webhook using the
Databricks CLI after deploy:

```bash
# Get the pipeline ID
PIPELINE_ID=$(databricks pipelines list --output json | python3 -c "import sys,json; print(next(p['pipeline_id'] for p in json.load(sys.stdin) if 'remediation-agent-mock-pipeline' in p['name']))")

# Set the on_failure webhook to POST to the agent
databricks pipelines update $PIPELINE_ID --json '{
  "webhook_notifications": {
    "on_failure": [{"id": "https://<your-app-url>/webhook"}]
  }
}'
```

Or configure it in the Databricks UI: open the pipeline → **Settings** → **Notifications**.

### Step 5 — Trigger a test pipeline run

```bash
databricks bundle run pipeline_mock
```

The mock Silver notebook deliberately fails on a null-`id` row (`EXPECTATION_FAILURE`).
Databricks posts the event to the agent, which classifies it as non-transient, calls the LLM
for an explanation, and writes the result to `main.default.pipeline_notifications`.

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `TEAMS_WEBHOOK_URL` | No | Teams Incoming Webhook URL. When set, non-transient failures post an Adaptive Card with the LLM summary and fix suggestions. When unset, notifications are logged to stdout and Delta only. |
| `DATABRICKS_LLM_ENDPOINT` | No | Foundation Model API endpoint name. Defaults to `databricks-meta-llama-3-3-70b-instruct`. |
| `DATABRICKS_HOST` | Yes (on Databricks) | Workspace URL — set automatically when running as a Databricks App or Job. |
| `DATABRICKS_TOKEN` | Yes (on Databricks) | Personal access token — set automatically when running as a Databricks App or Job. |

---

## Mock vs production

The pipeline notebooks (`bronze_ingest.py`, `silver_transform.py`) are test fixtures that use
hardcoded in-memory data. The Silver notebook deliberately introduces a null-`id` row to trigger
the `EXPECTATION_FAILURE` path and exercise the full agent flow end-to-end.

In production, replace these notebooks with your real DLT pipeline. The agent itself requires
no changes — configure your live pipeline's webhook notifications to POST to the deployed app URL.
