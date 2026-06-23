# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An agentic data reconciliation system where Claude drives a `tool_use` loop over Databricks Delta Lake tables. Claude observes data-quality discrepancies, calls tools to gather evidence, then decides to auto-remediate, flag for human review, or escalate. The app runs as a **Databricks App** (FastAPI + browser SPA) and streams agent events to the UI via SSE.

## Local development

```bash
pip install -r requirements.txt

export ANTHROPIC_API_KEY=sk-ant-...
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>

uvicorn app.main:app --reload
```

Open <http://localhost:8000>. The demo tables (`main.recon_demo.orders_source` / `orders_warehouse`) must exist first — run `notebooks/01_setup_tables.py` in the workspace once.

## Deploying to Databricks

```bash
# One-time: create the secret scope and secrets
databricks secrets create-scope recon-agent
databricks secrets put-secret recon-agent anthropic-api-key --string-value $ANTHROPIC_API_KEY
databricks secrets put-secret recon-agent warehouse-id    --string-value $DATABRICKS_WAREHOUSE_ID

# Deploy the Asset Bundle (app + 4 notebook jobs)
databricks bundle deploy -t dev
databricks apps deploy recon-agent
```

## Architecture

```
Browser → POST /api/run → FastAPI spawns asyncio background task
                            ↓
                    ReconciliationAgent.run() (app/agent.py)
                      Claude tool_use loop (max 10 iterations)
                            ↓ tool calls
                    ReconTools.dispatch() (app/tools.py)
                      → Databricks SQL Connector / Jobs SDK / MLflow
                            ↓ AgentEvent objects
              GET /api/{run_id}/stream → SSE → Browser
```

Active runs are held in the in-memory `RUNS` dict in `app/main.py`. Completed runs are persisted to the `recon_audit_log` Delta table and MLflow.

## Key files

| File | Role |
|---|---|
| `app/agent.py` | Claude `tool_use` loop, `SYSTEM_PROMPT`, `TOOL_DEFINITIONS`, `_TOOL_TO_AGENT` persona map |
| `app/tools.py` | `ReconTools` class — 9 tool implementations, `dispatch()` router, `_query`/`_execute` SQL helpers |
| `app/main.py` | FastAPI routes, SSE streaming, in-memory `RUNS` state |
| `app/config.py` | `Settings` (pydantic-settings, env-driven, `lru_cache`'d) |
| `app/models.py` | `RunRequest`, `AgentEvent`, `ReconciliationResult` Pydantic models |
| `databricks.yml` | Asset Bundle: app resource + 4 notebook jobs (profiler, differ, backfill, verifier) |
| `app.yaml` | Databricks App manifest — wires secrets into `ANTHROPIC_API_KEY` and `DATABRICKS_WAREHOUSE_ID` |

## Adding a tool

1. Implement the method on `ReconTools` in `app/tools.py` (use `_query`/`_execute` helpers; add it to `dispatch()`).
2. Add a JSON Schema entry to `TOOL_DEFINITIONS` in `app/agent.py` so Claude can call it.
3. Optionally map it to a UI persona in `_TOOL_TO_AGENT`.

## Tuning agent behaviour

The decision logic lives entirely in `SYSTEM_PROMPT` in `app/agent.py` — edit the "Decision rules" block to change thresholds or add outcome conditions. The terminal verdict is parsed from the `DECISION: <OUTCOME>` line Claude emits (`MATCHED` / `REMEDIATED` / `FLAGGED` / `ESCALATED`); keep that contract intact.

## Configuration

All settings come from environment variables (or `.env` locally). Key overrides:

| Env var | Default | Purpose |
|---|---|---|
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Claude model used by the agent |
| `CATALOG` | `main` | Unity Catalog catalog |
| `SCHEMA_NAME` | `recon_demo` | Schema holding demo tables |
| `MAX_AGENT_ITERATIONS` | `10` | Loop guard before forced escalation |
| `MLFLOW_EXPERIMENT` | `/Shared/recon-agent` | MLflow experiment path |
