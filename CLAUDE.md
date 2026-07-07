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

Open <http://localhost:8000>. The demo tables (`main.recon_demo.orders_source` / `orders_warehouse`) must exist first — run `notebooks/01_setup_tables.py` in the Databricks workspace once.

There are no automated tests in this repo.

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

After deployment, grant the app's service principal access to the SQL Warehouse, Unity Catalog catalog/schema, and the 4 notebook jobs. See `SetupInstructions.md` for the full permission grant commands and common deployment gotchas (CLI version, `~/.databrickscfg` format, job name resolution, stale process flushing).

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

Active runs are held in the in-memory `RUNS` dict in `app/main.py`. When a run finishes, `_execute_run`/`_persist_audit` in `app/main.py` authoritatively writes the audit record to the `recon_audit_log` Delta table — with the real `run_id`, the parsed `outcome`, a server-measured `duration_seconds`, and issue counts derived from the run's `run_full_diff` results. (The agent does **not** write the audit log itself; relying on the LLM produced blank outcomes/durations and invented run IDs.) MLflow logging remains an agent-driven tool (`log_to_mlflow`).

## Key files

| File | Role |
|---|---|
| `app/agent.py` | Claude `tool_use` loop, `SYSTEM_PROMPT`, `TOOL_DEFINITIONS`, `_TOOL_TO_AGENT` persona map |
| `app/tools.py` | `ReconTools` class — 8 agent tools + the `write_recon_log` server-side audit writer, `dispatch()` router, `_query`/`_execute` SQL helpers |
| `app/main.py` | FastAPI routes, SSE streaming, in-memory `RUNS` state |
| `app/config.py` | `Settings` (pydantic-settings, env-driven, `lru_cache`'d) |
| `app/models.py` | `RunRequest`, `AgentEvent`, `ReconciliationResult` Pydantic models |
| `databricks.yml` | Asset Bundle: app resource + 4 notebook jobs (profiler, differ, backfill, verifier) |
| `app.yaml` | Databricks App manifest — wires secrets into `ANTHROPIC_API_KEY` and `DATABRICKS_WAREHOUSE_ID` |

## The 8 agent tools

| Tool | What it does |
|---|---|
| `compare_schemas` | DESCRIBE TABLE diff — finds columns only in one table, type mismatches |
| `get_row_counts` | COUNT(*) on both tables + absolute/pct delta |
| `run_column_profiler` | Per-column null_rate, distinct_count, min/max/avg |
| `run_full_diff` | Full outer join; classifies rows as MATCHED/SOURCE_ONLY/TARGET_ONLY/VALUE_MISMATCH |
| `investigate_nulls` | Per-day null distribution on a column; heuristic likely cause |
| `trigger_databricks_job` | Fuzzy-matches job name (tolerates DAB dev-mode prefixes), runs with notebook params, polls every 5 s |
| `log_to_mlflow` | Logs metrics/params to the configured MLflow experiment |
| `get_baseline_metrics` | Reads recent MLflow history to get avg/p95 null rate and avg mismatch rate for context |

`write_recon_log` exists on `ReconTools` but is **not** an agent tool — the app calls it server-side after each run (see the persistence note under Architecture).

## Agent decision outcomes

The terminal verdict is parsed from the exact `DECISION: <OUTCOME>` line Claude emits. Keep this contract intact when editing `SYSTEM_PROMPT`.

| Outcome | Meaning |
|---|---|
| `MATCHED` | Tables reconcile within tolerance; no action required |
| `REMEDIATED` | Discrepancy found and auto-fixed (backfill job ran and verifier passed) |
| `FLAGGED` | Discrepancy found but requires human approval before remediation |
| `ESCALATED` | Discrepancy found but Claude cannot determine a safe fix |

## Adding a tool

1. Implement the method on `ReconTools` in `app/tools.py` (use `_query`/`_execute` helpers; add it to `dispatch()`).
2. Add a JSON Schema entry to `TOOL_DEFINITIONS` in `app/agent.py` so Claude can call it.
3. Optionally map it to a UI persona in `_TOOL_TO_AGENT`.

## Tuning agent behaviour

The decision logic lives entirely in `SYSTEM_PROMPT` in `app/agent.py` — edit the "Decision rules" block to change thresholds or add outcome conditions.

## SSE event types

The `AgentEvent.event_type` field drives frontend rendering:

| Type | When emitted |
|---|---|
| `thinking` | Claude text block before a tool call |
| `tool_call` | A tool is about to be dispatched |
| `tool_result` | Tool returned; `data.metrics` carries key numbers for UI cards |
| `decision` | Terminal `DECISION:` line extracted from Claude output |
| `error` | Unhandled exception in the loop |
| `complete` | Loop ended with no tool calls (fallback) |
| `approval_required` | Agent paused before a gated tool; `data` carries the proposed `tool`+`input` |
| `approval_resolved` | Human decided; `data.decision` is `approve`/`reject` |

## Human-in-the-loop approval gate

Remediation is data-mutating, so `trigger_databricks_job` (the only tool in `_GATED_TOOLS` in `app/agent.py`) can be gated on human approval:

- `ReconciliationAgent.run(request, approval_hook=...)` yields an `approval_required` event, then `await`s the injected `approval_hook` before dispatching the tool. The event is yielded **before** the await so the SSE stream delivers the prompt before the agent parks.
- `_execute_run` in `app/main.py` supplies the hook, which blocks on `RunState.approval_event` (with an optional `approval_timeout_seconds` auto-reject). `POST /api/run/{run_id}/approve` sets `approval_decision` + fires the Event (`mode: gate_resolved`); with no gate pending it falls back to the legacy post-run relabel (`mode: post_run_relabel`).
- **Reject** feeds a synthetic "declined by operator" result back to Claude so it emits `DECISION: FLAGGED`/`ESCALATED` (recorded normally by `_persist_audit`). The gate never sets `outcome` directly.
- Enabled per-run via the `require_approval` field on `RunRequest` (UI checkbox, default on), globally gated by the `REQUIRE_APPROVAL_DEFAULT` setting. Ungated when no `approval_hook` is passed (backward compatible). The frontend renders the `#approval` panel and resends the decision to the same `/approve` endpoint; the panel is reconstructable on SSE reconnect because `approval_required`/`approval_resolved` are replayed from `state.events`.

## Credential detection in tools.py

`ReconTools._connect()` auto-detects the execution context:
- **Inside Databricks Apps**: uses the injected M2M OAuth token (no `DATABRICKS_TOKEN` needed).
- **Local dev**: reads `DATABRICKS_HOST` + `DATABRICKS_TOKEN` environment variables.

Never hardcode PATs; the M2M path is what makes secrets-in-`app.yaml` the only secret surface.

## Notebook jobs and their parameters

The 4 jobs deployed by the Asset Bundle each accept Databricks widget parameters:

| Job / Notebook | Key parameters |
|---|---|
| `02_profiler.py` | `catalog`, `schema`, `table`, `columns` (comma-separated) |
| `03_differ.py` | `catalog`, `schema`, `source_table`, `target_table`, `key_columns`, `tolerance_pct` |
| `04_backfill.py` | `source_table`, `target_table`, `partition_date` (YYYY-MM-DD), `key_columns` |
| `05_verifier.py` | `catalog`, `schema`, `source_table`, `target_table`, `key_columns` |

The verifier reads the most recent `diff_snapshots` row to compute pre/post delta — run the differ before the verifier or the baseline comparison will be empty.

## Configuration

All settings come from environment variables (or `.env` locally). Key overrides:

| Env var | Default | Purpose |
|---|---|---|
| `ANTHROPIC_MODEL` | `claude-sonnet-4-6` | Claude model used by the agent |
| `CATALOG` | `main` | Unity Catalog catalog |
| `SCHEMA_NAME` | `recon_demo` | Schema holding demo tables |
| `MAX_AGENT_ITERATIONS` | `10` | Loop guard before forced escalation |
| `MLFLOW_EXPERIMENT` | `/Shared/recon-agent` | MLflow experiment path |
| `REQUIRE_APPROVAL_DEFAULT` | `true` | Global kill-switch for the remediation approval gate (see HITL section) |
| `APPROVAL_TIMEOUT_SECONDS` | `0` | Seconds to wait for human approval before auto-rejecting; `0` = wait forever |
