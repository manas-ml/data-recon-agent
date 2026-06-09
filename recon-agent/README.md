# Agentic Data Reconciliation — Claude × Databricks

A production-ready demo of an **agentic data reconciliation** system where
**Claude AI** acts as the orchestrating agent — observing data-quality issues,
calling Databricks tools, reasoning about findings, and deciding whether to
auto-remediate, flag for human review, or escalate. The entire application runs
natively inside Databricks as a **Databricks App** (FastAPI + a single-page
browser UI), with Claude driving a `tool_use` loop over real Delta Lake tables,
Databricks Jobs, and MLflow.

```
recon-agent/
├── databricks.yml          # Asset Bundle: app + 4 notebook jobs
├── app.yaml                # Databricks App manifest (secrets-wired env)
├── requirements.txt        # Python dependencies
├── app/
│   ├── main.py             # FastAPI entrypoint (+ SSE streaming)
│   ├── agent.py            # Claude agent loop with tool_use
│   ├── tools.py            # 9 Databricks tool implementations
│   ├── config.py           # Settings (env-driven)
│   ├── models.py           # Pydantic request/response models
│   └── static/             # index.html · style.css · app.js
└── notebooks/
    ├── 01_setup_tables.py  # Create source + target tables with drift
    ├── 02_profiler.py      # Column profiling
    ├── 03_differ.py        # Full outer join diff
    ├── 04_backfill.py      # Partition backfill (MERGE)
    └── 05_verifier.py      # Post-remediation verification
```

---

## Prerequisites

- A **Databricks workspace** with **Unity Catalog** enabled
- **DBR 14.3 LTS** (or above) for the notebook jobs
- **Databricks CLI v0.220+** installed locally
- An **Anthropic API key** (Claude)
- A **SQL Warehouse** (note its warehouse ID)

---

## Setup (step by step)

1. **Clone the repo**

   ```bash
   git clone <your-fork-url> && cd recon-agent
   ```

2. **Configure the Databricks CLI** (workspace URL + token)

   ```bash
   databricks configure
   ```

3. **Create the secret scope** and add the two secrets the app reads:

   ```bash
   databricks secrets create-scope recon-agent
   databricks secrets put-secret recon-agent anthropic-api-key --string-value YOUR_ANTHROPIC_KEY
   databricks secrets put-secret recon-agent warehouse-id    --string-value YOUR_WAREHOUSE_ID
   ```

4. **Create the demo tables** — run `notebooks/01_setup_tables.py` in the
   workspace (or as a one-off job). It writes `orders_source` and
   `orders_warehouse` into `main.recon_demo` with seeded drift.

5. **Adjust the catalog/schema** if needed. Defaults live in `app/config.py`
   (`catalog = "main"`, `schema_name = "recon_demo"`) and can be overridden with
   the `CATALOG` / `SCHEMA_NAME` environment variables.

6. **Deploy the bundle and the app**

   ```bash
   databricks bundle deploy -t dev
   databricks apps deploy recon-agent
   ```

7. **Open the App URL** from the Databricks Apps console and click **Run Agent**.

---

## Architecture

```
  ┌──────────┐    HTTPS / SSE    ┌──────────────────────────────────────┐
  │ Browser  │ ◀───────────────▶ │  Databricks App (native hosting)     │
  │  (UI)    │                   │  ┌────────────────────────────────┐  │
  └──────────┘                   │  │ FastAPI (app/main.py)          │  │
                                 │  │   /api/run  → background task  │  │
                                 │  │   /api/.../stream → SSE         │  │
                                 │  └───────────────┬────────────────┘  │
                                 │                  │                    │
                                 │     ┌────────────▼─────────────┐      │
                                 │     │ ReconciliationAgent      │      │
                                 │     │  (Claude tool_use loop)  │      │
                                 │     └────────────┬─────────────┘      │
                                 └──────────────────┼────────────────────┘
                                                    │ tool calls
              ┌────────────────────┬────────────────┼───────────────┬──────────────┐
              ▼                    ▼                ▼               ▼              ▼
        ┌───────────┐      ┌──────────────┐  ┌────────────┐  ┌────────────┐ ┌──────────┐
        │ Claude API│      │ Databricks   │  │ Databricks │  │ Delta Lake │ │  MLflow  │
        │ (tool_use)│      │ SQL Warehouse│  │ Jobs (SDK) │  │ Unity Cat. │ │ Tracking │
        └───────────┘      └──────────────┘  └────────────┘  └────────────┘ └──────────┘
```

**Flow:** the browser POSTs a `RunRequest` → FastAPI spawns a background task
running the Claude agent loop → Claude reasons and calls tools (`compare_schemas`,
`get_row_counts`, `run_full_diff`, `investigate_nulls`, `trigger_databricks_job`,
…) → each step is streamed to the UI as a Server-Sent Event → Claude reaches a
verdict (MATCHED / REMEDIATED / FLAGGED / ESCALATED) → the result is logged to
MLflow and the `recon_audit_log` Delta table.

---

## The 9 agent tools

| Tool                       | What it does                                                      |
| -------------------------- | ---------------------------------------------------------------- |
| `compare_schemas`          | `DESCRIBE TABLE` diff — column/type differences                  |
| `get_row_counts`           | `COUNT(*)` on both tables + delta / delta %                      |
| `run_column_profiler`      | null_rate, distinct_count, min/max/avg per column                |
| `run_full_diff`            | Full outer join classification (MATCHED / *_ONLY / VALUE_MISMATCH)|
| `investigate_nulls`        | Temporal clustering of nulls → likely cause                      |
| `trigger_databricks_job`   | `run_now()` a named job and poll to completion                   |
| `log_to_mlflow`            | Log metrics/params to the MLflow experiment                      |
| `write_recon_log`          | Append the result to the `recon_audit_log` Delta table           |
| `get_baseline_metrics`     | Read recent MLflow runs to establish a baseline                  |

---

## Running locally (for development)

The app runs anywhere the Databricks SDK can authenticate (PAT works fine).

```bash
pip install -r requirements.txt

export ANTHROPIC_API_KEY=sk-ant-...
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>

uvicorn app.main:app --reload
```

Then open <http://localhost:8000>. The UI loads tables from
`main.recon_demo`; run `notebooks/01_setup_tables.py` first so they exist.

> The agent uses the Claude model `claude-sonnet-4-20250514` (set in
> `app/config.py` → `anthropic_model`). Override with the `ANTHROPIC_MODEL`
> environment variable.

---

## Customizing for your tables

**Point at real tables.** In the UI just pick any two tables from the dropdowns
(populated from `SHOW TABLES IN {catalog}.{schema}`). To change the default
catalog/schema, set `CATALOG` / `SCHEMA_NAME` env vars or edit `app/config.py`.
The agent works on any source/target pair that shares join keys — no code change
needed.

**Add a custom tool.** Two steps:

1. Implement the method on `ReconTools` in `app/tools.py` (give it a docstring,
   type hints, and use the `_query` / `_execute` helpers) and add it to the
   `dispatch()` routing dict.
2. Add the matching JSON-Schema definition to `TOOL_DEFINITIONS` in
   `app/agent.py` so Claude can see and call it. Optionally map it to a UI
   pipeline persona in `_TOOL_TO_AGENT`.

**Tune the decision rules.** The agent's behaviour is governed by the
`SYSTEM_PROMPT` in `app/agent.py`. Edit the "Decision rules" block to change the
thresholds (tolerance, null-rate multipliers, which columns are "critical") or
to add new outcome conditions. The terminal verdict is parsed from the
`DECISION: <OUTCOME>` line Claude emits, so keep that contract intact.

---

## Notes & safety

- **No hardcoded secrets.** `ANTHROPIC_API_KEY` and `DATABRICKS_WAREHOUSE_ID`
  come from the `recon-agent` secret scope via `app.yaml`. Databricks workspace
  auth is auto-detected by the SDK inside Databricks Apps.
- **Iteration guard.** The loop stops after `max_agent_iterations` (default 10)
  and escalates rather than looping forever.
- **SSE lifecycle.** The stream stays open until a `complete` event; the frontend
  `EventSource` closes cleanly on `complete` or `error`.
