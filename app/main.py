"""FastAPI entrypoint for the Agentic Data Reconciliation Databricks App.

Serves the single-page UI, exposes the table catalog and run-history endpoints,
accepts reconciliation run requests, and streams agent events to the browser via
Server-Sent Events. Active runs execute in background asyncio tasks; their event
streams are buffered in-memory and replayed to the SSE consumer.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from .agent import ReconciliationAgent
from .config import get_settings
from .models import AgentEvent, RunRequest
from .tools import ReconTools

settings = get_settings()
app = FastAPI(title="Agentic Data Reconciliation", version="1.0.0")

# Permissive CORS for the demo.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


class RunState:
    """In-memory state for a single reconciliation run.

    Holds an asyncio.Queue of streamed events plus the accumulated event list so
    a completed run can be replayed from history.
    """

    def __init__(self, request: RunRequest) -> None:
        self.request = request
        self.queue: asyncio.Queue[AgentEvent | None] = asyncio.Queue()
        self.events: list[AgentEvent] = []
        self.done = False
        self.outcome: str | None = None


# Keyed by run_id (UUID).
RUNS: dict[str, RunState] = {}


async def _execute_run(run_id: str) -> None:
    """Background task: drive the agent and push events into the run's queue."""
    state = RUNS[run_id]
    agent = ReconciliationAgent(settings)
    try:
        async for event in agent.run(state.request):
            state.events.append(event)
            if event.event_type == "decision" and event.data:
                state.outcome = event.data.get("outcome")
            await state.queue.put(event)
    finally:
        state.done = True
        # Sentinel signals the SSE generator to close.
        await state.queue.put(None)


# --------------------------------------------------------------------------- #
# Static + health
# --------------------------------------------------------------------------- #
@app.get("/")
async def index() -> FileResponse:
    """Serve the single-page UI."""
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/health")
async def health() -> dict:
    """Liveness probe; reports the detected workspace URL."""
    workspace = ""
    try:
        from databricks.sdk import WorkspaceClient

        workspace = WorkspaceClient().config.host or ""
    except Exception:  # noqa: BLE001
        workspace = settings.databricks_host
    return {"status": "ok", "workspace": workspace}


# --------------------------------------------------------------------------- #
# Tables
# --------------------------------------------------------------------------- #
@app.get("/api/tables")
async def list_tables() -> JSONResponse:
    """List Delta tables available in the configured catalog.schema."""
    try:
        tools = ReconTools(settings)
        rows = await asyncio.to_thread(
            tools._query,
            f"SHOW TABLES IN {settings.catalog}.{settings.schema_name}",
        )
        tables = [
            f"{settings.catalog}.{settings.schema_name}.{r.get('tableName')}"
            for r in rows
            if r.get("tableName")
        ]
        return JSONResponse({"tables": tables})
    except Exception as exc:  # noqa: BLE001
        # Fall back to the demo tables so the UI is always usable.
        prefix = f"{settings.catalog}.{settings.schema_name}"
        return JSONResponse(
            {
                "tables": [f"{prefix}.orders_source", f"{prefix}.orders_warehouse"],
                "error": str(exc),
            }
        )


# --------------------------------------------------------------------------- #
# Runs
# --------------------------------------------------------------------------- #
@app.post("/api/run", status_code=202)
async def start_run(request: RunRequest) -> JSONResponse:
    """Accept a run request and start it in the background. Returns the run_id."""
    run_id = str(uuid.uuid4())
    RUNS[run_id] = RunState(request)
    asyncio.create_task(_execute_run(run_id))
    return JSONResponse({"run_id": run_id}, status_code=202)


@app.get("/api/run/{run_id}/stream")
async def stream_run(run_id: str) -> EventSourceResponse:
    """Stream a run's AgentEvent objects to the browser as SSE."""
    if run_id not in RUNS:
        raise HTTPException(status_code=404, detail="run not found")

    state = RUNS[run_id]

    async def event_generator() -> AsyncGenerator[dict, None]:
        # Replay any events that fired before the consumer connected.
        for event in list(state.events):
            yield {"data": event.model_dump_json()}
        if state.done:
            return
        while True:
            event = await state.queue.get()
            if event is None:  # sentinel
                break
            yield {"data": event.model_dump_json()}

    return EventSourceResponse(event_generator())


@app.get("/api/runs")
async def list_runs() -> JSONResponse:
    """List the last 20 runs from the recon_audit_log Delta table."""
    try:
        tools = ReconTools(settings)
        rows = await asyncio.to_thread(
            tools._query,
            f"""
            SELECT run_id, outcome, issues_found, issues_resolved,
                   duration_seconds, source_table, target_table,
                   CAST(logged_at AS STRING) AS logged_at
            FROM {settings.fq_audit_table}
            ORDER BY logged_at DESC
            LIMIT 20
            """,
        )
        return JSONResponse({"runs": rows})
    except Exception as exc:  # noqa: BLE001
        return JSONResponse({"runs": [], "error": str(exc)})


@app.get("/api/run/{run_id}")
async def get_run(run_id: str) -> JSONResponse:
    """Return a single run's events (in-memory) or audit-log record."""
    if run_id in RUNS:
        state = RUNS[run_id]
        return JSONResponse(
            {
                "run_id": run_id,
                "done": state.done,
                "outcome": state.outcome,
                "request": state.request.model_dump(),
                "events": [e.model_dump() for e in state.events],
            }
        )
    # Not in memory — look in the audit log.
    try:
        tools = ReconTools(settings)
        rows = await asyncio.to_thread(
            tools._query,
            f"SELECT result_json FROM {settings.fq_audit_table} "
            f"WHERE run_id = '{run_id}' LIMIT 1",
        )
        if rows:
            return JSONResponse({"run_id": run_id, "result": json.loads(rows[0]["result_json"])})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=404, detail=f"run not found: {exc}")
    raise HTTPException(status_code=404, detail="run not found")


@app.post("/api/run/{run_id}/approve")
async def approve_run(run_id: str, request: Request) -> JSONResponse:
    """Record a human decision (approve/reject) for a FLAGGED run."""
    body = await request.json()
    decision = body.get("decision", "approve")
    if run_id not in RUNS:
        raise HTTPException(status_code=404, detail="run not found")
    state = RUNS[run_id]
    state.outcome = "REMEDIATED" if decision == "approve" else "ESCALATED"
    return JSONResponse({"run_id": run_id, "decision": decision, "outcome": state.outcome})
