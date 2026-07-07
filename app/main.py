"""FastAPI entrypoint for the Agentic Data Reconciliation Databricks App.

Serves the single-page UI, exposes the table catalog and run-history endpoints,
accepts reconciliation run requests, and streams agent events to the browser via
Server-Sent Events. Active runs execute in background asyncio tasks; their event
streams are buffered in-memory and replayed to the SSE consumer.
"""

from __future__ import annotations

import asyncio
import json
import time
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
        # Human-in-the-loop gate: the agent parks on this Event before a gated
        # (data-mutating) tool; the /approve endpoint sets the decision + fires
        # the Event to resume it.
        self.approval_event: asyncio.Event = asyncio.Event()
        self.pending_approval: dict | None = None  # descriptor shown to the human
        self.approval_decision: str | None = None  # "approve" | "reject"


# Keyed by run_id (UUID).
RUNS: dict[str, RunState] = {}


async def _execute_run(run_id: str) -> None:
    """Background task: drive the agent and push events into the run's queue."""
    state = RUNS[run_id]
    agent = ReconciliationAgent(settings)
    started = time.monotonic()
    # Issue counts derived from each run_full_diff the agent runs, in order:
    # (source_only + target_only + value_mismatch).
    diff_totals: list[int] = []

    async def approval_hook(descriptor: dict) -> str:
        """Block until a human approves/rejects the proposed remediation.

        Clears the Event here (not in /approve) so repeated gated calls in one
        run can't reuse an already-set Event. Auto-rejects on timeout when
        approval_timeout_seconds > 0 so an abandoned run escalates instead of
        hanging forever.
        """
        state.pending_approval = descriptor
        state.approval_decision = None
        state.approval_event.clear()
        try:
            timeout = settings.approval_timeout_seconds
            if timeout and timeout > 0:
                await asyncio.wait_for(state.approval_event.wait(), timeout=timeout)
            else:
                await state.approval_event.wait()
        except asyncio.TimeoutError:
            state.approval_decision = "reject"
        finally:
            state.pending_approval = None
        return state.approval_decision or "reject"

    try:
        async for event in agent.run(state.request, approval_hook=approval_hook):
            state.events.append(event)
            if event.event_type == "decision" and event.data:
                state.outcome = event.data.get("outcome")
            elif (
                event.event_type == "tool_result"
                and event.data
                and event.data.get("tool") == "run_full_diff"
                and isinstance(event.data.get("result"), dict)
            ):
                r = event.data["result"]
                diff_totals.append(
                    int(r.get("source_only", 0) or 0)
                    + int(r.get("target_only", 0) or 0)
                    + int(r.get("value_mismatch", 0) or 0)
                )
            await state.queue.put(event)
    finally:
        # Authoritatively persist the audit record server-side. The agent no
        # longer writes it — the LLM lacked the finalized outcome, a clock, and
        # the real run_id — so outcome/duration/counts are computed here.
        await _persist_audit(run_id, state, diff_totals, round(time.monotonic() - started, 2))
        state.done = True
        # Sentinel signals the SSE generator to close.
        await state.queue.put(None)


async def _persist_audit(
    run_id: str, state: RunState, diff_totals: list[int], duration: float
) -> None:
    """Write the definitive run result to recon_audit_log (best-effort)."""
    issues_found = diff_totals[0] if diff_totals else 0
    if len(diff_totals) >= 2:
        # A re-diff ran after remediation: resolved = how much the count dropped.
        issues_resolved = max(0, diff_totals[0] - diff_totals[-1])
    elif state.outcome == "REMEDIATED":
        issues_resolved = issues_found
    else:
        issues_resolved = 0

    result = {
        "run_id": run_id,
        "outcome": state.outcome or "ESCALATED",
        "issues_found": issues_found,
        "issues_resolved": issues_resolved,
        "duration_seconds": duration,
        "source_table": state.request.source_table,
        "target_table": state.request.target_table,
        "trigger_mode": state.request.trigger_mode,
        "key_columns": state.request.key_columns,
        "diff_totals": diff_totals,
    }
    try:
        tools = ReconTools(settings)
        await asyncio.to_thread(tools.write_recon_log, result)
    except Exception as exc:  # noqa: BLE001
        # An audit-write failure must never crash the run.
        print(f"[audit] failed to persist run {run_id}: {exc}")


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
async def list_runs(limit: int = 20) -> JSONResponse:
    """List recent runs from the recon_audit_log Delta table.

    The sidebar uses the default (20); the Audit Log view requests more.
    """
    limit = max(1, min(limit, 500))
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
            LIMIT {limit}
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
    """Handle a human approve/reject decision.

    Two modes:
    (a) gate_resolved — a remediation gate is actively pending: unblock the
        agent so it runs (approve) or skips (reject) the job. The final outcome
        comes from Claude's post-resume DECISION line, so we do not set it here.
    (b) post_run_relabel — no gate pending (legacy path): relabel the finished
        run's outcome directly.
    """
    body = await request.json()
    decision = body.get("decision", "approve")
    if run_id not in RUNS:
        raise HTTPException(status_code=404, detail="run not found")
    state = RUNS[run_id]

    if state.pending_approval is not None and not state.approval_event.is_set():
        state.approval_decision = "approve" if decision == "approve" else "reject"
        state.approval_event.set()
        return JSONResponse(
            {"run_id": run_id, "decision": decision, "mode": "gate_resolved"}
        )

    state.outcome = "REMEDIATED" if decision == "approve" else "ESCALATED"
    return JSONResponse(
        {
            "run_id": run_id,
            "decision": decision,
            "outcome": state.outcome,
            "mode": "post_run_relabel",
        }
    )
