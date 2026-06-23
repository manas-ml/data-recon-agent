"""Pydantic request/response models for the reconciliation agent API.

These models define the contract between the browser UI, the FastAPI backend,
and the SSE event stream emitted by the agent loop.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class RunRequest(BaseModel):
    """Payload to kick off a reconciliation run."""

    source_table: str = Field(..., description="Fully-qualified source table name.")
    target_table: str = Field(..., description="Fully-qualified target table name.")
    key_columns: list[str] = Field(..., description="Join key columns for diffing.")
    tolerance_pct: float = Field(
        2.0, description="Allowed row/mismatch delta before escalation, as a percent."
    )
    trigger_mode: str = Field(
        "on_demand", description="How the run was triggered (on_demand, scheduled, ...)."
    )


class AgentEvent(BaseModel):
    """A single streamed event from the agent loop, rendered live in the UI."""

    event_type: Literal[
        "thinking", "tool_call", "tool_result", "decision", "error", "complete"
    ]
    agent_name: str
    message: str
    data: Optional[dict] = None
    timestamp: str
    step: int


class ReconciliationResult(BaseModel):
    """Final structured outcome of a reconciliation run."""

    run_id: str
    outcome: Literal["MATCHED", "REMEDIATED", "FLAGGED", "ESCALATED"]
    issues_found: int
    issues_resolved: int
    duration_seconds: float
    metrics: dict
