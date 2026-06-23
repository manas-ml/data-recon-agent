"""Claude-driven reconciliation agent.

This module contains the heart of the application: a tool_use agentic loop that
lets Claude observe data-quality issues, call Databricks tools, reason about the
findings, and decide whether to auto-remediate, flag for review, or escalate.

The loop is exposed as an async generator (`ReconciliationAgent.run`) yielding
`AgentEvent` objects for Server-Sent-Events streaming. The Anthropic SDK calls
and the (synchronous) Databricks tools are run via `asyncio.to_thread` so the
FastAPI event loop is never blocked.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncGenerator

import anthropic

from .config import Settings
from .models import AgentEvent, RunRequest
from .tools import ReconTools

# --------------------------------------------------------------------------- #
# Tool schemas exposed to Claude (JSON Schema / tool_use format).
# --------------------------------------------------------------------------- #
TOOL_DEFINITIONS = [
    {
        "name": "compare_schemas",
        "description": (
            "Compare the schemas of the source and target tables. Returns columns "
            "only in source, only in target, type mismatches, and common columns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_table": {"type": "string", "description": "Source table name."},
                "target_table": {"type": "string", "description": "Target table name."},
            },
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "get_row_counts",
        "description": (
            "Get COUNT(*) for both tables plus the absolute and percentage delta. "
            "Use this first to gauge the scale of any discrepancy."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_table": {"type": "string"},
                "target_table": {"type": "string"},
            },
            "required": ["source_table", "target_table"],
        },
    },
    {
        "name": "run_column_profiler",
        "description": (
            "Profile columns of a table: null_rate, distinct_count, min, max, avg. "
            "Use to detect anomalous null spikes or value drift."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Columns to profile.",
                },
            },
            "required": ["table", "columns"],
        },
    },
    {
        "name": "run_full_diff",
        "description": (
            "Full outer join the two tables on key columns and classify every row "
            "as MATCHED, SOURCE_ONLY, TARGET_ONLY, or VALUE_MISMATCH. Returns counts "
            "and a sample of mismatching keys."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source_table": {"type": "string"},
                "target_table": {"type": "string"},
                "key_columns": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["source_table", "target_table", "key_columns"],
        },
    },
    {
        "name": "investigate_nulls",
        "description": (
            "Find temporal clustering of nulls in a column by load_date. Returns a "
            "per-day null distribution and a heuristic likely cause (e.g. a failed "
            "partition load)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "column": {"type": "string"},
            },
            "required": ["table", "column"],
        },
    },
    {
        "name": "trigger_databricks_job",
        "description": (
            "Trigger a named Databricks job (e.g. the backfill or verifier notebook) "
            "with parameters and poll until it terminates. Returns run status, "
            "duration, and the notebook's exit output."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_name": {"type": "string"},
                "parameters": {
                    "type": "object",
                    "description": "Notebook widget parameters as key/value pairs.",
                    "additionalProperties": {"type": "string"},
                },
            },
            "required": ["job_name", "parameters"],
        },
    },
    {
        "name": "log_to_mlflow",
        "description": "Log reconciliation metrics and params to the MLflow experiment.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "metrics": {"type": "object", "additionalProperties": {"type": "number"}},
                "params": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["run_id", "metrics", "params"],
        },
    },
    {
        "name": "write_recon_log",
        "description": (
            "Write the structured reconciliation result to the Delta audit log "
            "table (creates the table if needed)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "result": {
                    "type": "object",
                    "description": "The reconciliation result document.",
                }
            },
            "required": ["result"],
        },
    },
    {
        "name": "get_baseline_metrics",
        "description": (
            "Read the last N days of MLflow runs for a table to establish baseline "
            "avg_null_rate, p95_null_rate, and avg_mismatch_rate for comparison."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table": {"type": "string"},
                "days": {"type": "integer", "default": 30},
            },
            "required": ["table"],
        },
    },
]

SYSTEM_PROMPT = """\
You are an expert data reconciliation agent running on Databricks. Your job is to:
1. Investigate data quality issues between a source and target Delta table
2. Use the available tools to gather evidence before drawing conclusions
3. Remediate issues you are confident about (auto-fix)
4. Flag issues that require human judgment
5. Escalate if mismatch rates exceed tolerance across critical columns

Decision rules:
- If row delta < tolerance_pct AND caused by identifiable partition failure -> AUTO-REMEDIATE via backfill
- If null rate increase < 2x baseline AND column is non-critical -> TOLERATE, log only
- If null rate increase > 5x baseline OR column is revenue/order_id -> FLAG for review
- If mismatch rate > tolerance_pct AND persists after remediation -> ESCALATE
- Always re-verify after any remediation before declaring success
- Always explain your reasoning step by step before calling a tool
- Always compare findings against baseline metrics from MLflow

You have access to these tools: compare_schemas, get_row_counts, run_column_profiler,
run_full_diff, investigate_nulls, trigger_databricks_job, log_to_mlflow,
write_recon_log, get_baseline_metrics.

When you have reached a final conclusion, respond with plain text (no tool calls)
that begins with a line "DECISION: <OUTCOME>" where <OUTCOME> is exactly one of
MATCHED, REMEDIATED, FLAGGED, or ESCALATED, followed by a concise justification.

Current run context: {run_context}
"""

# Map agent "personas" to UI pipeline steps. The agent narrates which sub-agent
# is acting; the frontend lights up the corresponding step.
_TOOL_TO_AGENT = {
    "compare_schemas": "Ingestion Agent",
    "get_row_counts": "Ingestion Agent",
    "run_column_profiler": "Profiling Agent",
    "investigate_nulls": "Profiling Agent",
    "get_baseline_metrics": "Profiling Agent",
    "run_full_diff": "Reconciliation Agent",
    "trigger_databricks_job": "Remediation Agent",
    "log_to_mlflow": "Reporting Agent",
    "write_recon_log": "Reporting Agent",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ReconciliationAgent:
    """Drives the Claude tool_use loop for a single reconciliation run."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        self.tools = ReconTools(settings)

    async def run(self, request: RunRequest) -> AsyncGenerator[AgentEvent, None]:
        """Run the agentic loop, yielding AgentEvent objects for SSE streaming."""
        step = 0
        run_context = json.dumps(request.model_dump())
        system = SYSTEM_PROMPT.format(run_context=run_context)

        messages: list[dict] = [
            {
                "role": "user",
                "content": (
                    "Begin the reconciliation. Investigate the discrepancy between "
                    f"source table `{request.source_table}` and target table "
                    f"`{request.target_table}` (keys: {', '.join(request.key_columns)}, "
                    f"tolerance: {request.tolerance_pct}%). Gather evidence with the "
                    "tools, compare against baseline, then decide."
                ),
            }
        ]

        try:
            for _iteration in range(self.settings.max_agent_iterations):
                response = await asyncio.to_thread(
                    self.client.messages.create,
                    model=self.settings.anthropic_model,
                    max_tokens=4096,
                    system=system,
                    tools=TOOL_DEFINITIONS,
                    messages=messages,
                )

                # Surface Claude's reasoning text (thinking) and gather tool calls.
                tool_use_blocks = []
                for block in response.content:
                    if block.type == "text" and block.text.strip():
                        step += 1
                        # A "DECISION:" line marks the terminal verdict.
                        if "DECISION:" in block.text:
                            outcome = self._parse_outcome(block.text)
                            yield AgentEvent(
                                event_type="decision",
                                agent_name="Reporting Agent",
                                message=block.text.strip(),
                                data={"outcome": outcome},
                                timestamp=_now(),
                                step=step,
                            )
                        else:
                            yield AgentEvent(
                                event_type="thinking",
                                agent_name="Reconciliation Agent",
                                message=block.text.strip(),
                                data=None,
                                timestamp=_now(),
                                step=step,
                            )
                    elif block.type == "tool_use":
                        tool_use_blocks.append(block)

                # No tool calls -> Claude has finished. Emit completion.
                if not tool_use_blocks:
                    if response.stop_reason != "tool_use":
                        step += 1
                        yield AgentEvent(
                            event_type="complete",
                            agent_name="Reporting Agent",
                            message="Reconciliation run complete.",
                            data=None,
                            timestamp=_now(),
                            step=step,
                        )
                        return

                # Append the assistant turn (text + tool_use) to history verbatim.
                messages.append({"role": "assistant", "content": response.content})

                # Execute each tool call and collect tool_result blocks.
                tool_results = []
                for tu in tool_use_blocks:
                    agent_name = _TOOL_TO_AGENT.get(tu.name, "Reconciliation Agent")
                    step += 1
                    yield AgentEvent(
                        event_type="tool_call",
                        agent_name=agent_name,
                        message=f"Calling {tu.name}",
                        data={"tool": tu.name, "input": tu.input},
                        timestamp=_now(),
                        step=step,
                    )

                    try:
                        result = await asyncio.to_thread(
                            self.tools.dispatch, tu.name, dict(tu.input)
                        )
                    except Exception as exc:  # noqa: BLE001
                        result = {"error": str(exc)}

                    step += 1
                    yield AgentEvent(
                        event_type="tool_result",
                        agent_name=agent_name,
                        message=f"{tu.name} returned",
                        data={
                            "tool": tu.name,
                            "result": result,
                            "metrics": self._extract_metrics(tu.name, result),
                        },
                        timestamp=_now(),
                        step=step,
                    )

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tu.id,
                            "content": json.dumps(result, default=str),
                        }
                    )

                # Feed tool results back as the next user turn.
                messages.append({"role": "user", "content": tool_results})

            # Iteration guard hit -> escalate.
            step += 1
            yield AgentEvent(
                event_type="decision",
                agent_name="Reporting Agent",
                message=(
                    "DECISION: ESCALATED — reached the maximum number of agent "
                    "iterations without a confident resolution."
                ),
                data={"outcome": "ESCALATED"},
                timestamp=_now(),
                step=step,
            )
            step += 1
            yield AgentEvent(
                event_type="complete",
                agent_name="Reporting Agent",
                message="Reconciliation run complete (iteration guard).",
                data=None,
                timestamp=_now(),
                step=step,
            )

        except Exception as exc:  # noqa: BLE001
            step += 1
            yield AgentEvent(
                event_type="error",
                agent_name="Reconciliation Agent",
                message=f"Agent run failed: {exc}",
                data={"error": str(exc)},
                timestamp=_now(),
                step=step,
            )
            yield AgentEvent(
                event_type="complete",
                agent_name="Reporting Agent",
                message="Reconciliation run ended with an error.",
                data=None,
                timestamp=_now(),
                step=step + 1,
            )

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _parse_outcome(text: str) -> str:
        """Extract the outcome token from a 'DECISION: <OUTCOME>' line."""
        valid = {"MATCHED", "REMEDIATED", "FLAGGED", "ESCALATED"}
        for token in text.replace(":", " ").replace("—", " ").split():
            up = token.strip().upper()
            if up in valid:
                return up
        return "FLAGGED"

    @staticmethod
    def _extract_metrics(tool_name: str, result: dict) -> dict | None:
        """Derive the four UI metric-card values from a tool result when possible.

        Keys: row_match_pct, null_rate, mismatches, source_only.
        """
        if not isinstance(result, dict):
            return None
        metrics: dict = {}
        if tool_name == "get_row_counts":
            src = result.get("source_count", 0)
            tgt = result.get("target_count", 0)
            if src:
                metrics["row_match_pct"] = round(min(src, tgt) / src * 100.0, 2)
            metrics["source_only"] = max(result.get("delta", 0), 0)
        elif tool_name == "run_full_diff":
            total = (
                result.get("matched", 0)
                + result.get("source_only", 0)
                + result.get("target_only", 0)
                + result.get("value_mismatch", 0)
            )
            if total:
                metrics["row_match_pct"] = round(
                    result.get("matched", 0) / total * 100.0, 2
                )
            metrics["mismatches"] = result.get("value_mismatch", 0)
            metrics["source_only"] = result.get("source_only", 0)
        elif tool_name == "run_column_profiler":
            profile = result.get("profile", {})
            if profile:
                worst = max((c.get("null_rate", 0) for c in profile.values()), default=0)
                metrics["null_rate"] = round(worst * 100.0, 3)
        return metrics or None
