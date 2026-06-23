/*
 * Agentic Data Reconciliation — frontend logic.
 *
 * Responsibilities:
 *   - Populate the table dropdowns from /api/tables.
 *   - Start a run (POST /api/run) and consume its SSE stream in real time.
 *   - Render each AgentEvent into the reasoning feed, drive the 5-step pipeline,
 *     update the four live metric cards, and render the final decision verdict.
 *   - Maintain the recent-runs list and replay past runs.
 */

"use strict";

const $ = (id) => document.getElementById(id);

// Map agent persona names to pipeline step numbers.
const AGENT_STEP = {
  "Ingestion Agent": 1,
  "Profiling Agent": 2,
  "Reconciliation Agent": 3,
  "Remediation Agent": 4,
  "Reporting Agent": 5,
};

const ICONS = {
  thinking: "💭",
  tool_call: "⚡",
  tool_result: "✓",
  decision: "🎯",
  error: "✗",
};

let evtSource = null;
let currentRunId = null;

// ----------------------------------------------------------------------- //
// Table loading
// ----------------------------------------------------------------------- //
async function loadTables() {
  try {
    const res = await fetch("/api/tables");
    const data = await res.json();
    const tables = data.tables || [];
    const src = $("source-table");
    const tgt = $("target-table");
    src.innerHTML = "";
    tgt.innerHTML = "";
    tables.forEach((t) => {
      src.add(new Option(t, t));
      tgt.add(new Option(t, t));
    });
    if (tables.length > 0) src.selectedIndex = 0;
    if (tables.length > 1) tgt.selectedIndex = 1;
  } catch (e) {
    console.error("Failed to load tables", e);
  }
}

async function loadWorkspace() {
  try {
    const res = await fetch("/health");
    const data = await res.json();
    if (data.workspace) {
      $("workspace-url").textContent =
        "workspace: " + data.workspace.replace(/^https?:\/\//, "");
    }
  } catch (_) {
    /* ignore */
  }
}

// ----------------------------------------------------------------------- //
// Run execution
// ----------------------------------------------------------------------- //
async function runAgent() {
  resetUI();
  setStatus("live", "running");
  $("run-btn").disabled = true;
  $("typing").classList.remove("hidden");

  const payload = {
    source_table: $("source-table").value,
    target_table: $("target-table").value,
    key_columns: $("key-columns")
      .value.split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    tolerance_pct: parseFloat($("tolerance").value) || 2.0,
    trigger_mode: $("trigger-mode").value,
  };

  try {
    const res = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    currentRunId = data.run_id;
    openStream(currentRunId);
  } catch (e) {
    setStatus("error", "error");
    $("run-btn").disabled = false;
    $("typing").classList.add("hidden");
    console.error(e);
  }
}

function openStream(runId) {
  if (evtSource) evtSource.close();
  evtSource = new EventSource(`/api/run/${runId}/stream`);

  evtSource.onmessage = (msg) => {
    let event;
    try {
      event = JSON.parse(msg.data);
    } catch (_) {
      return;
    }
    renderEvent(event);

    if (event.event_type === "complete") {
      evtSource.close();
      $("run-btn").disabled = false;
      $("typing").classList.add("hidden");
      setStatus("idle", "idle");
      markRemainingStepsDone();
      refreshRecentRuns();
    }
    if (event.event_type === "error") {
      setStatus("error", "error");
    }
  };

  evtSource.onerror = () => {
    if (evtSource) evtSource.close();
    $("run-btn").disabled = false;
    $("typing").classList.add("hidden");
  };
}

// ----------------------------------------------------------------------- //
// Event rendering
// ----------------------------------------------------------------------- //
function renderEvent(event) {
  updateSteps(event.agent_name);

  const item = document.createElement("div");
  item.className = `feed-item ${event.event_type}`;

  const head = document.createElement("div");
  head.className = "fi-head";
  head.innerHTML = `<span>${ICONS[event.event_type] || "•"}</span>
    <span>${escapeHtml(titleFor(event))}</span>
    <span class="fi-agent">${escapeHtml(event.agent_name)} · step ${event.step}</span>`;
  item.appendChild(head);

  const body = document.createElement("div");
  body.className = "fi-body";
  body.textContent = event.message;
  item.appendChild(body);

  // Tool-call: show the formatted input JSON.
  if (event.event_type === "tool_call" && event.data && event.data.input) {
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify(event.data.input, null, 2);
    item.appendChild(pre);
  }

  // Tool-result: show a small table or JSON of the result, and update metrics.
  if (event.event_type === "tool_result" && event.data) {
    if (event.data.metrics) updateMetrics(event.data.metrics);
    const result = event.data.result;
    const tableEl = resultToTable(result);
    if (tableEl) {
      item.appendChild(tableEl);
    } else {
      const pre = document.createElement("pre");
      pre.textContent = JSON.stringify(result, null, 2).slice(0, 1200);
      item.appendChild(pre);
    }
  }

  // Decision: drive the verdict box.
  if (event.event_type === "decision") {
    const outcome = (event.data && event.data.outcome) || "FLAGGED";
    showVerdict(outcome, event.message);
  }

  $("feed").appendChild(item);
  $("feed").scrollTop = $("feed").scrollHeight;
}

function titleFor(event) {
  switch (event.event_type) {
    case "thinking":
      return "Reasoning";
    case "tool_call":
      return (event.data && event.data.tool) || "Tool call";
    case "tool_result":
      return ((event.data && event.data.tool) || "Tool") + " result";
    case "decision":
      return "Decision";
    case "error":
      return "Error";
    default:
      return event.event_type;
  }
}

// Render a result dict whose values are flat (or a list of flat dicts) as a table.
function resultToTable(result) {
  if (!result || typeof result !== "object") return null;

  // List of records (e.g. sample_mismatches / null_distribution).
  const listKey = Object.keys(result).find(
    (k) => Array.isArray(result[k]) && result[k].length && typeof result[k][0] === "object"
  );
  let rows = null;
  if (Array.isArray(result)) rows = result;
  else if (listKey) rows = result[listKey];

  if (rows && rows.length) {
    const cols = Object.keys(rows[0]);
    const table = document.createElement("table");
    table.innerHTML =
      "<tr>" + cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("") + "</tr>";
    rows.slice(0, 8).forEach((r) => {
      const tr = document.createElement("tr");
      tr.innerHTML = cols
        .map((c) => `<td>${escapeHtml(String(r[c]))}</td>`)
        .join("");
      table.appendChild(tr);
    });
    return table;
  }

  // Flat scalar dict -> two-column key/value table.
  const flat = Object.entries(result).filter(
    ([, v]) => v === null || typeof v !== "object"
  );
  if (flat.length) {
    const table = document.createElement("table");
    flat.forEach(([k, v]) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<th>${escapeHtml(k)}</th><td>${escapeHtml(String(v))}</td>`;
      table.appendChild(tr);
    });
    return table;
  }
  return null;
}

// ----------------------------------------------------------------------- //
// Pipeline steps
// ----------------------------------------------------------------------- //
function updateSteps(agentName) {
  const step = AGENT_STEP[agentName];
  if (!step) return;
  document.querySelectorAll(".step").forEach((el) => {
    const n = parseInt(el.dataset.step, 10);
    if (n < step) {
      el.classList.remove("active");
      el.classList.add("done");
      el.querySelector(".step-icon").textContent = "✓";
    } else if (n === step) {
      el.classList.add("active");
      el.classList.remove("done");
    }
  });
}

function markRemainingStepsDone() {
  document.querySelectorAll(".step").forEach((el) => {
    if (el.classList.contains("active")) {
      el.classList.remove("active");
      el.classList.add("done");
      el.querySelector(".step-icon").textContent = "✓";
    }
  });
}

// ----------------------------------------------------------------------- //
// Metrics
// ----------------------------------------------------------------------- //
function updateMetrics(metrics) {
  if (metrics.row_match_pct !== undefined) {
    setMetric("row-match", metrics.row_match_pct.toFixed(1) + "%", metrics.row_match_pct, {
      good: 99,
      warn: 95,
      higherBetter: true,
    });
  }
  if (metrics.null_rate !== undefined) {
    setMetric("null-rate", metrics.null_rate.toFixed(2) + "%", Math.min(metrics.null_rate * 10, 100), {
      good: 10,
      warn: 30,
      higherBetter: false,
    });
  }
  if (metrics.mismatches !== undefined) {
    const v = metrics.mismatches;
    setMetric("mismatches", String(v), Math.min(v / 50, 100), {
      good: 5,
      warn: 30,
      higherBetter: false,
    });
  }
  if (metrics.source_only !== undefined) {
    const v = metrics.source_only;
    setMetric("source-only", String(v), Math.min(v / 50, 100), {
      good: 5,
      warn: 40,
      higherBetter: false,
    });
  }
}

function setMetric(key, text, fillPct, thresholds) {
  $("m-" + key).textContent = text;
  const bar = $("bar-" + key);
  bar.style.width = Math.max(0, Math.min(fillPct, 100)) + "%";
  bar.classList.remove("warn", "bad");
  // Color logic differs for higher-is-better vs lower-is-better metrics.
  if (thresholds.higherBetter) {
    if (fillPct >= thresholds.good) {
      /* green */
    } else if (fillPct >= thresholds.warn) {
      bar.classList.add("warn");
    } else {
      bar.classList.add("bad");
    }
  } else {
    if (fillPct <= thresholds.good) {
      /* green */
    } else if (fillPct <= thresholds.warn) {
      bar.classList.add("warn");
    } else {
      bar.classList.add("bad");
    }
  }
}

// ----------------------------------------------------------------------- //
// Verdict
// ----------------------------------------------------------------------- //
function showVerdict(outcome, message) {
  const box = $("verdict");
  box.className = "verdict " + outcome.toLowerCase();
  $("verdict-body").textContent = message;
  const actions = $("verdict-actions");
  if (outcome === "FLAGGED") {
    actions.classList.remove("hidden");
  } else {
    actions.classList.add("hidden");
  }
}

async function submitApproval(decision) {
  if (!currentRunId) return;
  try {
    const res = await fetch(`/api/run/${currentRunId}/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ decision }),
    });
    const data = await res.json();
    showVerdict(data.outcome, `Human ${decision}d. Final outcome: ${data.outcome}.`);
  } catch (e) {
    console.error(e);
  }
}

// ----------------------------------------------------------------------- //
// Recent runs
// ----------------------------------------------------------------------- //
async function refreshRecentRuns() {
  try {
    const res = await fetch("/api/runs");
    const data = await res.json();
    const list = $("recent-runs");
    list.innerHTML = "";
    (data.runs || []).forEach((r) => {
      const li = document.createElement("li");
      const shortId = (r.run_id || "").slice(0, 8);
      li.innerHTML = `<span>${escapeHtml(shortId)}</span>
        <span class="tag ${escapeHtml(r.outcome || "")}">${escapeHtml(r.outcome || "—")}</span>`;
      li.onclick = () => replayRun(r.run_id);
      list.appendChild(li);
    });
  } catch (_) {
    /* ignore */
  }
}

async function replayRun(runId) {
  try {
    const res = await fetch(`/api/run/${runId}`);
    const data = await res.json();
    resetUI();
    currentRunId = runId;
    if (data.events) {
      data.events.forEach((e) => renderEvent(e));
      markRemainingStepsDone();
    } else if (data.result) {
      showVerdict(data.result.outcome || "FLAGGED", JSON.stringify(data.result, null, 2));
    }
  } catch (e) {
    console.error(e);
  }
}

// ----------------------------------------------------------------------- //
// UI helpers
// ----------------------------------------------------------------------- //
function resetUI() {
  $("feed").innerHTML = "";
  $("verdict").className = "verdict";
  $("verdict-body").textContent = "Awaiting run…";
  $("verdict-actions").classList.add("hidden");
  ["row-match", "null-rate", "mismatches", "source-only"].forEach((k) => {
    $("m-" + k).textContent = "—";
    const bar = $("bar-" + k);
    bar.style.width = "0";
    bar.classList.remove("warn", "bad");
  });
  document.querySelectorAll(".step").forEach((el, i) => {
    el.classList.remove("active", "done");
    el.querySelector(".step-icon").textContent = String(i + 1);
  });
}

function setStatus(cls, text) {
  const pill = $("status-pill");
  pill.className = "status-pill " + (cls === "idle" ? "" : cls);
  $("status-text").textContent = text;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// ----------------------------------------------------------------------- //
// Init
// ----------------------------------------------------------------------- //
window.addEventListener("DOMContentLoaded", () => {
  loadTables();
  loadWorkspace();
  refreshRecentRuns();
  $("run-btn").addEventListener("click", runAgent);
  $("approve-btn").addEventListener("click", () => submitApproval("approve"));
  $("reject-btn").addEventListener("click", () => submitApproval("reject"));
});
