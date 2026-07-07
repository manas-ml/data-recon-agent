"""Databricks tool implementations callable by the Claude agent.

Every tool authenticates through the Databricks SDK / SQL connector using the
workspace's native credentials (no hardcoded tokens). All SQL access uses the
databricks-sql-connector against the configured SQL Warehouse, wrapped in context
managers so connections and cursors are always closed.

These functions are the *actual* implementations; agent.py exposes their JSON
schemas to Claude and dispatches tool_use blocks here.
"""

from __future__ import annotations

import json
import re
import time
from typing import Any, Optional

from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

from .config import Settings


class ReconTools:
    """Bundle of reconciliation tools bound to a Settings instance.

    A single instance is created per run so the warehouse connection details and
    catalog/schema target are resolved once.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        # WorkspaceClient auto-detects credentials inside Databricks Apps. When
        # running locally it falls back to DATABRICKS_HOST/DATABRICKS_TOKEN env
        # vars resolved by the SDK's default credential chain.
        self._wc: Optional[WorkspaceClient] = None

    # ------------------------------------------------------------------ #
    # Connection helpers
    # ------------------------------------------------------------------ #
    @property
    def workspace(self) -> WorkspaceClient:
        """Lazily construct (and cache) the WorkspaceClient."""
        if self._wc is None:
            self._wc = WorkspaceClient()
        return self._wc

    def _connect(self):
        """Open a databricks-sql-connector connection to the SQL Warehouse.

        Uses the SDK-resolved host and a workspace OAuth token so no static
        credentials are required in Databricks Apps.
        """
        wc = self.workspace
        host = self.settings.databricks_host or wc.config.host
        # Strip protocol for the connector's server_hostname.
        server_hostname = host.replace("https://", "").replace("http://", "").rstrip("/")
        http_path = f"/sql/1.0/warehouses/{self.settings.databricks_warehouse_id}"

        # Resolve a Bearer token from the SDK credential chain.
        # wc.config.authenticate() uses requests.PreparedRequest as its visitor
        # interface, which is the standard way to extract a token from the SDK
        # regardless of auth type (PAT, M2M OAuth, Databricks Apps OAuth).
        if self.settings.databricks_token:
            token = self.settings.databricks_token
        else:
            headers = wc.config.authenticate()
            auth_header = headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                raise RuntimeError("SDK credential chain did not return a Bearer token")
            token = auth_header[len("Bearer "):]

        return dbsql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=token,
        )

    def _query(self, statement: str) -> list[dict[str, Any]]:
        """Run a SQL statement and return rows as a list of dicts."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(statement)
                if cur.description is None:
                    return []
                columns = [d[0] for d in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def _execute(self, statement: str) -> None:
        """Run a SQL statement that returns no rows (DDL/DML)."""
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(statement)

    # ------------------------------------------------------------------ #
    # 1. compare_schemas
    # ------------------------------------------------------------------ #
    def compare_schemas(self, source_table: str, target_table: str) -> dict:
        """Compare the schemas of two tables via DESCRIBE TABLE.

        Returns columns only in source, only in target, type mismatches, and the
        set of common columns.
        """

        def describe(table: str) -> dict[str, str]:
            rows = self._query(f"DESCRIBE TABLE {table}")
            cols: dict[str, str] = {}
            for r in rows:
                name = r.get("col_name") or ""
                # DESCRIBE emits blank rows / partition headers; skip them.
                if not name or name.startswith("#"):
                    continue
                cols[name] = (r.get("data_type") or "").strip()
            return cols

        src = describe(source_table)
        tgt = describe(target_table)

        columns_only_in_source = sorted(set(src) - set(tgt))
        columns_only_in_target = sorted(set(tgt) - set(src))
        common = sorted(set(src) & set(tgt))
        type_mismatches = [
            {"column": c, "source_type": src[c], "target_type": tgt[c]}
            for c in common
            if src[c] != tgt[c]
        ]

        return {
            "columns_only_in_source": columns_only_in_source,
            "columns_only_in_target": columns_only_in_target,
            "type_mismatches": type_mismatches,
            "common_columns": common,
        }

    # ------------------------------------------------------------------ #
    # 2. get_row_counts
    # ------------------------------------------------------------------ #
    def get_row_counts(self, source_table: str, target_table: str) -> dict:
        """Return row counts for both tables and the delta between them."""
        src = self._query(f"SELECT COUNT(*) AS c FROM {source_table}")[0]["c"]
        tgt = self._query(f"SELECT COUNT(*) AS c FROM {target_table}")[0]["c"]
        src = int(src)
        tgt = int(tgt)
        delta = src - tgt
        delta_pct = (abs(delta) / src * 100.0) if src else 0.0
        return {
            "source_count": src,
            "target_count": tgt,
            "delta": delta,
            "delta_pct": round(delta_pct, 4),
        }

    # ------------------------------------------------------------------ #
    # 3. run_column_profiler
    # ------------------------------------------------------------------ #
    def run_column_profiler(self, table: str, columns: list[str]) -> dict:
        """Compute null_rate, distinct_count, min, max, avg for each column."""
        total = int(self._query(f"SELECT COUNT(*) AS c FROM {table}")[0]["c"])
        profile: dict[str, dict] = {}
        for col in columns:
            stats = self._query(
                f"""
                SELECT
                  COUNT(*) - COUNT(`{col}`)            AS null_count,
                  COUNT(DISTINCT `{col}`)              AS distinct_count,
                  CAST(MIN(`{col}`) AS STRING)         AS min_val,
                  CAST(MAX(`{col}`) AS STRING)         AS max_val,
                  CAST(AVG(TRY_CAST(`{col}` AS DOUBLE)) AS STRING) AS avg_val
                FROM {table}
                """
            )[0]
            null_count = int(stats["null_count"] or 0)
            profile[col] = {
                "null_count": null_count,
                "null_rate": round((null_count / total) if total else 0.0, 6),
                "distinct_count": int(stats["distinct_count"] or 0),
                "min": stats["min_val"],
                "max": stats["max_val"],
                "avg": stats["avg_val"],
            }
        return {"table": table, "row_count": total, "profile": profile}

    # ------------------------------------------------------------------ #
    # 4. run_full_diff
    # ------------------------------------------------------------------ #
    def run_full_diff(
        self, source_table: str, target_table: str, key_columns: list[str]
    ) -> dict:
        """Full outer join the two tables on the keys and classify each row.

        Classification buckets: MATCHED, SOURCE_ONLY, TARGET_ONLY, VALUE_MISMATCH.
        For VALUE_MISMATCH we compare non-key common columns. A small sample of
        mismatching keys is returned for display.
        """
        schema = self.compare_schemas(source_table, target_table)
        common = [c for c in schema["common_columns"] if c not in key_columns]
        keys = key_columns

        on_clause = " AND ".join([f"s.`{k}` = t.`{k}`" for k in keys])
        key_null_src = " AND ".join([f"s.`{k}` IS NULL" for k in keys])
        key_null_tgt = " AND ".join([f"t.`{k}` IS NULL" for k in keys])

        # A row is a value mismatch when keys match on both sides but at least
        # one non-key common column differs (null-safe comparison).
        if common:
            mismatch_predicate = " OR ".join(
                [f"NOT (s.`{c}` <=> t.`{c}`)" for c in common]
            )
        else:
            mismatch_predicate = "FALSE"

        classify_sql = f"""
        WITH joined AS (
          SELECT s.*, t.*,
                 CASE
                   WHEN {key_null_src} THEN 'TARGET_ONLY'
                   WHEN {key_null_tgt} THEN 'SOURCE_ONLY'
                   WHEN {mismatch_predicate} THEN 'VALUE_MISMATCH'
                   ELSE 'MATCHED'
                 END AS recon_class
          FROM {source_table} s
          FULL OUTER JOIN {target_table} t ON {on_clause}
        )
        SELECT recon_class, COUNT(*) AS c FROM joined GROUP BY recon_class
        """
        counts = {r["recon_class"]: int(r["c"]) for r in self._query(classify_sql)}

        # Pull a few sample mismatching keys for the UI.
        sample_select_keys = ", ".join([f"s.`{k}` AS `{k}`" for k in keys])
        sample_sql = f"""
        SELECT {sample_select_keys}
        FROM {source_table} s
        JOIN {target_table} t ON {on_clause}
        WHERE {mismatch_predicate}
        LIMIT 10
        """
        try:
            sample = self._query(sample_sql) if common else []
        except Exception:
            sample = []

        return {
            "matched": counts.get("MATCHED", 0),
            "source_only": counts.get("SOURCE_ONLY", 0),
            "target_only": counts.get("TARGET_ONLY", 0),
            "value_mismatch": counts.get("VALUE_MISMATCH", 0),
            "sample_mismatches": sample,
        }

    # ------------------------------------------------------------------ #
    # 5. investigate_nulls
    # ------------------------------------------------------------------ #
    def investigate_nulls(self, table: str, column: str) -> dict:
        """Find temporal clustering of nulls in a column by load_date.

        Returns a per-day distribution of null counts and a heuristic likely
        cause (a single dominant day suggests a failed partition load).
        """
        rows = self._query(
            f"""
            SELECT CAST(DATE_TRUNC('day', load_date) AS STRING) AS day,
                   COUNT(*) AS null_count
            FROM {table}
            WHERE `{column}` IS NULL
            GROUP BY DATE_TRUNC('day', load_date)
            ORDER BY null_count DESC
            """
        )
        distribution = [
            {"date": r["day"], "count": int(r["null_count"])} for r in rows
        ]
        total_nulls = sum(d["count"] for d in distribution)
        likely_cause = "no_nulls_detected"
        if distribution:
            top = distribution[0]
            share = (top["count"] / total_nulls) if total_nulls else 0
            if share >= 0.6:
                likely_cause = (
                    f"failed_partition_load on {top['date']} "
                    f"({top['count']} of {total_nulls} nulls)"
                )
            else:
                likely_cause = "nulls_distributed_across_partitions"
        return {"null_distribution": distribution, "likely_cause": likely_cause}

    # ------------------------------------------------------------------ #
    # 6. trigger_databricks_job
    # ------------------------------------------------------------------ #
    def trigger_databricks_job(self, job_name: str, parameters: dict) -> dict:
        """Trigger a named Databricks job and poll until it terminates.

        Looks up the job by name via the SDK, runs it with the given notebook
        parameters, and polls run status every 5 seconds.
        """
        wc = self.workspace
        # Fuzzy match: score each job by how many significant query words (>3
        # chars) appear in its name, return the highest-scoring job.
        # Handles DAB dev-mode prefixes like "[dev user] [recon-agent] Verifier"
        # and tolerates extra words Claude adds (e.g. "orders_warehouse_backfill"
        # still resolves to the Partition Backfill job via "backfill").
        def _words(s: str) -> set[str]:
            return set(re.sub(r"[^a-z0-9]+", " ", s.lower()).split())

        sig_words = {w for w in _words(job_name) if len(w) > 3}
        best_id, best_score = None, 0
        for job in wc.jobs.list():
            if job.settings and job.settings.name:
                score = len(sig_words & _words(job.settings.name))
                if score > best_score:
                    best_score, best_id = score, job.job_id
        job_id = best_id if best_score > 0 else None
        if job_id is None:
            return {
                "run_id": None,
                "status": "ERROR",
                "duration_seconds": 0.0,
                "output": f"Job named '{job_name}' not found.",
            }

        started = time.time()
        run = wc.jobs.run_now(job_id=job_id, notebook_params=parameters)
        run_id = run.run_id

        # Poll until the run reaches a terminal life-cycle state.
        terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        state = None
        result_state = None
        output_text = ""
        while True:
            info = wc.jobs.get_run(run_id=run_id)
            life = info.state.life_cycle_state.value if info.state else "PENDING"
            state = life
            if life in terminal:
                if info.state and info.state.result_state:
                    result_state = info.state.result_state.value
                break
            time.sleep(5)

        duration = round(time.time() - started, 2)
        # Attempt to read the notebook exit output of the first task.
        try:
            tasks = info.tasks or []
            if tasks:
                task_out = wc.jobs.get_run_output(run_id=tasks[0].run_id)
                output_text = task_out.notebook_output.result if task_out.notebook_output else ""
        except Exception as exc:  # noqa: BLE001
            output_text = f"(could not fetch output: {exc})"

        return {
            "run_id": run_id,
            "status": result_state or state,
            "duration_seconds": duration,
            "output": output_text,
        }

    # ------------------------------------------------------------------ #
    # 7. log_to_mlflow
    # ------------------------------------------------------------------ #
    def log_to_mlflow(self, run_id: str, metrics: dict, params: dict) -> bool:
        """Log reconciliation metrics and params to the MLflow experiment."""
        import mlflow

        mlflow.set_experiment(self.settings.mlflow_experiment)
        with mlflow.start_run(run_name=f"recon-{run_id}"):
            for k, v in (params or {}).items():
                mlflow.log_param(k, v)
            for k, v in (metrics or {}).items():
                # Only numeric values are valid MLflow metrics.
                try:
                    mlflow.log_metric(k, float(v))
                except (TypeError, ValueError):
                    mlflow.log_param(k, str(v))
            mlflow.set_tag("recon_run_id", run_id)
        return True

    # ------------------------------------------------------------------ #
    # 8. write_recon_log
    # ------------------------------------------------------------------ #
    def write_recon_log(self, result: dict) -> bool:
        """Append a structured JSON result to the audit-log Delta table.

        Creates the table on first write. Stores the full result document as a
        JSON string alongside a few promoted columns for easy querying.
        """
        table = self.settings.fq_audit_table
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
              run_id STRING,
              outcome STRING,
              issues_found INT,
              issues_resolved INT,
              duration_seconds DOUBLE,
              source_table STRING,
              target_table STRING,
              result_json STRING,
              logged_at TIMESTAMP
            ) USING DELTA
            """
        )
        payload = json.dumps(result).replace("'", "\\'")
        self._execute(
            f"""
            INSERT INTO {table}
            SELECT
              '{result.get("run_id", "")}',
              '{result.get("outcome", "")}',
              {int(result.get("issues_found", 0))},
              {int(result.get("issues_resolved", 0))},
              {float(result.get("duration_seconds", 0.0))},
              '{result.get("source_table", "")}',
              '{result.get("target_table", "")}',
              '{payload}',
              current_timestamp()
            """
        )
        return True

    # ------------------------------------------------------------------ #
    # 9. get_baseline_metrics
    # ------------------------------------------------------------------ #
    def get_baseline_metrics(self, table: str, days: int = 30) -> dict:
        """Read recent MLflow runs for a table to establish baseline metrics.

        Returns averaged null-rate and mismatch-rate plus a p95 null-rate. If no
        history exists, returns conservative defaults so the agent can still
        reason about relative change.
        """
        import mlflow

        try:
            mlflow.set_experiment(self.settings.mlflow_experiment)
            exp = mlflow.get_experiment_by_name(self.settings.mlflow_experiment)
            if exp is None:
                raise RuntimeError("experiment missing")
            cutoff_ms = int((time.time() - days * 86400) * 1000)
            runs = mlflow.search_runs(
                experiment_ids=[exp.experiment_id],
                filter_string=f"attributes.start_time > {cutoff_ms}",
                max_results=500,
            )
            if runs is None or len(runs) == 0:
                raise RuntimeError("no runs")

            def col_mean(name: str) -> float:
                c = f"metrics.{name}"
                if c in runs.columns:
                    vals = runs[c].dropna()
                    return float(vals.mean()) if len(vals) else 0.0
                return 0.0

            def col_p95(name: str) -> float:
                c = f"metrics.{name}"
                if c in runs.columns:
                    vals = runs[c].dropna()
                    return float(vals.quantile(0.95)) if len(vals) else 0.0
                return 0.0

            return {
                "avg_null_rate": round(col_mean("null_rate"), 6),
                "p95_null_rate": round(col_p95("null_rate"), 6),
                "avg_mismatch_rate": round(col_mean("mismatch_rate"), 6),
                "runs_considered": int(len(runs)),
            }
        except Exception:  # noqa: BLE001
            # No baseline available yet — return safe defaults.
            return {
                "avg_null_rate": 0.001,
                "p95_null_rate": 0.005,
                "avg_mismatch_rate": 0.001,
                "runs_considered": 0,
            }

    # ------------------------------------------------------------------ #
    # Dispatch
    # ------------------------------------------------------------------ #
    def dispatch(self, name: str, arguments: dict) -> dict:
        """Route a tool_use name + arguments to the matching method."""
        handler = {
            "compare_schemas": self.compare_schemas,
            "get_row_counts": self.get_row_counts,
            "run_column_profiler": self.run_column_profiler,
            "run_full_diff": self.run_full_diff,
            "investigate_nulls": self.investigate_nulls,
            "trigger_databricks_job": self.trigger_databricks_job,
            "log_to_mlflow": self.log_to_mlflow,
            "get_baseline_metrics": self.get_baseline_metrics,
        }.get(name)
        if handler is None:
            return {"error": f"Unknown tool: {name}"}
        result = handler(**arguments)
        # write_recon_log is not dispatched here — it is called by the app
        # server-side after each run. log_to_mlflow returns bool; normalise it.
        if isinstance(result, bool):
            return {"success": result}
        return result
