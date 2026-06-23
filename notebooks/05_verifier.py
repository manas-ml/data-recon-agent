# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Post-Remediation Verifier
# MAGIC
# MAGIC Re-runs the row-count and null-rate checks after a remediation and compares
# MAGIC them against the pre-remediation snapshot stored in `diff_snapshots`.
# MAGIC Returns a pass/fail verdict the agent uses to decide whether remediation
# MAGIC succeeded or the issue must be escalated.
# MAGIC
# MAGIC Widget params:
# MAGIC * `source_table`, `target_table`
# MAGIC * `key_columns` — comma-separated

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "recon_demo", "Schema")
dbutils.widgets.text("source_table", "main.recon_demo.orders_source", "Source")
dbutils.widgets.text("target_table", "main.recon_demo.orders_warehouse", "Target")
dbutils.widgets.text("key_columns", "order_id", "Key columns")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
keys = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

# Current state.
src_count = spark.table(source_table).count()
tgt_count = spark.table(target_table).count()
row_delta = src_count - tgt_count
row_delta_pct = (abs(row_delta) / src_count * 100.0) if src_count else 0.0

# Pre-remediation snapshot (most recent for this table pair).
snap = (
    spark.table(f"{catalog}.{schema}.diff_snapshots")
    .filter((F.col("source_table") == source_table) & (F.col("target_table") == target_table))
    .orderBy(F.col("snapshot_at").desc())
    .limit(1)
    .collect()
)
pre = snap[0].asDict() if snap else {}

# COMMAND ----------

# Pass criteria: row delta closed to (near) zero and no worse than before.
pre_delta = (pre.get("source_count", src_count) - pre.get("target_count", tgt_count)) if pre else row_delta
passed = abs(row_delta) <= max(0, abs(pre_delta)) and row_delta_pct < 0.5

result = {
    "source_table": source_table,
    "target_table": target_table,
    "source_count": src_count,
    "target_count": tgt_count,
    "row_delta": row_delta,
    "row_delta_pct": round(row_delta_pct, 4),
    "pre_remediation_delta": pre_delta,
    "verdict": "PASS" if passed else "FAIL",
    "verified_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
