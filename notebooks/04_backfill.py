# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Reconciling Upsert (Backfill + Value Repair)
# MAGIC
# MAGIC Reconciles the target table to the source on the key columns with a single
# MAGIC MERGE that fixes both classes of drift the differ surfaces:
# MAGIC
# MAGIC * **SOURCE_ONLY** (missing rows, e.g. a failed partition load) -> inserted.
# MAGIC * **VALUE_MISMATCH** (drifted non-key values, e.g. revenue rounding or a
# MAGIC   NULL `load_date` from a partial load) -> updated from source.
# MAGIC
# MAGIC Rows already in agreement are left untouched, and TARGET_ONLY rows are never
# MAGIC deleted. The MERGE is idempotent: a second run is a no-op once source and
# MAGIC target agree.
# MAGIC
# MAGIC The reconcile always operates **table-wide** so value drift spread across
# MAGIC many partitions is fully repaired (the earlier INSERT-only, single-partition
# MAGIC backfill silently wrote 0 rows against value drift). `partition_date` is kept
# MAGIC for backward compatibility but is informational only.
# MAGIC
# MAGIC Widget params:
# MAGIC * `source_table`, `target_table`
# MAGIC * `key_columns`    — comma-separated
# MAGIC * `partition_date` — optional; recorded in the result, does not scope the merge

# COMMAND ----------

dbutils.widgets.text("source_table", "main.recon_demo.orders_source", "Source")
dbutils.widgets.text("target_table", "main.recon_demo.orders_warehouse", "Target")
dbutils.widgets.text("partition_date", "", "Partition date (optional)")
dbutils.widgets.text("key_columns", "order_id", "Key columns")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
partition_date = dbutils.widgets.get("partition_date").strip()
keys = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]

# COMMAND ----------

import json
from datetime import datetime, timezone

before = spark.table(target_table).count()

# Compare + repair every non-key column the two tables share.
src_cols = spark.table(source_table).columns
tgt_cols = spark.table(target_table).columns
non_key_common = [c for c in src_cols if c in tgt_cols and c not in keys]

on_clause = " AND ".join([f"tgt.`{k}` = src.`{k}`" for k in keys])

# Only UPDATE matched rows that actually differ (null-safe) on some shared non-key
# column, so untouched rows aren't needlessly rewritten. Without shared non-key
# columns there is nothing to repair, so the UPDATE clause is omitted.
if non_key_common:
    differs = " OR ".join([f"NOT (tgt.`{c}` <=> src.`{c}`)" for c in non_key_common])
    matched_clause = f"WHEN MATCHED AND ({differs}) THEN UPDATE SET *"
else:
    matched_clause = ""

merge_sql = f"""
MERGE INTO {target_table} AS tgt
USING {source_table} AS src
ON {on_clause}
{matched_clause}
WHEN NOT MATCHED THEN INSERT *
"""

# Databricks returns MERGE row metrics as a single-row DataFrame on modern DBR;
# fall back to a count delta for inserts on runtimes that don't.
merge_metrics = {}
try:
    merge_rows = spark.sql(merge_sql).collect()
    if merge_rows:
        merge_metrics = merge_rows[0].asDict()
except Exception:
    merge_metrics = {}

after = spark.table(target_table).count()
rows_inserted = int(merge_metrics.get("num_inserted_rows", after - before))
rows_updated = int(merge_metrics.get("num_updated_rows", 0))

# COMMAND ----------

result = {
    "target_table": target_table,
    "requested_partition_date": partition_date or "ALL",
    "rows_before": before,
    "rows_after": after,
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "reconciled_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
