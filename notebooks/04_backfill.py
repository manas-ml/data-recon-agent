# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Partition Backfill
# MAGIC
# MAGIC Remediates a failed partition by MERGE-ing the missing source rows for a
# MAGIC given `created_at` date into the target table. Idempotent: rows that already
# MAGIC exist on the key are left untouched.
# MAGIC
# MAGIC Widget params:
# MAGIC * `source_table`, `target_table`
# MAGIC * `partition_date` — the created_at date to backfill (YYYY-MM-DD)
# MAGIC * `key_columns`    — comma-separated

# COMMAND ----------

dbutils.widgets.text("source_table", "main.recon_demo.orders_source", "Source")
dbutils.widgets.text("target_table", "main.recon_demo.orders_warehouse", "Target")
dbutils.widgets.text("partition_date", "2024-03-15", "Partition date")
dbutils.widgets.text("key_columns", "order_id", "Key columns")

source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
partition_date = dbutils.widgets.get("partition_date")
keys = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]

# COMMAND ----------

import json
from datetime import datetime, timezone

before = spark.table(target_table).count()

on_clause = " AND ".join([f"tgt.`{k}` = src.`{k}`" for k in keys])

merge_sql = f"""
MERGE INTO {target_table} AS tgt
USING (
  SELECT * FROM {source_table}
  WHERE CAST(created_at AS DATE) = DATE('{partition_date}')
) AS src
ON {on_clause}
WHEN NOT MATCHED THEN INSERT *
"""
spark.sql(merge_sql)

after = spark.table(target_table).count()
rows_inserted = after - before

# COMMAND ----------

result = {
    "target_table": target_table,
    "partition_date": partition_date,
    "rows_before": before,
    "rows_after": after,
    "rows_inserted": rows_inserted,
    "backfilled_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
