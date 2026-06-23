# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Column Profiler
# MAGIC
# MAGIC Computes per-column statistics for a table and writes them to
# MAGIC `{catalog}.{schema}.column_profiles`. Called by the agent (directly or via
# MAGIC a job) to gather profiling evidence.
# MAGIC
# MAGIC Widget params:
# MAGIC * `table`   — fully-qualified table name
# MAGIC * `columns` — comma-separated list of columns to profile

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "recon_demo", "Schema")
dbutils.widgets.text("table", "main.recon_demo.orders_warehouse", "Table")
dbutils.widgets.text("columns", "revenue,load_date,order_id", "Columns")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
columns = [c.strip() for c in dbutils.widgets.get("columns").split(",") if c.strip()]

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

df = spark.table(table)
total = df.count()

profiles = []
for col in columns:
    stats = df.select(
        F.count(F.lit(1)).alias("rows"),
        (F.count(F.lit(1)) - F.count(F.col(col))).alias("null_count"),
        F.countDistinct(F.col(col)).alias("distinct_count"),
        F.min(F.col(col)).cast("string").alias("min_val"),
        F.max(F.col(col)).cast("string").alias("max_val"),
        F.avg(F.col(col).cast("double")).cast("string").alias("avg_val"),
    ).collect()[0]

    null_count = int(stats["null_count"] or 0)
    profiles.append(
        {
            "table": table,
            "column": col,
            "row_count": total,
            "null_count": null_count,
            "null_rate": float(null_count / total) if total else 0.0,
            "distinct_count": int(stats["distinct_count"] or 0),
            "min_val": stats["min_val"],
            "max_val": stats["max_val"],
            "avg_val": stats["avg_val"],
            "profiled_at": datetime.now(timezone.utc).isoformat(),
        }
    )

# COMMAND ----------

profile_df = spark.createDataFrame(profiles)
(
    profile_df.write.mode("append")
    .format("delta")
    .saveAsTable(f"{catalog}.{schema}.column_profiles")
)

display(profile_df)

# COMMAND ----------

# Return a JSON summary so the agent's trigger_databricks_job tool can read it.
result = {
    "table": table,
    "row_count": total,
    "profiles": {p["column"]: {k: p[k] for k in ("null_rate", "distinct_count", "min_val", "max_val", "avg_val")} for p in profiles},
}
dbutils.notebook.exit(json.dumps(result))
