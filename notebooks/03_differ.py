# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Full Outer Join Differ
# MAGIC
# MAGIC Performs a full outer join of source and target on the key columns,
# MAGIC classifies every row, and writes the classified diff to
# MAGIC `{catalog}.{schema}.diff_results`. Also persists a pre-remediation snapshot
# MAGIC used later by the verifier.
# MAGIC
# MAGIC Widget params:
# MAGIC * `source_table`, `target_table`
# MAGIC * `key_columns`  — comma-separated
# MAGIC * `tolerance_pct`

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "recon_demo", "Schema")
dbutils.widgets.text("source_table", "main.recon_demo.orders_source", "Source")
dbutils.widgets.text("target_table", "main.recon_demo.orders_warehouse", "Target")
dbutils.widgets.text("key_columns", "order_id", "Key columns")
dbutils.widgets.text("tolerance_pct", "2.0", "Tolerance %")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
keys = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]
tolerance_pct = float(dbutils.widgets.get("tolerance_pct"))

# COMMAND ----------

import json
from datetime import datetime, timezone

from functools import reduce

from pyspark.sql import functions as F

src = spark.table(source_table).alias("s")
tgt = spark.table(target_table).alias("t")

common = [c for c in src.columns if c in tgt.columns]
non_key_common = [c for c in common if c not in keys]

join_cond = reduce(lambda a, b: a & b, [F.col(f"s.{k}") == F.col(f"t.{k}") for k in keys])
joined = src.join(tgt, join_cond, "full_outer")

src_key_null = reduce(lambda a, b: a & b, [F.col(f"s.{k}").isNull() for k in keys])
tgt_key_null = reduce(lambda a, b: a & b, [F.col(f"t.{k}").isNull() for k in keys])

if non_key_common:
    mismatch = reduce(
        lambda a, b: a | b,
        [~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}")) for c in non_key_common],
    )
else:
    mismatch = F.lit(False)

classified = joined.withColumn(
    "recon_class",
    F.when(src_key_null, F.lit("TARGET_ONLY"))
    .when(tgt_key_null, F.lit("SOURCE_ONLY"))
    .when(mismatch, F.lit("VALUE_MISMATCH"))
    .otherwise(F.lit("MATCHED")),
)

# COMMAND ----------

counts = {r["recon_class"]: r["c"] for r in classified.groupBy("recon_class").agg(F.count("*").alias("c")).collect()}

key_cols_select = [F.coalesce(F.col(f"s.{k}"), F.col(f"t.{k}")).alias(k) for k in keys]
diff_out = classified.select(*key_cols_select, "recon_class").withColumn(
    "diffed_at", F.lit(datetime.now(timezone.utc).isoformat())
)
(
    diff_out.write.mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.diff_results")
)

# COMMAND ----------

# Persist a pre-remediation snapshot for the verifier to compare against.
src_count = spark.table(source_table).count()
tgt_count = spark.table(target_table).count()
snapshot = [
    {
        "source_table": source_table,
        "target_table": target_table,
        "source_count": src_count,
        "target_count": tgt_count,
        "matched": int(counts.get("MATCHED", 0)),
        "source_only": int(counts.get("SOURCE_ONLY", 0)),
        "target_only": int(counts.get("TARGET_ONLY", 0)),
        "value_mismatch": int(counts.get("VALUE_MISMATCH", 0)),
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
    }
]
(
    spark.createDataFrame(snapshot)
    .write.mode("append")
    .format("delta")
    .saveAsTable(f"{catalog}.{schema}.diff_snapshots")
)

# COMMAND ----------

result = {
    "matched": int(counts.get("MATCHED", 0)),
    "source_only": int(counts.get("SOURCE_ONLY", 0)),
    "target_only": int(counts.get("TARGET_ONLY", 0)),
    "value_mismatch": int(counts.get("VALUE_MISMATCH", 0)),
    "source_count": src_count,
    "target_count": tgt_count,
    "tolerance_pct": tolerance_pct,
}
display(classified.groupBy("recon_class").agg(F.count("*").alias("count")))
dbutils.notebook.exit(json.dumps(result))
