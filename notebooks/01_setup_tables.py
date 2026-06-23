# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup Demo Tables
# MAGIC
# MAGIC Creates two Delta tables with intentional drift so the reconciliation
# MAGIC agent has something realistic to investigate:
# MAGIC
# MAGIC * `orders_source`    — 500,000 clean rows
# MAGIC * `orders_warehouse` — a copy of source with seeded defects:
# MAGIC     * 2,287 rows missing (simulated failed partition load on 2024-03-15)
# MAGIC     * 412 rows with revenue drifted by ±$0.01 (floating point drift)
# MAGIC     * `load_date` NULL for rows that loaded only partially
# MAGIC
# MAGIC Data is generated deterministically (seed = 42) for reproducibility.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "recon_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

print(f"Target: {catalog}.{schema}")

# COMMAND ----------

import random
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

random.seed(42)

N = 500_000
REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
STATUSES = ["completed", "pending", "shipped", "cancelled"]
SKUS = [f"SKU-{i:04d}" for i in range(200)]

# Span created_at across ~60 days so one bad partition (2024-03-15) is isolatable.
START = datetime(2024, 2, 15)


def gen_rows(n):
    rows = []
    for i in range(n):
        created = START + timedelta(
            days=random.randint(0, 59),
            seconds=random.randint(0, 86399),
        )
        rows.append(
            (
                i + 1,                                   # order_id
                random.randint(1, 50_000),               # customer_id
                round(random.uniform(5.0, 2000.0), 2),   # revenue
                random.choice(STATUSES),                 # status
                created,                                 # created_at
                random.choice(REGIONS),                  # region
                random.choice(SKUS),                     # product_sku
                random.randint(1, 10),                   # quantity
                round(random.uniform(0.0, 0.4), 2),      # discount_pct
                created,                                 # load_date
            )
        )
    return rows


schema_struct = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("region", StringType(), True),
        StructField("product_sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("load_date", TimestampType(), True),
    ]
)

source_df = spark.createDataFrame(gen_rows(N), schema_struct)
source_df.write.mode("overwrite").format("delta").saveAsTable("orders_source")
print(f"orders_source written: {spark.table('orders_source').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the warehouse copy with seeded drift

# COMMAND ----------

src = spark.table("orders_source")

# 1. Drop the simulated failed partition (created_at on 2024-03-15) -> missing rows.
bad_day = "2024-03-15"
warehouse = src.filter(~(F.to_date("created_at") == F.lit(bad_day)))

missing_rows = src.filter(F.to_date("created_at") == F.lit(bad_day)).count()
print(f"Rows dropped for failed partition {bad_day}: {missing_rows:,}")

# 2. Floating-point revenue drift on 412 rows.
drift_ids = [r.order_id for r in src.select("order_id").orderBy("order_id").limit(412).collect()]
warehouse = warehouse.withColumn(
    "revenue",
    F.when(F.col("order_id").isin(drift_ids), F.col("revenue") + F.lit(0.01)).otherwise(
        F.col("revenue")
    ),
)

# 3. Null out load_date for a slice of rows that "loaded partially" (next 1,000 ids).
partial_ids = [
    r.order_id
    for r in src.select("order_id").orderBy("order_id").limit(1412).collect()[412:]
]
warehouse = warehouse.withColumn(
    "load_date",
    F.when(F.col("order_id").isin(partial_ids), F.lit(None).cast("timestamp")).otherwise(
        F.col("load_date")
    ),
)

warehouse.write.mode("overwrite").format("delta").saveAsTable("orders_warehouse")
print(f"orders_warehouse written: {spark.table('orders_warehouse').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of seeded drift

# COMMAND ----------

src_count = spark.table("orders_source").count()
wh_count = spark.table("orders_warehouse").count()
null_loaddate = spark.table("orders_warehouse").filter(F.col("load_date").isNull()).count()

print(f"source rows        : {src_count:,}")
print(f"warehouse rows     : {wh_count:,}")
print(f"row delta          : {src_count - wh_count:,}")
print(f"revenue drift rows : 412")
print(f"null load_date rows: {null_loaddate:,}")
