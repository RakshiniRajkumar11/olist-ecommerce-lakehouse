# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from pyspark.sql.functions import count, when, col
warnings.filterwarnings("ignore")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

print("BRONZE INGESTION STARTED")

# COMMAND ----------

for table_name, url in DATASET.items():
    print(f"Loading {table_name}...")
    try:
        pandas_df = pd.read_csv(url)
        spark_df  = spark.createDataFrame(pandas_df)

        (
            spark_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
        )
        print(f" {table_name}: {pandas_df.shape[0]:,} rows loaded")

    except Exception as e:
        print(f" Failed to load {table_name}: {e}")

print("\n Bronze ingestion complete")

# COMMAND ----------

def generate_data_quality_report(table_name, df):
    total_rows = df.count()
    total_cols = len(df.columns)

    print(f"\n{'='*60}")
    print(f" DQ REPORT: {table_name}")
    print(f"{'='*60}")
    print(f"Rows: {total_rows:,}  |  Columns: {total_cols}")

    # Null analysis
    print("\n NULL ANALYSIS:")
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    has_nulls = False
    for col_name, null_cnt in sorted(null_counts.items(), key=lambda x: x[1], reverse=True):
        if null_cnt > 0:
            pct = (null_cnt / total_rows) * 100
            print(f"  {col_name}: {null_cnt:,} ({pct:.2f}%)")
            has_nulls = True
    if not has_nulls:
        print("  No nulls detected")

    # Duplicate detection
    duplicate_count = total_rows - df.dropDuplicates().count()
    print(f"\n DUPLICATES: {duplicate_count:,} rows")

    return {
        "table":      table_name,
        "rows":       total_rows,
        "columns":    total_cols,
        "nulls":      sum(null_counts.values()),
        "duplicates": duplicate_count
    }

# COMMAND ----------

# Run DQ on all bronze tables
tables = ["orders", "order_items", "products", "customers",
          "sellers", "payment", "reviews", "category"]

dq_reports = []
for table in tables:
    try:
        df     = spark.table(f"{BRONZE_SCHEMA}.{table}")
        report = generate_data_quality_report(table, df)
        dq_reports.append(report)
    except Exception as e:
        print(f"\n Could not analyse {table}: {e}")

# Summary
print("\n" + "=" * 60)
print("DATA QUALITY SUMMARY")
print("=" * 60)
dq_summary = pd.DataFrame(dq_reports)
display(dq_summary)