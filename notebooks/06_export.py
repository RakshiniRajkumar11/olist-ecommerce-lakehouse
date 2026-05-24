# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import os
import os
import shutil
os.makedirs(EXPORT_PATH, exist_ok=True)

workspace_export = "/Workspace/Users/rakshinirajkumar11022004@gmail.com/olist_downloads"
os.makedirs(workspace_export, exist_ok=True)

# COMMAND ----------

gold_tables = spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").collect()

for table_row in gold_tables:
    table_name = table_row["tableName"]
    full_table = f"{GOLD_SCHEMA}.{table_name}"
    try:
        df         = spark.table(full_table)
        row_count  = df.count()
        pandas_df  = df.toPandas()
        output     = f"{EXPORT_PATH}{table_name}.csv"
        pandas_df.to_csv(output, index=False)
        print(f" {table_name:<40} {row_count:>8,} rows → {output}")
    except Exception as e:
        print(f" Failed: {table_name} — {e}")

# COMMAND ----------

# COMMAND ----------

# Copy CSVs to Workspace (this IS visible in file browser)
print("\n COPYING TO WORKSPACE...")

csv_count = 0
for csv_file in os.listdir(EXPORT_PATH):
    if csv_file.endswith(".csv"):
        shutil.copy2(
            os.path.join(EXPORT_PATH, csv_file),
            os.path.join(workspace_export, csv_file)
        )
        csv_count += 1
        print(f"  {csv_file}")

# COMMAND ----------

print(f"\n All CSVs saved to {EXPORT_PATH}")
print("\nNext steps:")
print("  1. Download CSVs from /tmp/olist_export/")
print("  2. Download dashboard from /tmp/olist_dashboard.png")
print("  3. Push everything to GitHub")

# COMMAND ----------

