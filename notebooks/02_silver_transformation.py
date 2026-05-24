# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./01_bronze_ingestion

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, month, year
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# Load bronze tables
orders      = spark.table(f"{BRONZE_SCHEMA}.orders")
order_items = spark.table(f"{BRONZE_SCHEMA}.order_items")
products    = spark.table(f"{BRONZE_SCHEMA}.products")
sellers     = spark.table(f"{BRONZE_SCHEMA}.sellers")
customers   = spark.table(f"{BRONZE_SCHEMA}.customers")
category    = spark.table(f"{BRONZE_SCHEMA}.category")

print("Bronze tables loaded")

# COMMAND ----------

# --- Clean orders ---
date_cols = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
orders_clean = orders
for c in date_cols:
    orders_clean = orders_clean.withColumn(c, to_timestamp(col(c)))
orders_clean = orders_clean.dropna(subset=["order_id", "customer_id"])
print(f"Orders  — before: {orders.count():,}  after: {orders_clean.count():,}")

# COMMAND ----------

# --- Clean order_items ---
order_items_clean = (
    order_items
    .withColumn("price",          col("price").cast(DoubleType()))
    .withColumn("freight_value",  col("freight_value").cast(DoubleType()))
    .withColumn("order_item_id",  col("order_item_id").cast(IntegerType()))
    .dropna(subset=["order_id", "product_id"])
)
print(f"Items   — before: {order_items.count():,}  after: {order_items_clean.count():,}")


# COMMAND ----------

# --- Enrich products with English category names ---
products_clean = (
    products
    .join(category, on="product_category_name", how="left")
    .drop("product_category_name")
    .withColumnRenamed("product_category_name_english", "category")
    .fillna("unknown", subset=["category"])
)
print(f"Products— before: {products.count():,}  after: {products_clean.count():,}")


# COMMAND ----------

# --- Build silver master (star schema join) ---
silver_df = (
    orders_clean
    .join(order_items_clean, on="order_id",   how="left")
    .join(products_clean,    on="product_id", how="left")
    .join(customers,         on="customer_id",how="left")
    .join(sellers,           on="seller_id",  how="left")
    .withColumn("year",  year(col("order_purchase_timestamp")))
    .withColumn("month", month(col("order_purchase_timestamp")))
)

print(f"\nSilver master: {silver_df.count():,} rows, {len(silver_df.columns)} columns")


# COMMAND ----------

# --- Write to silver ---
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .saveAsTable(SILVER_TABLE)
)

print(f"\n Silver table written: {SILVER_TABLE}")
silver_df.printSchema()