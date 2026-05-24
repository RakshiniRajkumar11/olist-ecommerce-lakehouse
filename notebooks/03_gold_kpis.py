# Databricks notebook source
# MAGIC %run ./02_silver_transformation

# COMMAND ----------

# ============================================================
# 03_GOLD_KPIS — 5 core business KPI tables
# ============================================================

# COMMAND ----------

from pyspark.sql.functions import (
    col, sum, avg, count, round, datediff, desc, dense_rank
)
from pyspark.sql.window import Window

# COMMAND ----------

silver = spark.table(SILVER_TABLE)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print(f"Silver loaded: {silver.count():,} rows\n")

# COMMAND ----------

# ── KPI 1: Monthly Revenue Trend ─────────────────────────────
monthly_revenue = (
    silver
    .filter(col("order_status") == "delivered")
    .filter(col("price").isNotNull())
    .groupBy("year", "month")
    .agg(
        round(sum("price"),  2).alias("total_revenue"),
        count("order_id")     .alias("total_orders"),
        round(avg("price"),  2).alias("avg_order_value")
    )
    .orderBy("year", "month")
)
monthly_revenue.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.monthly_revenue")
print("KPI 1 done — Monthly Revenue")

# COMMAND ----------

# ── KPI 2: Top Sellers by State (window function) ────────────
windowSpec = Window.partitionBy("seller_state").orderBy(desc("revenue"))

top_sellers = (
    silver
    .filter(col("order_status") == "delivered")
    .filter(col("seller_id").isNotNull())
    .groupBy("seller_id", "seller_state", "seller_city")
    .agg(
        round(sum("price"), 2).alias("revenue"),
        count("order_id")     .alias("orders_fulfilled")
    )
    .withColumn("rank_in_state", dense_rank().over(windowSpec))
    .filter(col("rank_in_state") <= 3)
    .orderBy("seller_state", "rank_in_state")
)
top_sellers.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.top_sellers_by_state")
print("KPI 2 done — Top Sellers by State")

# COMMAND ----------

# ── KPI 3: Delivery Delay by Customer State ──────────────────
delivery = (
    silver
    .filter(col("order_status") == "delivered")
    .filter(col("order_delivered_customer_date").isNotNull())
    .filter(col("order_estimated_delivery_date").isNotNull())
    .withColumn("delay_days", datediff(
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
    ))
    .groupBy("customer_state")
    .agg(
        round(avg("delay_days"), 1).alias("avg_delay_days"),
        count("order_id")          .alias("total_orders")
    )
    .orderBy(desc("avg_delay_days"))
)
delivery.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.delivery_delay_by_state")
print("KPI 3 done — Delivery Delay by State")

# COMMAND ----------

# ── KPI 4: Review Score by Category ─────────────────────────
reviews = spark.table(f"{BRONZE_SCHEMA}.reviews")

review_scores = (
    silver
    .join(reviews.select("order_id", "review_score"), on="order_id", how="left")
    .filter(col("category").isNotNull())
    .filter(col("review_score").isNotNull())
    .groupBy("category")
    .agg(
        round(avg("review_score"), 2).alias("avg_review_score"),
        count("order_id")             .alias("total_reviews")
    )
    .orderBy(desc("avg_review_score"))
)
review_scores.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.review_scores_by_category")
print("KPI 4 done — Review Scores by Category")

# COMMAND ----------

# ── KPI 5: Payment Method Breakdown ─────────────────────────
payments = spark.table(f"{BRONZE_SCHEMA}.payment")

payment_analysis = (
    payments
    .groupBy("payment_type")
    .agg(
        count("order_id")          .alias("total_transactions"),
        round(sum("payment_value"), 2).alias("total_value"),
        round(avg("payment_value"), 2).alias("avg_value")
    )
    .orderBy(desc("total_transactions"))
)
payment_analysis.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.payment_breakdown")
print("KPI 5 done — Payment Breakdown")

# COMMAND ----------

print("\n All 5 KPIs written to gold layer")

# COMMAND ----------

