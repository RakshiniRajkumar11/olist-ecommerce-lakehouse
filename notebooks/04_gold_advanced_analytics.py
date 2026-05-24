# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(SILVER_TABLE)

# COMMAND ----------

# ── Analysis 1: Customer Lifetime Value ──────────────────────
customer_clv = (
    silver
    .filter(F.col("order_status") == "delivered")
    .groupBy("customer_id", "customer_state")
    .agg(
        F.count("order_id")                                           .alias("total_orders"),
        F.round(F.sum("price"), 2)                                    .alias("lifetime_value"),
        F.round(F.avg("price"), 2)                                    .alias("avg_order_value"),
        F.datediff(F.max("order_purchase_timestamp"),
                   F.min("order_purchase_timestamp"))                 .alias("customer_tenure_days")
    )
    .withColumn("clv_segment",
        F.when(F.col("lifetime_value") >= 1000, "High Value")
         .when(F.col("lifetime_value") >= 500,  "Medium Value")
         .otherwise("Low Value")
    )
)
customer_clv.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.customer_lifetime_value")

print("CLV SEGMENTATION:")
customer_clv.groupBy("clv_segment").agg(
    F.count("*")                         .alias("customers"),
    F.round(F.sum("lifetime_value"),  2) .alias("total_revenue"),
    F.round(F.avg("lifetime_value"),  2) .alias("avg_clv")
).orderBy(F.desc("total_revenue")).show()
print(" CLV saved")


# COMMAND ----------

# ── Analysis 2: Month-over-Month Revenue Growth ───────────────
window_spec     = Window.orderBy("year", "month")
monthly_rev     = spark.table(f"{GOLD_SCHEMA}.monthly_revenue")

revenue_trends = (
    monthly_rev
    .withColumn("prev_month_revenue",
        F.lag("total_revenue", 1).over(window_spec))
    .withColumn("mom_growth_pct",
        F.round(((F.col("total_revenue") - F.col("prev_month_revenue")) /
                  F.col("prev_month_revenue")) * 100, 2))
    .withColumn("prev_month_orders",
        F.lag("total_orders", 1).over(window_spec))
    .withColumn("mom_orders_growth_pct",
        F.round(((F.col("total_orders") - F.col("prev_month_orders")) /
                  F.col("prev_month_orders")) * 100, 2))
)
revenue_trends.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.revenue_trends")
print("Month-Over-Month Revenue Trends saved")

# COMMAND ----------

# ── Analysis 3: Product Performance Matrix (BCG-style) ───────
product_performance = (
    silver
    .filter(F.col("order_status") == "delivered")
    .filter(F.col("category").isNotNull())
    .groupBy("category")
    .agg(
        F.count("order_id")          .alias("order_volume"),
        F.round(F.sum("price"),  2)  .alias("total_revenue"),
        F.round(F.avg("price"),  2)  .alias("avg_price")
    )
    .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("volume_rank",  F.dense_rank().over(Window.orderBy(F.desc("order_volume"))))
    .withColumn("category_type",
        F.when((F.col("revenue_rank") <= 10) & (F.col("volume_rank") <= 10), "Star")
         .when((F.col("revenue_rank") <= 10) & (F.col("volume_rank") >  10), "Premium")
         .when((F.col("revenue_rank") >  10) & (F.col("volume_rank") <= 10), "Volume")
         .otherwise("Long Tail")
    )
)
product_performance.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.product_performance_matrix")
print("✅ Product Matrix saved")

# COMMAND ----------

# ── Analysis 4: Seller Performance Scorecard ─────────────────
reviews_table = spark.table(f"{BRONZE_SCHEMA}.reviews")

seller_scorecard = (
    silver
    .filter(F.col("order_status") == "delivered")
    .filter(F.col("seller_id").isNotNull())
    .join(reviews_table.select("order_id", "review_score"), on="order_id", how="left")
    .groupBy("seller_id", "seller_state", "seller_city")
    .agg(
        F.round(F.sum("price"),         2).alias("total_revenue"),
        F.count("order_id")              .alias("orders_fulfilled"),
        F.round(F.avg("price"),         2).alias("avg_order_value"),
        F.round(F.avg("review_score"),  2).alias("avg_rating"),
        F.count(F.when(F.col("review_score") >= 4, 1)).alias("positive_reviews")
    )
    .withColumn("performance_score",
        F.round((F.col("total_revenue")    / 1000) * 0.4 +
                (F.col("orders_fulfilled") / 10)   * 0.3 +
                 F.col("avg_rating") * 20          * 0.3, 2))
    .withColumn("seller_tier",
        F.when(F.col("performance_score") >= 80, "Elite")
         .when(F.col("performance_score") >= 60, "Advanced")
         .when(F.col("performance_score") >= 40, "Intermediate")
         .otherwise("Beginner")
    )
)
seller_scorecard.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_SCHEMA}.seller_performance_scorecard")
print("Seller Scorecard saved")


# COMMAND ----------

print("\n All advanced analytics written to gold layer")
