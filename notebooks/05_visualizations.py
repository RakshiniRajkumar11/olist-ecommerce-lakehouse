# Databricks notebook source
# MAGIC %run ./04_gold_advanced_analytics

# COMMAND ----------

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os

# ── Load gold tables ──────────────────────────────────────────
monthly_revenue = spark.table(f"{GOLD_SCHEMA}.monthly_revenue").toPandas()
top_sellers     = spark.table(f"{GOLD_SCHEMA}.top_sellers_by_state").toPandas()
delivery        = spark.table(f"{GOLD_SCHEMA}.delivery_delay_by_state").toPandas()
review_scores   = spark.table(f"{GOLD_SCHEMA}.review_scores_by_category").toPandas()
payments        = spark.table(f"{GOLD_SCHEMA}.payment_breakdown").toPandas()

# ── Prep ──────────────────────────────────────────────────────
monthly_revenue["period"] = (
    monthly_revenue["year"].astype(str) + "-" +
    monthly_revenue["month"].astype(str).str.zfill(2)
)
monthly_revenue = monthly_revenue.sort_values("period")

# ── Build figure ──────────────────────────────────────────────
sns.set_theme(style="darkgrid")
fig, axes = plt.subplots(3, 2, figsize=(20, 24))
fig.suptitle("Olist E-Commerce Analytics Dashboard",
             fontsize=22, fontweight="bold", y=0.98)

ax1, ax2, ax3, ax4, ax5, ax6 = axes.flatten()

# Chart 1 — Monthly Revenue Trend
ax1.plot(monthly_revenue["period"], monthly_revenue["total_revenue"],
         marker="o", color="#2196F3", linewidth=2.5, markersize=5)
ax1.fill_between(monthly_revenue["period"], monthly_revenue["total_revenue"],
                 alpha=0.15, color="#2196F3")
ax1.set_title("Monthly Revenue Trend", fontsize=13, fontweight="bold")
ax1.set_xlabel("Period"); ax1.set_ylabel("Revenue (R$)")
ax1.tick_params(axis="x", rotation=60)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1000:.0f}K"))

# Chart 2 — Monthly Orders Volume
ax2.bar(monthly_revenue["period"], monthly_revenue["total_orders"],
        color="#4CAF50", alpha=0.85)
ax2.set_title("Monthly Orders Volume", fontsize=13, fontweight="bold")
ax2.set_xlabel("Period"); ax2.set_ylabel("Total Orders")
ax2.tick_params(axis="x", rotation=60)

# Chart 3 — Payment Method Breakdown
colors = ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF"]
payments_sorted = payments.sort_values("total_transactions", ascending=True)
bars = ax3.barh(payments_sorted["payment_type"],
                payments_sorted["total_transactions"],
                color=colors[:len(payments_sorted)])
for bar, val in zip(bars, payments_sorted["total_transactions"]):
    ax3.text(bar.get_width() + 300, bar.get_y() + bar.get_height() / 2,
             f"{val:,}", va="center", fontsize=9)
ax3.set_title("Payment Method Breakdown", fontsize=13, fontweight="bold")
ax3.set_xlabel("Total Transactions")

# Chart 4 — Delivery Performance
delivery_sorted = delivery.sort_values("avg_delay_days", ascending=True)
bar_colors = ["#4CAF50" if x < 0 else "#F44336"
              for x in delivery_sorted["avg_delay_days"]]
ax4.barh(delivery_sorted["customer_state"],
         delivery_sorted["avg_delay_days"], color=bar_colors)
ax4.axvline(x=0, color="black", linewidth=1, linestyle="--")
ax4.set_title("Avg Delivery vs Estimate\nGreen = Early, Red = Late",
              fontsize=13, fontweight="bold")
ax4.set_xlabel("Days (negative = early)")

# Chart 5 — Top 15 Categories by Review Score
top_reviews = review_scores.sort_values("avg_review_score", ascending=False).head(15)
sns.barplot(data=top_reviews, x="avg_review_score", y="category",
            palette="YlGn", ax=ax5)
ax5.set_title("Top 15 Categories by Review Score", fontsize=13, fontweight="bold")
ax5.set_xlabel("Avg Review Score (out of 5)"); ax5.set_ylabel("")
ax5.set_xlim(3.5, 5.0)

# Chart 6 — Top 10 Sellers by Revenue
top10 = top_sellers.sort_values("revenue", ascending=False).head(10)
sns.barplot(data=top10, x="revenue", y="seller_city", palette="Blues_r", ax=ax6)
ax6.set_title("Top 10 Sellers by Revenue", fontsize=13, fontweight="bold")
ax6.set_xlabel("Revenue (R$)"); ax6.set_ylabel("Seller City")
ax6.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x/1000:.0f}K"))

# ── Save ──────────────────────────────────────────────────────
plt.tight_layout(rect=[0, 0, 1, 0.97])

workspace_export = "/Workspace/Users/rakshinirajkumar11022004@gmail.com/olist_downloads"
os.makedirs(workspace_export, exist_ok=True)

save_path = f"{workspace_export}/olist_dashboard.png"
plt.savefig(save_path, dpi=150, bbox_inches="tight")
plt.show()
print(f"Dashboard saved — check olist_downloads folder")