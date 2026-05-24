# Olist E-Commerce Data Lakehouse Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Enabled-blue)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Governance-green)](https://databricks.com/product/unity-catalog)
[![Python](https://img.shields.io/badge/Python-3.x-yellow)](https://python.org/)

## Overview

Production-grade data lakehouse pipeline implementing **Medallion Architecture (Bronze → Silver → Gold)** 
to analyze 113,000+ Brazilian e-commerce orders from the Olist public dataset. Built on Databricks 
with Unity Catalog governance, PySpark transformations, and Delta Lake storage — generating 
actionable business insights across revenue, delivery, customer segmentation, and product analytics.

## Dashboard Preview

![Analytics Dashboard](images/olist_dashboard.png)

## Project Highlights

- **113,425 orders** processed across 9 source tables
- **Medallion architecture** with Delta Lake ACID transactions
- **9 analytical tables** in gold layer with pre-computed KPIs
- **4 advanced analytics** modules — CLV, MoM growth, product matrix, seller scorecard
- **Data quality checks** with automated null and duplicate detection
- **6-panel visualization dashboard** with matplotlib and seaborn

## Architecture
CSV Sources → Bronze Layer → Silver Layer → Gold Layer → Dashboard
(Raw)         (Cleaned)      (KPIs)
↓               ↓             ↓
Delta Lake      Unity Catalog   Business Metrics
9 tables        1 master table  9 analytical tables

## Tech Stack

| Component | Technology |
|-----------|------------|
| Compute | Databricks Serverless (auto-scaling) |
| Storage | Unity Catalog + Delta Lake |
| Language | PySpark (DataFrame API) + Python |
| Visualization | Matplotlib + Seaborn |
| Data Format | Delta (Parquet + transaction log) |

## Repository Structure
olist-ecommerce-lakehouse/
├── notebooks/
│   ├── 00_config.ipynb                   # Shared constants and paths
│   ├── 01_bronze_ingestion.ipynb         # Load 9 CSVs into Delta tables
│   ├── 02_data_quality.ipynb             # Null checks and duplicate detection
│   ├── 03_silver_transformation.ipynb    # Clean, join, partition → silver master
│   ├── 04_gold_kpis.ipynb                # 5 core business KPI tables
│   ├── 05_gold_advanced_analytics.ipynb  # CLV, MoM growth, product matrix, scorecard
│   ├── 06_visualizations.ipynb           # 6-panel analytics dashboard
│   └── 07_export.ipynb                   # Export gold tables to CSV
├── images/
│   └── olist_dashboard.png               # Dashboard screenshot
├── data/
│   └── sample_outputs/                   # Sample CSVs from gold tables
├── requirements.txt
└── README.md

## Notebooks Guide

| Notebook | Purpose | Runs after |
|----------|---------|------------|
| `00_config` | Paths, schema names, dataset URLs | — |
| `01_bronze_ingestion` | Ingest 9 CSVs into Delta bronze tables | 00 |
| `02_data_quality` | Validate nulls and duplicates on bronze | 01 |
| `03_silver_transformation` | Clean + join into silver master table | 01 |
| `04_gold_kpis` | Compute 5 KPI tables in gold layer | 03 |
| `05_gold_advanced_analytics` | CLV, MoM growth, product matrix, seller scorecard | 04 |
| `06_visualizations` | Build and save 6-panel dashboard | 05 |
| `07_export` | Export all gold tables to CSV | 04 |

## Key Metrics Generated

1. **Monthly Revenue Trend** — Revenue and order volume with MoM growth %
2. **Top Sellers by State** — Ranked using PySpark window functions (`dense_rank`)
3. **Delivery Performance** — Avg delay vs estimate by customer state
4. **Product Category Ratings** — Review scores aggregated by category
5. **Payment Method Analysis** — Transaction volume and value by payment type
6. **Customer Lifetime Value** — High / Medium / Low value segmentation
7. **Product Performance Matrix** — BCG-style: Star / Premium / Volume / Long Tail
8. **Seller Performance Scorecard** — Elite / Advanced / Intermediate / Beginner tiers
9. **Month-over-Month Growth** — Revenue and order volume growth using `lag()`

## PySpark Techniques Used

- **Window Functions** — `dense_rank()`, `lag()` for rankings and growth analysis
- **Date Operations** — `datediff()`, `to_timestamp()` for delivery analysis
- **Star Schema Joins** — orders + items + products + customers + sellers
- **Partitioning** — Year/month partitioning on silver layer for query performance
- **Delta Lake** — ACID transactions, overwrite mode for idempotent runs
- **Unity Catalog** — Centralized schema governance across bronze/silver/gold

## Data Quality

Every bronze table is validated for:
- Null rates per column with percentage reporting
- Duplicate row detection
- Automated summary report with `display()`

## Quick Start

### Prerequisites
- Databricks workspace (Community Edition works)
- Unity Catalog enabled
- Serverless compute or any cluster

### Steps

1. Clone this repo
```bash
git clone https://github.com/RakshiniRajkumar11/olist-ecommerce-lakehouse.git
```

2. Import notebooks into Databricks
   - Workspace → Import → select each `.ipynb` from `notebooks/`

3. Run in order
00 → 01 → 02 → 03 → 04 → 05 → 06 → 07

4. View dashboard in `olist_downloads` folder in your Workspace

### Data Source
Dataset loads automatically from public GitHub URLs — no manual download needed.

## Optimizations Applied

| Technique | Status |
|-----------|--------|
| Delta Lake ACID transactions | ✅ Implemented |
| Unity Catalog governance | ✅ Implemented |
| Year/month partitioning on silver | ✅ Implemented |
| Column pruning in joins | ✅ Implemented |
| Z-ordering on gold tables | ⚠️ Not implemented |
| OPTIMIZE + VACUUM jobs | ⚠️ Not implemented |
| Incremental processing (CDF) | ⚠️ Not implemented |

## Business Insights

- **Delivery Excellence** — Majority of orders delivered ahead of estimated date
- **Payment Dominance** — Credit card is the primary payment method
- **Revenue Concentration** — Top 20% of sellers drive majority of revenue
- **Customer Retention Gap** — Most customers are one-time buyers, retention opportunity identified
- **Star Categories** — Identified high-volume + high-revenue product categories

## Future Enhancements

- [ ] Z-ordering on frequently filtered columns
- [ ] Incremental processing with Delta Change Data Feed
- [ ] Demand forecasting ML model
- [ ] Real-time streaming with Delta Live Tables
- [ ] Automated VACUUM and OPTIMIZE jobs
- [ ] Databricks SQL dashboard for live monitoring

## Author

**Rakshini Rajkumar**
- LinkedIn: [linkedin.com/in/rakshinirajkumar](https://linkedin.com/in/rakshinirajkumar)
- GitHub: [github.com/RakshiniRajkumar11](https://github.com/RakshiniRajkumar11)

---
*Dataset: Olist Brazilian E-Commerce Public Dataset (Kaggle)*
*Built on Databricks Lakehouse Platform*
