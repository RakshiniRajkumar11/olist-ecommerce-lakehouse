# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

BASE_URL="https://raw.githubusercontent.com/wheff70/OlistDataAnalysis/main/"
BASE_URL2="https://raw.githubusercontent.com/Leprofesseur18/Olist-Customer-Datasets/main/"

# COMMAND ----------

DATASET ={
    "orders": BASE_URL+"olist_orders_dataset.csv",
    "order_items": BASE_URL+"olist_order_items_dataset.csv",
    "products": BASE_URL+"olist_products_dataset.csv",
    "sellers": BASE_URL2+"olist_sellers_dataset.csv",
    "customers": BASE_URL+"olist_customers_dataset.csv",
    "geolocation": BASE_URL2+"olist_geolocation_dataset.csv",
    "payment": BASE_URL+"olist_order_payments_dataset.csv",
    "reviews": BASE_URL2+"olist_order_reviews_dataset.csv",
    "category": BASE_URL+"product_category_name_translation.csv"
}

# Catalog paths
BRONZE_SCHEMA = "workspace.olist_bronze"
SILVER_SCHEMA = "workspace.olist_silver"
GOLD_SCHEMA   = "workspace.olist_gold"

# COMMAND ----------

SILVER_TABLE  = f"{SILVER_SCHEMA}.orders_master"

# Export path
EXPORT_PATH   = "/tmp/olist_export/"

print("Config loaded")
print(f"   Bronze : {BRONZE_SCHEMA}")
print(f"   Silver : {SILVER_SCHEMA}")
print(f"   Gold   : {GOLD_SCHEMA}")