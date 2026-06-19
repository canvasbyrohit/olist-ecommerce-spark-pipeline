# ============================================================
# config.py
# Centralised configuration for the Olist Spark ETL pipeline.
# Update BASE_PATH to match your local or Drive directory.
# ============================================================

import os

# ── Paths ───────────────────────────────────────────────────
BASE_PATH      = "/content/drive/MyDrive/olist_data"
RAW_PATH       = f"{BASE_PATH}/raw"
PROCESSED_PATH = f"{BASE_PATH}/processed"
VIZ_PATH       = f"{BASE_PATH}/visualisations"

# ── Output Parquet paths ─────────────────────────────────────
PATHS = {
    "integrated"   : f"{PROCESSED_PATH}/olist_integrated.parquet",
    "aggregated"   : f"{PROCESSED_PATH}/olist_aggregated.parquet",
    "orders_clean" : f"{PROCESSED_PATH}/orders_clean.parquet",
    "items_clean"  : f"{PROCESSED_PATH}/order_items_clean.parquet",
    "reviews_clean": f"{PROCESSED_PATH}/reviews_clean.parquet",
}

# ── Source file paths ────────────────────────────────────────
FILE_PATHS = {
    "orders"      : f"{RAW_PATH}/olist_orders_dataset.csv",
    "order_items" : f"{RAW_PATH}/olist_order_items_dataset.csv",
    "customers"   : f"{RAW_PATH}/olist_customers_dataset.csv",
    "products"    : f"{RAW_PATH}/olist_products_dataset.csv",
    "sellers"     : f"{RAW_PATH}/olist_sellers_dataset.csv",
    "payments"    : f"{RAW_PATH}/olist_order_payments_dataset.csv",
    "reviews"     : f"{RAW_PATH}/olist_order_reviews_dataset.csv",
    "geolocation" : f"{RAW_PATH}/olist_geolocation_dataset.csv",
    "category_tr" : f"{RAW_PATH}/product_category_name_translation.csv",
}

# ── Spark settings ───────────────────────────────────────────
SPARK_APP_NAME           = "Olist_Brazilian_Ecommerce_Pipeline"
SPARK_SHUFFLE_PARTITIONS = "8"
SPARK_DRIVER_MEMORY      = "4g"

# ── Pipeline settings ────────────────────────────────────────
TIMESTAMP_FORMAT    = "yyyy-MM-dd HH:mm:ss"
PARQUET_COMPRESSION = "snappy"
PARQUET_MODE        = "overwrite"

VALID_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced",  "processing", "created", "approved"
]

# Brazil geographic bounding box for geolocation validation
GEO_LAT_MIN, GEO_LAT_MAX = -33.75,  5.27
GEO_LNG_MIN, GEO_LNG_MAX = -73.99, -28.85

# Outlier filter: IQR multiplier for price column
IQR_MULTIPLIER = 3.0

# Minimum order count threshold for seller quality analysis
SELLER_MIN_ORDERS = 50
