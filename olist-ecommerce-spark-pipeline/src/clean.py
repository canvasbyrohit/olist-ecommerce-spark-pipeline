# ============================================================
# clean.py
# Data cleaning functions for the Olist ETL pipeline.
# Applied after ingestion, before integration.
# Covers: timestamp casting, deduplication, null handling,
#         invalid value filtering, column standardisation.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from src.config import (
    TIMESTAMP_FORMAT, VALID_STATUSES,
    GEO_LAT_MIN, GEO_LAT_MAX,
    GEO_LNG_MIN, GEO_LNG_MAX,
    IQR_MULTIPLIER
)


def trim_strings(df):
    """Trim leading/trailing whitespace from all string columns."""
    str_cols = [f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType)]
    for c in str_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def clean_orders(df):
    """
    Clean the orders DataFrame.
    - Cast 5 timestamp columns from string to TimestampType
    - Deduplicate on order_id
    - Drop rows with null critical keys
    - Filter to known valid order statuses
    - Trim string columns
    """
    df = df \
        .withColumn("order_purchase_timestamp",
            F.to_timestamp("order_purchase_timestamp", TIMESTAMP_FORMAT)) \
        .withColumn("order_approved_at",
            F.to_timestamp("order_approved_at", TIMESTAMP_FORMAT)) \
        .withColumn("order_delivered_carrier_date",
            F.to_timestamp("order_delivered_carrier_date", TIMESTAMP_FORMAT)) \
        .withColumn("order_delivered_customer_date",
            F.to_timestamp("order_delivered_customer_date", TIMESTAMP_FORMAT)) \
        .withColumn("order_estimated_delivery_date",
            F.to_timestamp("order_estimated_delivery_date", TIMESTAMP_FORMAT))

    df = df.dropDuplicates(["order_id"])
    df = df.dropna(subset=["order_id", "customer_id",
                            "order_purchase_timestamp"])
    df = df.filter(F.col("order_status").isin(VALID_STATUSES))
    df = trim_strings(df)
    return df


def clean_order_items(df):
    """
    Clean the order_items DataFrame.
    - Cast shipping_limit_date to TimestampType
    - Deduplicate on (order_id, order_item_id)
    - Drop rows with null critical keys or price
    - Fill null freight_value with 0.0
    - Filter negative prices and freight
    - Apply 3xIQR outlier filter on price
    """
    df = df.withColumn("shipping_limit_date",
        F.to_timestamp("shipping_limit_date", TIMESTAMP_FORMAT))

    df = df.dropDuplicates(["order_id", "order_item_id"])
    df = df.dropna(subset=["order_id", "product_id", "seller_id", "price"])
    df = df.fillna({"freight_value": 0.0})
    df = df.filter((F.col("price") >= 0) & (F.col("freight_value") >= 0))

    # 3xIQR outlier removal on price
    stats = df.select(
        F.expr("percentile_approx(price, 0.25)").alias("q1"),
        F.expr("percentile_approx(price, 0.75)").alias("q3")
    ).collect()[0]
    iqr_upper = stats["q3"] + IQR_MULTIPLIER * (stats["q3"] - stats["q1"])
    df = df.filter(F.col("price") <= iqr_upper)
    return df


def clean_customers(df):
    """
    Clean the customers DataFrame.
    - Deduplicate on customer_id
    - Drop rows with null customer_id or state
    - Fill null city and zip with sentinel values
    - Standardise state to uppercase, city to title case
    """
    df = df.dropDuplicates(["customer_id"])
    df = df.dropna(subset=["customer_id", "customer_state"])
    df = df.fillna({
        "customer_city"            : "unknown",
        "customer_zip_code_prefix" : "00000"
    })
    df = df.withColumn("customer_state", F.upper("customer_state"))
    df = df.withColumn("customer_city",  F.initcap("customer_city"))
    df = trim_strings(df)
    return df


def clean_products(df):
    """
    Clean the products DataFrame.
    - Deduplicate on product_id
    - Drop rows with null product_id
    - Fill null category with 'uncategorised'
    - Fill null dimensions/weight with 0
    - Correct column name typos (lenght -> length)
    """
    df = df.dropDuplicates(["product_id"])
    df = df.dropna(subset=["product_id"])
    df = df.fillna({
        "product_category_name"      : "uncategorised",
        "product_weight_g"           : 0,
        "product_length_cm"          : 0,
        "product_height_cm"          : 0,
        "product_width_cm"           : 0,
        "product_photos_qty"         : 0,
        "product_name_lenght"        : 0,
        "product_description_lenght" : 0,
    })
    # Correct documented typos in original Kaggle dataset
    df = df \
        .withColumnRenamed("product_name_lenght",
                           "product_name_length") \
        .withColumnRenamed("product_description_lenght",
                           "product_description_length")
    df = trim_strings(df)
    return df


def clean_sellers(df):
    """
    Clean the sellers DataFrame.
    - Deduplicate on seller_id
    - Drop rows with null seller_id
    - Standardise state to uppercase, city to title case
    """
    df = df.dropDuplicates(["seller_id"])
    df = df.dropna(subset=["seller_id"])
    df = df.withColumn("seller_state", F.upper("seller_state"))
    df = df.withColumn("seller_city",  F.initcap("seller_city"))
    df = trim_strings(df)
    return df


def clean_payments(df):
    """
    Clean the payments DataFrame.
    - Deduplicate on (order_id, payment_sequential)
    - Drop rows with null order_id or payment_value
    - Fill null payment_type with 'unknown'
    - Filter zero and negative payment values
    """
    df = df.dropDuplicates(["order_id", "payment_sequential"])
    df = df.dropna(subset=["order_id", "payment_value"])
    df = df.fillna({"payment_type": "unknown", "payment_installments": 1})
    df = df.filter(F.col("payment_value") > 0)
    return df


def clean_reviews(df):
    """
    Clean the reviews DataFrame.
    - Cast timestamp columns from string to TimestampType
    - Deduplicate on review_id
    - Drop rows with null order_id or review_score
    - Fill null comment fields with sentinel values
    - Filter review scores outside valid range 1-5
    """
    df = df \
        .withColumn("review_creation_date",
            F.to_timestamp("review_creation_date", TIMESTAMP_FORMAT)) \
        .withColumn("review_answer_timestamp",
            F.to_timestamp("review_answer_timestamp", TIMESTAMP_FORMAT))

    df = df.dropDuplicates(["review_id"])
    df = df.dropna(subset=["order_id", "review_score"])
    df = df.fillna({
        "review_comment_title"   : "no title",
        "review_comment_message" : "no comment"
    })
    df = df.filter(F.col("review_score").between(1, 5))
    return df


def clean_geolocation(df):
    """
    Clean the geolocation DataFrame.
    - Deduplicate on (zip_code, lat, lng)
    - Drop rows with null coordinate or zip fields
    - Filter coordinates outside Brazil's bounding box
    """
    df = df.dropDuplicates([
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng"
    ])
    df = df.dropna(subset=[
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng"
    ])
    df = df.filter(
        F.col("geolocation_lat").between(GEO_LAT_MIN, GEO_LAT_MAX) &
        F.col("geolocation_lng").between(GEO_LNG_MIN, GEO_LNG_MAX)
    )
    return df


def clean_category_translation(df):
    """Drop any rows with nulls in the category translation table."""
    return df.dropna()


def clean_all_tables(raw):
    """
    Apply cleaning functions to all 9 raw DataFrames.

    Parameters
    ----------
    raw : dict of {table_name: DataFrame}

    Returns
    -------
    dict of {table_name: cleaned DataFrame}
    """
    return {
        "orders"      : clean_orders(raw["orders"]),
        "order_items" : clean_order_items(raw["order_items"]),
        "customers"   : clean_customers(raw["customers"]),
        "products"    : clean_products(raw["products"]),
        "sellers"     : clean_sellers(raw["sellers"]),
        "payments"    : clean_payments(raw["payments"]),
        "reviews"     : clean_reviews(raw["reviews"]),
        "geolocation" : clean_geolocation(raw["geolocation"]),
        "category_tr" : clean_category_translation(raw["category_tr"]),
    }


def print_quality_report(raw, clean):
    """Print before/after row counts for all tables."""
    print(f"\n{'Table':<18} {'Raw':>10} {'Clean':>10} "
          f"{'Removed':>10} {'Retained%':>12}")
    print("-" * 64)
    total_raw = total_clean = 0
    for name in raw:
        r = raw[name].count()
        c = clean[name].count()
        removed = r - c
        pct = c / r * 100 if r > 0 else 0
        total_raw   += r
        total_clean += c
        print(f"{name:<18} {r:>10,} {c:>10,} "
              f"{removed:>10,} {pct:>11.1f}%")
    print("-" * 64)
    t_removed = total_raw - total_clean
    t_pct = total_clean / total_raw * 100
    print(f"{'TOTAL':<18} {total_raw:>10,} {total_clean:>10,} "
          f"{t_removed:>10,} {t_pct:>11.1f}%\n")
