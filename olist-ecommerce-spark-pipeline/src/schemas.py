# ============================================================
# schemas.py
# Explicit PySpark schema definitions for all 9 Olist tables.
# Timestamps are loaded as StringType and cast in clean.py.
# Imported by ingest.py and pipeline.py.
# ============================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

schema_orders = StructType([
    StructField("order_id",                       StringType(),  True),
    StructField("customer_id",                    StringType(),  True),
    StructField("order_status",                   StringType(),  True),
    StructField("order_purchase_timestamp",       StringType(),  True),
    StructField("order_approved_at",              StringType(),  True),
    StructField("order_delivered_carrier_date",   StringType(),  True),
    StructField("order_delivered_customer_date",  StringType(),  True),
    StructField("order_estimated_delivery_date",  StringType(),  True),
])

schema_order_items = StructType([
    StructField("order_id",            StringType(),  True),
    StructField("order_item_id",       IntegerType(), True),
    StructField("product_id",          StringType(),  True),
    StructField("seller_id",           StringType(),  True),
    StructField("shipping_limit_date", StringType(),  True),
    StructField("price",               DoubleType(),  True),
    StructField("freight_value",       DoubleType(),  True),
])

schema_customers = StructType([
    StructField("customer_id",              StringType(), True),
    StructField("customer_unique_id",       StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city",            StringType(), True),
    StructField("customer_state",           StringType(), True),
])

schema_products = StructType([
    StructField("product_id",                 StringType(),  True),
    StructField("product_category_name",      StringType(),  True),
    StructField("product_name_lenght",        IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty",         IntegerType(), True),
    StructField("product_weight_g",           IntegerType(), True),
    StructField("product_length_cm",          IntegerType(), True),
    StructField("product_height_cm",          IntegerType(), True),
    StructField("product_width_cm",           IntegerType(), True),
])

schema_sellers = StructType([
    StructField("seller_id",              StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city",            StringType(), True),
    StructField("seller_state",           StringType(), True),
])

schema_payments = StructType([
    StructField("order_id",             StringType(),  True),
    StructField("payment_sequential",   IntegerType(), True),
    StructField("payment_type",         StringType(),  True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value",        DoubleType(),  True),
])

schema_reviews = StructType([
    StructField("review_id",               StringType(),  True),
    StructField("order_id",                StringType(),  True),
    StructField("review_score",            IntegerType(), True),
    StructField("review_comment_title",    StringType(),  True),
    StructField("review_comment_message",  StringType(),  True),
    StructField("review_creation_date",    StringType(),  True),
    StructField("review_answer_timestamp", StringType(),  True),
])

schema_geolocation = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat",             DoubleType(), True),
    StructField("geolocation_lng",             DoubleType(), True),
    StructField("geolocation_city",            StringType(), True),
    StructField("geolocation_state",           StringType(), True),
])

schema_category_tr = StructType([
    StructField("product_category_name",         StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])

ALL_SCHEMAS = {
    "orders"      : schema_orders,
    "order_items" : schema_order_items,
    "customers"   : schema_customers,
    "products"    : schema_products,
    "sellers"     : schema_sellers,
    "payments"    : schema_payments,
    "reviews"     : schema_reviews,
    "geolocation" : schema_geolocation,
    "category_tr" : schema_category_tr,
}
