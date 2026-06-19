# ============================================================
# integrate.py
# Data integration functions for the Olist ETL pipeline.
# Implements star-schema join across 6 core tables.
# Steps:
#   1. Category translation alignment (Portuguese -> English)
#   2. Derived columns on orders and items tables
#   3. Payments pre-aggregation to order level
#   4. Reviews deduplication to order level
#   5. Main star-schema integration join
#   6. Final column selection and renaming
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def align_categories(products_clean, category_tr_clean):
    """
    Join product category translation table to products.
    Falls back to Portuguese name where no English exists.
    Returns products DataFrame with 'product_category' column (English).
    """
    df = products_clean.join(
        category_tr_clean,
        on="product_category_name",
        how="left"
    ).withColumn(
        "product_category",
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name")
        )
    ).drop("product_category_name", "product_category_name_english")

    return df


def enrich_orders(orders_clean):
    """
    Add derived columns to the orders DataFrame:
    - delivery_delay_days  : signed int (positive=late, negative=early)
    - processing_time_days : purchase to approval in days
    - order_year           : year extracted from purchase timestamp
    - order_month          : month number extracted from purchase timestamp
    - order_month_label    : formatted as 'yyyy-MM' for time-series plots
    - order_day_of_week    : full day name (Monday, Tuesday, etc.)
    - is_late              : boolean True if delivered after estimated date
    """
    return orders_clean \
        .withColumn("delivery_delay_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date")
            )) \
        .withColumn("processing_time_days",
            F.datediff(
                F.col("order_approved_at"),
                F.col("order_purchase_timestamp")
            )) \
        .withColumn("order_year",
            F.year("order_purchase_timestamp")) \
        .withColumn("order_month",
            F.month("order_purchase_timestamp")) \
        .withColumn("order_month_label",
            F.date_format("order_purchase_timestamp", "yyyy-MM")) \
        .withColumn("order_day_of_week",
            F.date_format("order_purchase_timestamp", "EEEE")) \
        .withColumn("is_late",
            F.when(F.col("delivery_delay_days") > 0, True)
             .otherwise(False))


def enrich_items(items_clean):
    """
    Add derived revenue columns to the order_items DataFrame:
    - total_item_value : price + freight_value (rounded to 2dp)
    - freight_ratio    : freight as proportion of price (null if price=0)
    """
    return items_clean \
        .withColumn("total_item_value",
            F.round(F.col("price") + F.col("freight_value"), 2)) \
        .withColumn("freight_ratio",
            F.when(
                F.col("price") > 0,
                F.round(F.col("freight_value") / F.col("price"), 4)
            ).otherwise(F.lit(None)))


def aggregate_payments(payments_clean):
    """
    Aggregate the payments table from multiple rows per order
    to one row per order_id.
    Aggregations:
    - total_payment_value  : sum of all payment values
    - max_installments     : maximum instalment count used
    - payment_types_used   : comma-separated unique payment types
    - payment_count        : number of payment transactions
    """
    return payments_clean.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2)
         .alias("total_payment_value"),
        F.max("payment_installments")
         .alias("max_installments"),
        F.array_join(F.collect_set("payment_type"), ", ")
         .alias("payment_types_used"),
        F.count("payment_sequential")
         .alias("payment_count")
    )


def deduplicate_reviews(reviews_clean):
    """
    Reduce reviews to one row per order_id by selecting the
    most recent review (highest review_creation_date).
    Uses row_number() window function.
    """
    window = Window.partitionBy("order_id") \
                   .orderBy(F.col("review_creation_date").desc())

    return reviews_clean \
        .withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn") \
        .select(
            "order_id",
            "review_score",
            "review_creation_date",
            "review_comment_message"
        )


def integrate_tables(clean):
    """
    Build the unified integrated dataset from cleaned tables.

    Join sequence (star schema):
      orders (fact centre)
        -> order_items  [inner] — only orders with line items
        -> customers    [left]
        -> products     [left]
        -> sellers      [left]
        -> payments_agg [left]
        -> reviews_agg  [left]

    Parameters
    ----------
    clean : dict of {table_name: cleaned DataFrame}

    Returns
    -------
    df_final : DataFrame — unified analytical dataset (34 columns)
    """
    # Step 1: align category names to English
    products_aligned = align_categories(
        clean["products"], clean["category_tr"]
    )

    # Step 2: enrich orders and items with derived columns
    orders_enriched = enrich_orders(clean["orders"])
    items_enriched  = enrich_items(clean["order_items"])

    # Step 3: pre-aggregate payments to order level
    payments_agg = aggregate_payments(clean["payments"])

    # Step 4: deduplicate reviews to order level
    reviews_agg = deduplicate_reviews(clean["reviews"])

    # Step 5: star-schema join
    df = orders_enriched \
        .join(items_enriched,   on="order_id",    how="inner") \
        .join(clean["customers"], on="customer_id", how="left") \
        .join(products_aligned,  on="product_id",  how="left") \
        .join(clean["sellers"],  on="seller_id",   how="left") \
        .join(payments_agg,      on="order_id",    how="left") \
        .join(reviews_agg,       on="order_id",    how="left")

    # Step 6: select and order final columns
    df_final = df.select(
        # Identifiers
        "order_id", "customer_id", "product_id",
        "seller_id", "order_item_id",

        # Order metadata
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_year", "order_month",
        "order_month_label", "order_day_of_week",

        # Delivery metrics
        "delivery_delay_days",
        "processing_time_days",
        "is_late",

        # Revenue metrics
        "price", "freight_value",
        "total_item_value", "freight_ratio",

        # Payment metrics
        "total_payment_value", "max_installments",
        "payment_types_used",  "payment_count",

        # Customer location
        "customer_city", "customer_state",
        "customer_zip_code_prefix",

        # Product details
        "product_category",
        "product_weight_g",
        "product_photos_qty",

        # Seller location
        "seller_city", "seller_state",

        # Review metrics
        "review_score",
        "review_comment_message",
    )

    return df_final
