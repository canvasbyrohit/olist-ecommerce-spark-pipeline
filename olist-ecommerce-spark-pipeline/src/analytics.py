# ============================================================
# analytics.py
# Spark SQL and DataFrame API analytics functions.
# All functions accept a SparkSession and return a DataFrame.
# Call register_views() first to make Parquet data available
# as Spark SQL temporary views.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config import PATHS, SELLER_MIN_ORDERS


def register_views(spark):
    """
    Reload curated Parquet files and register as Spark SQL views.
    Must be called before any analytics function.
    """
    df_integrated = spark.read.parquet(PATHS["integrated"])
    df_aggregated = spark.read.parquet(PATHS["aggregated"])

    df_integrated.createOrReplaceTempView("olist_integrated")
    df_aggregated.createOrReplaceTempView("olist_aggregated")

    print(f"olist_integrated : {df_integrated.count():,} rows "
          f"| {len(df_integrated.columns)} columns")
    print(f"olist_aggregated : {df_aggregated.count():,} rows "
          f"| {len(df_aggregated.columns)} columns")
    return df_integrated, df_aggregated


def bq1_category_revenue(spark):
    """
    BQ1: Top 10 product categories by total revenue.
    Returns DataFrame with revenue, order count, avg price,
    avg review score, and revenue per order.
    """
    return spark.sql("""
        SELECT
            product_category,
            COUNT(DISTINCT order_id)          AS total_orders,
            COUNT(order_item_id)              AS total_items_sold,
            ROUND(SUM(price), 2)              AS total_revenue_brl,
            ROUND(AVG(price), 2)              AS avg_item_price_brl,
            ROUND(SUM(freight_value), 2)      AS total_freight_brl,
            ROUND(AVG(review_score), 2)       AS avg_review_score,
            ROUND(SUM(price) /
                COUNT(DISTINCT order_id), 2)  AS revenue_per_order
        FROM olist_integrated
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY total_revenue_brl DESC
        LIMIT 10
    """)


def bq2_delay_vs_review(spark):
    """
    BQ2: Average review score and poor review rate by delivery delay band.
    Delivery delay bucketed into 6 bands from 'very early' to 'very late'.
    Restricted to delivered orders with non-null review scores.
    """
    return spark.sql("""
        SELECT
            CASE
                WHEN delivery_delay_days < -7  THEN '1. Very early (>7d)'
                WHEN delivery_delay_days < 0   THEN '2. Early (1-7d)'
                WHEN delivery_delay_days = 0   THEN '3. On time'
                WHEN delivery_delay_days <= 7  THEN '4. Late (1-7d)'
                WHEN delivery_delay_days <= 14 THEN '5. Late (8-14d)'
                ELSE                                '6. Very late (>14d)'
            END                               AS delay_band,
            COUNT(DISTINCT order_id)          AS order_count,
            ROUND(AVG(review_score), 3)       AS avg_review_score,
            SUM(CASE WHEN review_score <= 2
                THEN 1 ELSE 0 END)            AS poor_reviews,
            ROUND(
                SUM(CASE WHEN review_score <= 2
                    THEN 1 ELSE 0 END) * 100.0
                / COUNT(DISTINCT order_id), 1
            )                                 AS pct_poor_reviews
        FROM olist_integrated
        WHERE order_status          = 'delivered'
          AND review_score          IS NOT NULL
          AND delivery_delay_days   IS NOT NULL
        GROUP BY delay_band
        ORDER BY delay_band
    """)


def bq3_state_revenue(spark):
    """
    BQ3: Revenue, order volume, and delivery performance by customer state.
    Includes revenue share % using SUM OVER() window function.
    """
    return spark.sql("""
        SELECT
            customer_state,
            COUNT(DISTINCT order_id)           AS total_orders,
            COUNT(DISTINCT customer_id)        AS unique_customers,
            ROUND(SUM(price), 2)               AS total_revenue_brl,
            ROUND(AVG(price), 2)               AS avg_order_value_brl,
            ROUND(AVG(review_score), 2)        AS avg_review_score,
            ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay,
            ROUND(
                SUM(price) * 100.0 /
                SUM(SUM(price)) OVER (), 2
            )                                  AS revenue_share_pct
        FROM olist_integrated
        WHERE customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY total_revenue_brl DESC
    """)


def bq4_monthly_trends(spark):
    """
    BQ4: Monthly revenue and order volume trend (2016-2018).
    Adds 3-month rolling average using DataFrame window function
    (Spark SQL cannot apply rolling avg on string-ordered partitions).
    """
    monthly = spark.sql("""
        SELECT
            order_month_label,
            order_year,
            order_month,
            COUNT(DISTINCT order_id)      AS monthly_orders,
            ROUND(SUM(price), 2)          AS monthly_revenue_brl,
            ROUND(AVG(price), 2)          AS avg_order_value_brl,
            ROUND(AVG(review_score), 2)   AS avg_review_score,
            SUM(CASE WHEN is_late
                THEN 1 ELSE 0 END)        AS late_deliveries
        FROM olist_integrated
        WHERE order_year BETWEEN 2016 AND 2018
        GROUP BY order_month_label, order_year, order_month
        ORDER BY order_month_label
    """)

    month_window = (
        Window.orderBy("order_month_label")
              .rowsBetween(-2, 0)
    )

    return monthly \
        .withColumn("rolling_3m_avg_revenue",
            F.round(F.avg("monthly_revenue_brl").over(month_window), 2)) \
        .withColumn("rolling_3m_avg_orders",
            F.round(F.avg("monthly_orders").over(month_window), 0))


def bq5a_payment_types(spark):
    """
    BQ5a: Payment method distribution and average order value.
    Shows top 10 payment type combinations by order count.
    """
    return spark.sql("""
        SELECT
            payment_types_used,
            COUNT(DISTINCT order_id)            AS order_count,
            ROUND(AVG(total_payment_value), 2)  AS avg_payment_value_brl,
            ROUND(SUM(total_payment_value), 2)  AS total_payment_value_brl,
            ROUND(AVG(max_installments), 2)     AS avg_installments,
            ROUND(AVG(review_score), 2)         AS avg_review_score,
            ROUND(
                COUNT(DISTINCT order_id) * 100.0 /
                SUM(COUNT(DISTINCT order_id)) OVER (), 2
            )                                   AS pct_of_orders
        FROM olist_integrated
        WHERE payment_types_used IS NOT NULL
        GROUP BY payment_types_used
        ORDER BY order_count DESC
        LIMIT 10
    """)


def bq5b_installments(spark):
    """
    BQ5b: Average order value by number of payment instalments (1-12).
    Tests whether instalment financing enables higher-value purchases.
    """
    return spark.sql("""
        SELECT
            max_installments                    AS installments,
            COUNT(DISTINCT order_id)            AS order_count,
            ROUND(AVG(total_payment_value), 2)  AS avg_payment_value_brl,
            ROUND(SUM(total_payment_value), 2)  AS total_revenue_brl,
            ROUND(AVG(review_score), 2)         AS avg_review_score
        FROM olist_integrated
        WHERE max_installments IS NOT NULL
          AND max_installments BETWEEN 1 AND 12
        GROUP BY max_installments
        ORDER BY max_installments
    """)


def bq6_underperforming_sellers(spark):
    """
    BQ6: High-volume sellers with low review scores and high late rates.
    Minimum volume threshold: SELLER_MIN_ORDERS orders.
    Ordered by avg_review_score ASC, then total_orders DESC.
    """
    return spark.sql(f"""
        SELECT
            seller_id,
            seller_state,
            seller_city,
            COUNT(DISTINCT order_id)            AS total_orders,
            ROUND(SUM(price), 2)                AS total_revenue_brl,
            ROUND(AVG(price), 2)                AS avg_item_price,
            ROUND(AVG(review_score), 2)         AS avg_review_score,
            ROUND(AVG(delivery_delay_days), 2)  AS avg_delivery_delay,
            SUM(CASE WHEN is_late
                THEN 1 ELSE 0 END)              AS late_deliveries,
            ROUND(
                SUM(CASE WHEN is_late THEN 1 ELSE 0 END) * 100.0
                / COUNT(DISTINCT order_id), 1
            )                                   AS pct_late,
            ROUND(
                SUM(CASE WHEN review_score <= 2
                    THEN 1 ELSE 0 END) * 100.0
                / COUNT(DISTINCT order_id), 1
            )                                   AS pct_poor_reviews
        FROM olist_integrated
        WHERE seller_id IS NOT NULL
        GROUP BY seller_id, seller_state, seller_city
        HAVING COUNT(DISTINCT order_id) >= {SELLER_MIN_ORDERS}
        ORDER BY avg_review_score ASC, total_orders DESC
        LIMIT 20
    """)


def bq7_day_of_week(spark):
    """
    BQ7: Order volume and revenue by day of the week.
    Ordered Monday to Sunday using explicit CASE WHEN sort key.
    """
    return spark.sql("""
        SELECT
            order_day_of_week,
            COUNT(DISTINCT order_id)      AS total_orders,
            ROUND(SUM(price), 2)          AS total_revenue_brl,
            ROUND(AVG(price), 2)          AS avg_order_value_brl,
            ROUND(AVG(review_score), 2)   AS avg_review_score,
            CASE order_day_of_week
                WHEN 'Monday'    THEN 1
                WHEN 'Tuesday'   THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday'  THEN 4
                WHEN 'Friday'    THEN 5
                WHEN 'Saturday'  THEN 6
                WHEN 'Sunday'    THEN 7
            END                           AS day_order
        FROM olist_integrated
        GROUP BY order_day_of_week
        ORDER BY day_order
    """)


def bq8_freight_by_state(spark):
    """
    BQ8: Average freight cost and delivery performance by customer state.
    Restricted to states with at least 100 orders for statistical validity.
    """
    return spark.sql("""
        SELECT
            customer_state,
            COUNT(DISTINCT order_id)          AS total_orders,
            ROUND(AVG(freight_value), 2)      AS avg_freight_brl,
            ROUND(AVG(price), 2)              AS avg_item_price_brl,
            ROUND(AVG(freight_ratio), 4)      AS avg_freight_ratio,
            ROUND(AVG(review_score), 2)       AS avg_review_score,
            ROUND(AVG(delivery_delay_days),2) AS avg_delay_days
        FROM olist_integrated
        WHERE customer_state IS NOT NULL
        GROUP BY customer_state
        HAVING COUNT(DISTINCT order_id) >= 100
        ORDER BY avg_freight_brl DESC
    """)


def run_all_analytics(spark):
    """
    Run all 8 analytics queries and return results as a
    dictionary of {query_name: DataFrame}.
    Registers Parquet views before running.
    """
    register_views(spark)

    results = {
        "bq1_category_revenue"   : bq1_category_revenue(spark),
        "bq2_delay_vs_review"    : bq2_delay_vs_review(spark),
        "bq3_state_revenue"      : bq3_state_revenue(spark),
        "bq4_monthly_trends"     : bq4_monthly_trends(spark),
        "bq5a_payment_types"     : bq5a_payment_types(spark),
        "bq5b_installments"      : bq5b_installments(spark),
        "bq6_underperforming"    : bq6_underperforming_sellers(spark),
        "bq7_day_of_week"        : bq7_day_of_week(spark),
        "bq8_freight_by_state"   : bq8_freight_by_state(spark),
    }

    for name, df in results.items():
        print(f"\n=== {name.upper()} ===")
        df.show(10, truncate=40)

    return results
