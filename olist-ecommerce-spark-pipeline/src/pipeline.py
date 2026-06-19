# ============================================================
# pipeline.py
# ETL pipeline orchestrator for the Olist e-commerce project.
# Entry point: run_pipeline(spark)
# Encapsulates Extract -> Transform -> Load in one function.
# ============================================================

import os
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config    import (
    FILE_PATHS, PATHS, ALL_SCHEMAS,
    SPARK_APP_NAME, SPARK_SHUFFLE_PARTITIONS,
    SPARK_DRIVER_MEMORY, PARQUET_COMPRESSION, PARQUET_MODE,
    PROCESSED_PATH
)
from src.schemas   import ALL_SCHEMAS
from src.ingest    import load_all_tables
from src.clean     import clean_all_tables
from src.integrate import integrate_tables


def get_spark_session():
    """Initialise and return a tuned SparkSession."""
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def log(stage, msg):
    """Structured pipeline logger with timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{stage.upper():<12}] {msg}")


def run_pipeline(spark=None):
    """
    Execute the full Olist ETL pipeline.

    Stages
    ------
    EXTRACT   : load 9 raw CSVs into Spark DataFrames
    TRANSFORM : clean all tables, then integrate into one dataset
    LOAD      : write integrated + aggregated Parquet outputs

    Parameters
    ----------
    spark : SparkSession, optional
        Provide an existing session or None to create one.

    Returns
    -------
    df_final : DataFrame — integrated analytical dataset
    metrics  : dict      — timing and row count metadata
    """
    if spark is None:
        spark = get_spark_session()

    os.makedirs(PROCESSED_PATH, exist_ok=True)

    metrics    = {}
    t_pipeline = time.time()

    # ── EXTRACT ────────────────────────────────────────────
    log("EXTRACT", "Loading 9 raw CSV files...")
    t0 = time.time()

    raw = load_all_tables(spark, FILE_PATHS, ALL_SCHEMAS)

    metrics["raw_rows"]      = sum(df.count() for df in raw.values())
    metrics["extract_secs"]  = round(time.time() - t0, 2)
    log("EXTRACT", f"Done — {metrics['raw_rows']:,} total rows "
                   f"in {metrics['extract_secs']}s")

    # ── TRANSFORM: CLEAN ───────────────────────────────────
    log("TRANSFORM", "Cleaning all tables...")
    t0 = time.time()

    clean = clean_all_tables(raw)

    metrics["clean_secs"] = round(time.time() - t0, 2)
    log("TRANSFORM", f"Cleaning done in {metrics['clean_secs']}s")

    # ── TRANSFORM: INTEGRATE ───────────────────────────────
    log("TRANSFORM", "Integrating tables (star-schema join)...")
    t0 = time.time()

    df_final = integrate_tables(clean)
    df_final.cache()   # cache before multiple actions below

    metrics["integrated_rows"]    = df_final.count()
    metrics["integrated_columns"] = len(df_final.columns)
    metrics["integrate_secs"]     = round(time.time() - t0, 2)
    log("TRANSFORM", f"Integration done — "
                     f"{metrics['integrated_rows']:,} rows, "
                     f"{metrics['integrated_columns']} columns "
                     f"in {metrics['integrate_secs']}s")

    # ── LOAD: INTEGRATED PARQUET ───────────────────────────
    log("LOAD", "Writing olist_integrated.parquet...")
    t0 = time.time()

    df_final.write \
        .mode(PARQUET_MODE) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(PATHS["integrated"])

    metrics["write_integrated_secs"] = round(time.time() - t0, 2)
    log("LOAD", f"Integrated Parquet written in "
                f"{metrics['write_integrated_secs']}s")

    # ── LOAD: AGGREGATED PARQUET ───────────────────────────
    log("LOAD", "Writing olist_aggregated.parquet...")
    t0 = time.time()

    df_agg = df_final.groupBy(
        "order_month_label", "customer_state",
        "product_category",  "order_status"
    ).agg(
        F.count("order_id")
         .alias("order_count"),
        F.round(F.sum("price"), 2)
         .alias("total_revenue"),
        F.round(F.avg("price"), 2)
         .alias("avg_price"),
        F.round(F.avg("review_score"), 2)
         .alias("avg_review_score"),
        F.round(F.avg("delivery_delay_days"), 2)
         .alias("avg_delay_days"),
        F.sum(F.col("is_late").cast("int"))
         .alias("late_order_count"),
        F.round(F.avg("freight_value"), 2)
         .alias("avg_freight"),
        F.round(F.avg("total_payment_value"), 2)
         .alias("avg_payment_value"),
    )

    df_agg.write \
        .mode(PARQUET_MODE) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(PATHS["aggregated"])

    metrics["write_aggregated_secs"] = round(time.time() - t0, 2)
    log("LOAD", f"Aggregated Parquet written in "
                f"{metrics['write_aggregated_secs']}s")

    # ── LOAD: INDIVIDUAL CLEAN TABLES ─────────────────────
    log("LOAD", "Writing individual clean table Parquets...")
    clean["orders"].write.mode(PARQUET_MODE) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(PATHS["orders_clean"])
    clean["order_items"].write.mode(PARQUET_MODE) \
        .option("compression", PARQUET_COMPRESSION) \
        .parquet(PATHS["items_clean"])

    # ── SUMMARY ───────────────────────────────────────────
    metrics["total_secs"] = round(time.time() - t_pipeline, 2)
    log("COMPLETE", f"Pipeline finished in {metrics['total_secs']}s")

    print_metrics(metrics)
    return df_final, metrics


def print_metrics(metrics):
    """Print a formatted pipeline run summary."""
    print("\n" + "=" * 55)
    print("  PIPELINE RUN SUMMARY")
    print("=" * 55)
    print(f"  {'Raw rows ingested':<32} {metrics['raw_rows']:>10,}")
    print(f"  {'Integrated rows output':<32} "
          f"{metrics['integrated_rows']:>10,}")
    print(f"  {'Integrated columns':<32} "
          f"{metrics['integrated_columns']:>10}")
    print(f"  {'Extract time':<32} {metrics['extract_secs']:>9}s")
    print(f"  {'Clean time':<32} {metrics['clean_secs']:>9}s")
    print(f"  {'Integrate time':<32} {metrics['integrate_secs']:>9}s")
    print(f"  {'Write integrated':<32} "
          f"{metrics['write_integrated_secs']:>9}s")
    print(f"  {'Write aggregated':<32} "
          f"{metrics['write_aggregated_secs']:>9}s")
    print(f"  {'Total pipeline time':<32} {metrics['total_secs']:>9}s")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    spark = get_spark_session()
    df, metrics = run_pipeline(spark)
