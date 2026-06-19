# ============================================================
# ingest.py
# Data ingestion functions for the Olist ETL pipeline.
# Loads all 9 CSV files into PySpark DataFrames using
# explicit schemas defined in schemas.py.
# ============================================================

from pyspark.sql import SparkSession


def load_csv(spark, path, schema, multiline=False):
    """
    Load a single CSV file into a PySpark DataFrame.

    Parameters
    ----------
    spark     : SparkSession
    path      : str   — full path to the CSV file
    schema    : StructType — explicit schema definition
    multiline : bool  — True for review text with embedded newlines

    Returns
    -------
    DataFrame
    """
    return (
        spark.read
        .option("header",    "true")
        .option("encoding",  "UTF-8")
        .option("multiLine", str(multiline).lower())
        .option("escape",    '"')
        .schema(schema)
        .csv(path)
    )


def load_all_tables(spark, file_paths, schemas):
    """
    Load all 9 Olist CSV files into a dictionary of DataFrames.

    Parameters
    ----------
    spark      : SparkSession
    file_paths : dict — mapping of table name to file path
    schemas    : dict — mapping of table name to StructType

    Returns
    -------
    dict of {table_name: DataFrame}
    """
    raw = {}

    raw["orders"]      = load_csv(spark,
                                   file_paths["orders"],
                                   schemas["orders"])

    raw["order_items"] = load_csv(spark,
                                   file_paths["order_items"],
                                   schemas["order_items"])

    raw["customers"]   = load_csv(spark,
                                   file_paths["customers"],
                                   schemas["customers"])

    raw["products"]    = load_csv(spark,
                                   file_paths["products"],
                                   schemas["products"])

    raw["sellers"]     = load_csv(spark,
                                   file_paths["sellers"],
                                   schemas["sellers"])

    raw["payments"]    = load_csv(spark,
                                   file_paths["payments"],
                                   schemas["payments"])

    # multiLine=True: review comments may contain embedded newlines
    raw["reviews"]     = load_csv(spark,
                                   file_paths["reviews"],
                                   schemas["reviews"],
                                   multiline=True)

    raw["geolocation"] = load_csv(spark,
                                   file_paths["geolocation"],
                                   schemas["geolocation"])

    raw["category_tr"] = load_csv(spark,
                                   file_paths["category_tr"],
                                   schemas["category_tr"])

    return raw


def print_inventory(raw):
    """Print row and column counts for all loaded tables."""
    print(f"\n{'Table':<18} {'Rows':>10} {'Columns':>10}")
    print("-" * 42)
    for name, df in raw.items():
        print(f"{name:<18} {df.count():>10,} {len(df.columns):>10}")
    print()
