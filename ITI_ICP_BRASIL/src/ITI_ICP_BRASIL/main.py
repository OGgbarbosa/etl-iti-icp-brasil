from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from ITI_ICP_BRASIL import taxis


def main() -> None:
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    # Get active SparkSession
    try:
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
    except Exception:  # noqa: BLE001
        spark = SparkSession.builder.getOrCreate()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    # Example: just find all taxis from a sample catalog
    taxis.find_all_taxis(spark=spark).show(5)


if __name__ == "__main__":
    main()
