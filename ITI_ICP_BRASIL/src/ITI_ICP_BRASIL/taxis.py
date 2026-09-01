from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def _get_active_spark() -> SparkSession:
    """Retrieve an active SparkSession instance."""
    session = SparkSession.getActiveSession()
    if session is not None:
        return session
    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()
    except Exception:  # noqa: BLE001
        return SparkSession.builder.getOrCreate()


def find_all_taxis(spark: SparkSession | None = None) -> DataFrame:
    """Find all taxi data.

    Args:
        spark: Optional SparkSession instance. If not provided, resolves active session.
    """
    active_spark = spark or _get_active_spark()
    return active_spark.read.table("samples.nyctaxi.trips")
