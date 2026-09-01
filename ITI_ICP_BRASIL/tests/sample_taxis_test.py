from pyspark.sql import SparkSession

from ITI_ICP_BRASIL import taxis


def test_find_all_taxis(spark: SparkSession) -> None:
    results = taxis.find_all_taxis(spark=spark)
    assert results.count() > 5
