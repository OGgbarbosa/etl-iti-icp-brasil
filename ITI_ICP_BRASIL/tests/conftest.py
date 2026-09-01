"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager

try:
    import pytest
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
except ImportError:
    raise ImportError(
        "Test dependencies not found.\n\n"
        "Run tests using 'uv run pytest'. See https://docs.astral.sh/uv to learn more about uv."
    )


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    try:
        conf = WorkspaceClient().config
        if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
            return

        url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
        print("⚠️ No compute specified, falling back to serverless compute.", file=sys.stderr)
        print(f"  See {url} for manual configuration.", file=sys.stdout)

        os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"
    except Exception as exc:  # noqa: BLE001
        print(f"⚠️ Could not resolve Databricks workspace config: {exc}", file=sys.stderr)


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests via Databricks Connect.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    _enable_fallback_compute()

    try:
        if hasattr(DatabricksSession.builder, "validateSession"):
            return DatabricksSession.builder.validateSession().getOrCreate()
        return DatabricksSession.builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"Databricks Connect session could not be established: {exc}\n"
            "Ensure you are authenticated with 'databricks configure' and have compute access."
        )


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:
        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text(encoding="utf-8"))
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader
