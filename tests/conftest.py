"""Shared pytest fixtures for HTQ2 GTFS pipeline tests."""

import os

import pytest
from pyspark.sql import SparkSession

from htq2_gtfs.config import IngestConfig, QualityConfig, TransformConfig


def _is_databricks() -> bool:
    """Detect if running on Databricks (Spark Connect or classic)."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Shared SparkSession for all tests.

    On Databricks: getOrCreate() without master (uses existing Spark Connect).
    Locally (CI): create local[2] session.
    """
    if _is_databricks():
        return SparkSession.builder.getOrCreate()
    else:
        return (
            SparkSession.builder
            .master("local[2]")
            .appName("htq2-gtfs-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .getOrCreate()
        )


@pytest.fixture
def ingest_config(tmp_path) -> IngestConfig:
    """Test ingestion config with temp paths."""
    return IngestConfig(
        catalog="test_catalog",
        schema="bronze",
        volume="test_volume",
        run_mode="realtime",
        region="skane",
    )


@pytest.fixture
def transform_config(tmp_path) -> TransformConfig:
    """Test transform config."""
    return TransformConfig(
        catalog="test_catalog",
        bronze_schema="bronze",
        silver_schema="silver",
        volume="test_volume",
        region="skane",
    )


@pytest.fixture
def quality_config() -> QualityConfig:
    """Test quality config."""
    return QualityConfig(
        catalog="test_catalog",
        schema="silver",
    )
