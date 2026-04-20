"""Quality results reporter — writes check results to Delta table."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from htq2_gtfs.config import QUALITY_RESULTS_TABLE, QualityConfig
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.quality.checks import CheckResult

logger = get_logger(__name__)

QUALITY_SCHEMA = T.StructType([
    T.StructField("run_timestamp", T.TimestampType(), False),
    T.StructField("check_name", T.StringType(), False),
    T.StructField("check_level", T.IntegerType(), False),
    T.StructField("result", T.StringType(), False),
    T.StructField("metric_value", T.DoubleType(), True),
    T.StructField("threshold", T.DoubleType(), True),
    T.StructField("details", T.StringType(), True),
])


def write_quality_results(
    spark: SparkSession,
    config: QualityConfig,
    results: list[CheckResult],
) -> None:
    """Write quality check results to the _quality_results table."""
    table_name = config.table(QUALITY_RESULTS_TABLE)
    now = datetime.now(timezone.utc)

    rows = [
        (
            now,
            r.name,
            r.level,
            r.result,
            float(r.metric_value),
            float(r.threshold),
            r.details,
        )
        for r in results
    ]

    df = spark.createDataFrame(rows, schema=QUALITY_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(table_name)

    logger.info(
        f"Wrote {len(results)} quality check results to {table_name}",
        extra={"row_count": len(results)},
    )
