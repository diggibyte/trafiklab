"""Pipeline watermark management via _pipeline_metadata table.

Replaces the Synapse SourceFiles column-scanning approach (O(n))
with a dedicated metadata table (O(1) lookup).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from htq2_gtfs.config import METADATA_TABLE, TransformConfig
from htq2_gtfs.helpers.logging_config import get_logger

logger = get_logger(__name__)

# Schema for _pipeline_metadata table
METADATA_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("run_timestamp", T.TimestampType(), False),
    T.StructField("operating_date", T.DateType(), False),
    T.StructField("source_files", T.ArrayType(T.StringType()), False),
    T.StructField("file_count", T.IntegerType(), False),
    T.StructField("table_row_counts", T.MapType(T.StringType(), T.LongType()), True),
    T.StructField("duration_seconds", T.DoubleType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("error_message", T.StringType(), True),
])


def get_processed_files(
    spark: SparkSession,
    config: TransformConfig,
) -> set[str]:
    """Get the set of already-processed filenames from metadata table.

    Returns an empty set if the table doesn't exist yet (first run).
    This is O(1) — reads a small metadata table, not scanning all data rows.

    NOTE: Only returns successful runs. Failed runs are excluded so their
    files can be reprocessed on retry.

    IMPORTANT: Returns basenames (just the filename, not the full path)
    to match what list_realtime_files() uses for comparison.
    """
    table_name = config.silver_table(METADATA_TABLE)

    try:
        if not spark.catalog.tableExists(table_name):
            logger.info(f"Metadata table {table_name} does not exist — first run")
            return set()

        df = (
            spark.table(table_name)
            .filter(F.col("status") == "success")
            .select(F.explode("source_files").alias("file"))
            .distinct()
        )

        # Extract basenames — watermark stores full paths but
        # list_realtime_files compares against filenames only
        files = {os.path.basename(row["file"]) for row in df.collect()}
        logger.info(f"Found {len(files)} previously processed files")
        return files

    except Exception as e:
        logger.warning(f"Could not read metadata table: {e}. Treating as first run.")
        return set()


def write_run_metadata(
    spark: SparkSession,
    config: TransformConfig,
    run_id: str,
    operating_date,
    source_files: list[str],
    table_row_counts: dict[str, int],
    duration_seconds: float,
    status: str = "success",
    error_message: Optional[str] = None,
) -> None:
    """Append a new row to the _pipeline_metadata table."""
    table_name = config.silver_table(METADATA_TABLE)

    row_data = [(
        run_id,
        datetime.now(timezone.utc),
        operating_date,
        source_files,
        len(source_files),
        table_row_counts,
        duration_seconds,
        status,
        error_message,
    )]

    df = spark.createDataFrame(row_data, schema=METADATA_SCHEMA)

    df.write.format("delta").mode("append").saveAsTable(table_name)

    logger.info(
        f"Wrote metadata for run {run_id}: {len(source_files)} files, "
        f"status={status}",
        extra={"run_id": run_id, "status": status},
    )
