"""Entry point for the HTQ2 GTFS Prep task (parse_and_enrich).

Layer 2 in the 8-task architecture. Responsibilities:
1. Parse protobuf .pb files from Volume
2. Enrich from static GTFS lookups (trips, routes, stops, DVJId)
3. Inline validation (fail-fast before Silver)
4. MERGE to durable staging tables: bronze.parsed_journey, bronze.parsed_call

Usage (via python_wheel_task):
    htq2_gtfs prep --catalog htq2_dev
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from htq2_gtfs.config import parse_prep_args
from htq2_gtfs.helpers.file_utils import (
    check_realtime_completeness,
    list_realtime_files,
)
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.core import GTFSProcessor, JOURNEY_SCHEMA, CALL_SCHEMA, VP_SCHEMA
from htq2_gtfs.processing.watermark import get_processed_files, write_run_metadata
from htq2_gtfs.prep.validator import validate_prep_output

logger = get_logger(__name__)


def _ensure_staging_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create staging tables if they don't exist."""
    # parsed_journey
    j_fields = list(JOURNEY_SCHEMA.fields) + [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_parsed_timestamp", T.TimestampType(), False),
    ]
    j_schema = T.StructType(j_fields)
    fq_journey = f"{catalog}.{schema}.parsed_journey"
    if not spark.catalog.tableExists(fq_journey):
        spark.createDataFrame([], j_schema).write.format("delta").saveAsTable(fq_journey)
        logger.info(f"Created staging table {fq_journey}")

    # parsed_call
    c_fields = list(CALL_SCHEMA.fields) + [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_parsed_timestamp", T.TimestampType(), False),
    ]
    c_schema = T.StructType(c_fields)
    fq_call = f"{catalog}.{schema}.parsed_call"
    if not spark.catalog.tableExists(fq_call):
        spark.createDataFrame([], c_schema).write.format("delta").saveAsTable(fq_call)
        logger.info(f"Created staging table {fq_call}")


def _merge_to_staging(
    spark: SparkSession,
    df,
    catalog: str,
    schema: str,
    table_name: str,
    merge_keys: list[str],
) -> int:
    """MERGE DataFrame into a staging table by composite key.

    Idempotent: re-running with the same _run_id produces identical results.
    """
    from delta.tables import DeltaTable

    fq_table = f"{catalog}.{schema}.{table_name}"
    row_count = df.count()

    if row_count == 0:
        logger.warning(f"0 rows for {table_name} — skipping MERGE")
        return 0

    merge_condition = " AND ".join(
        [f"target.{k} = source.{k}" for k in merge_keys]
    )

    target_table = DeltaTable.forName(spark, fq_table)
    (
        target_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"Merged {row_count:,} rows into {fq_table}")
    return row_count


def _print_summary(
    run_id: str, status: str, duration: float,
    j_count: int, c_count: int, source_files: list[str],
    error_msg: str | None = None,
) -> None:
    """Print summary to stdout (visible in driver logs)."""
    print("\n" + "=" * 70)
    print("  HTQ2 PREP (parse_and_enrich) RESULTS")
    print("=" * 70)
    print(f"  Run ID:        {run_id}")
    print(f"  Status:        {status.upper()}")
    print(f"  Duration:      {duration:.1f}s")
    print(f"  Files:         {len(source_files)}")
    print(f"  Journeys:      {j_count:,}")
    print(f"  Calls:         {c_count:,}")
    if error_msg:
        print(f"  Error:         {error_msg}")
    print("=" * 70)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_prep_args()
    run_id = str(uuid.uuid4())[:8]

    logger.info(
        f"HTQ2 Prep starting: run_id={run_id}, catalog={config.catalog}",
        extra={"run_id": run_id},
    )

    spark = SparkSession.builder.getOrCreate()
    today = date.today()
    j_count = 0
    c_count = 0
    source_files: list[str] = []
    status = "success"
    error_msg = None

    try:
        # Step 0: Ensure staging tables exist
        _ensure_staging_tables(spark, config.catalog, config.bronze_schema)

        # Step 1: Get already-processed files (from watermark)
        processed_files = get_processed_files(spark, config)
        logger.info(f"Watermark: {len(processed_files)} files already processed")

        # Step 2: Discover new realtime file sets
        file_sets = list_realtime_files(
            config.realtime_path,
            processed_files=processed_files,
        )

        if not file_sets:
            logger.info("No new files to process — exiting")
            _print_summary(run_id, "success", time.time() - start_time, 0, 0, [])
            return

        if not check_realtime_completeness(file_sets):
            logger.warning("No complete file sets found — exiting")
            _print_summary(run_id, "success", time.time() - start_time, 0, 0, [])
            return

        # Collect source file names
        for fs in file_sets:
            source_files.extend(fs.files.values())

        logger.info(f"Processing {len(file_sets)} file sets ({len(source_files)} files)")

        # Step 3: Initialize processor and load static data
        processor = GTFSProcessor(spark, config)
        processor.load_static_data(today)

        # Step 4: Parse protobuf files into base DataFrames
        journey_df, call_df, vp_df, stops_df = processor.parse_realtime_files(file_sets)

        # Step 5: Add traceability columns
        journey_df = (
            journey_df
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_parsed_timestamp", F.current_timestamp())
        )
        call_df = (
            call_df
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_parsed_timestamp", F.current_timestamp())
        )

        # Step 6: Inline validation (fail-fast)
        validate_prep_output(journey_df, call_df)

        # Step 7: MERGE to staging tables
        j_count = _merge_to_staging(
            spark, journey_df,
            config.catalog, config.bronze_schema,
            "parsed_journey",
            merge_keys=["_run_id", "DVJId"],
        )
        c_count = _merge_to_staging(
            spark, call_df,
            config.catalog, config.bronze_schema,
            "parsed_call",
            merge_keys=["_run_id", "DVJId", "SequenceNumber"],
        )

        # Step 8: Also write VP data for GPS tables (append, not critical)
        if vp_df is not None and not vp_df.isEmpty():
            fq_vp = f"{config.catalog}.{config.bronze_schema}.parsed_vehicle_positions"
            vp_with_meta = (
                vp_df
                .withColumn("_run_id", F.lit(run_id))
                .withColumn("_parsed_timestamp", F.current_timestamp())
            )
            vp_with_meta.write.format("delta").mode("append").option(
                "mergeSchema", "true"
            ).saveAsTable(fq_vp)
            logger.info(f"Wrote {vp_df.count():,} VP rows to {fq_vp}")

    except Exception as e:
        logger.error(f"Prep failed: {e}", exc_info=True)
        status = "failed"
        error_msg = str(e)

    duration = time.time() - start_time

    # Write run metadata
    try:
        write_run_metadata(
            spark=spark,
            config=config,
            run_id=run_id,
            operating_date=today,
            source_files=[str(f) for f in source_files],
            table_row_counts={"parsed_journey": j_count, "parsed_call": c_count},
            duration_seconds=duration,
            status=status,
            error_message=error_msg,
        )
    except Exception as e:
        logger.error(f"Failed to write metadata: {e}")

    _print_summary(run_id, status, duration, j_count, c_count,
                   [str(f) for f in source_files], error_msg)

    exit_output = {
        "status": status,
        "run_id": run_id,
        "duration_seconds": round(duration, 1),
        "files_processed": len(source_files),
        "parsed_journey_rows": j_count,
        "parsed_call_rows": c_count,
    }

    if status == "failed":
        logger.error(f"Exiting with error: {error_msg}")
        print(json.dumps(exit_output, indent=2))
        sys.exit(1)

    print(json.dumps(exit_output, indent=2))
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        dbutils.notebook.exit(json.dumps(exit_output))
    except ImportError:
        pass


if __name__ == "__main__":
    main()
