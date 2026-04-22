"""Entry point for the HTQ2 GTFS Transform task (Silver Build).

Layer 3 in the 8-task architecture. Reads from staging tables
(bronze.parsed_journey, bronze.parsed_call) and builds Silver tables.

Supports --tables parameter:
  base: journey + journey_call + 14 extension tables (16 total)
  gps:  6 GPS tables (journey_link_*, stop_point_*)
  all:  all 22 tables

Usage (via python_wheel_task):
    htq2_gtfs transform --catalog htq2_dev --tables base
    htq2_gtfs transform --catalog htq2_dev --tables gps
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import date

from pyspark.sql import SparkSession

from htq2_gtfs.config import parse_transform_args
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.view_builder import ViewBuilder
from htq2_gtfs.processing.writer import (
    ensure_all_table_schemas,
    ensure_table_properties,
    write_table,
)

logger = get_logger(__name__)


def _print_summary(
    run_id: str,
    status: str,
    duration: float,
    result: dict,
    tables_mode: str,
    error_msg: str | None = None,
) -> None:
    """Print a detailed summary to stdout so it appears in the job output."""
    table_counts = result.get("table_row_counts", {})
    total_rows = sum(table_counts.values())

    print("\n" + "=" * 70)
    print(f"  HTQ2 TRANSFORM RESULTS (--tables {tables_mode})")
    print("=" * 70)
    print(f"  Run ID:       {run_id}")
    print(f"  Status:       {status.upper()}")
    print(f"  Duration:     {duration:.1f}s")
    print(f"  Tables:       {len(table_counts)}")
    print(f"  Total rows:   {total_rows:,}")

    if error_msg:
        print(f"  Error:        {error_msg}")

    if table_counts:
        print(f"\n  {'Table':<50s} {'Rows':>10s}")
        print(f"  {'-'*50} {'-'*10}")
        for tname, count in sorted(table_counts.items()):
            marker = " (empty)" if count == 0 else ""
            print(f"  {tname:<50s} {count:>10,}{marker}")

    print("\n" + "=" * 70)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_transform_args()
    run_id = str(uuid.uuid4())[:8]

    logger.info(
        f"HTQ2 Transform starting: run_id={run_id}, catalog={config.catalog}, "
        f"tables={config.tables}",
        extra={"run_id": run_id},
    )

    spark = SparkSession.builder.getOrCreate()

    try:
        result = run_transform(spark, config, run_id)
        status = "success"
        error_msg = None
    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        status = "failed"
        error_msg = str(e)
        result = {"table_row_counts": {}}

    duration = time.time() - start_time

    _print_summary(run_id, status, duration, result, config.tables, error_msg)

    exit_output = {
        "status": status,
        "run_id": run_id,
        "tables_mode": config.tables,
        "duration_seconds": round(duration, 1),
        "tables_written": len(result.get("table_row_counts", {})),
        "total_rows": sum(result.get("table_row_counts", {}).values()),
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


def run_transform(
    spark: SparkSession,
    config,
    run_id: str,
) -> dict:
    """Execute the Silver build pipeline.

    Reads from staging tables (written by prep task) instead of
    parsing raw .pb files. Routes to base or GPS builders based
    on --tables parameter.
    """
    # Step 0: Ensure independent table schemas exist
    ensure_all_table_schemas(spark, config)

    # Step 1: Read from staging tables
    fq_journey = f"{config.catalog}.{config.bronze_schema}.parsed_journey"
    fq_call = f"{config.catalog}.{config.bronze_schema}.parsed_call"

    if not spark.catalog.tableExists(fq_journey):
        raise RuntimeError(
            f"Staging table {fq_journey} does not exist. "
            f"Run parse_and_enrich task first."
        )

    parsed_journey = spark.table(fq_journey)
    parsed_call = spark.table(fq_call)

    # Drop staging metadata columns before building Silver tables
    journey_df = parsed_journey.drop("_run_id", "_parsed_timestamp")
    call_df = parsed_call.drop("_run_id", "_parsed_timestamp")

    if journey_df.isEmpty() and call_df.isEmpty():
        logger.warning("Staging tables are empty — nothing to build")
        return {"table_row_counts": {}}

    # Step 2: Load supporting data (VP, stops, shapes, trips, stop_times)
    vp_df = None
    stops_df = None
    shapes_df = None
    trips_df = None
    stop_times_df = None

    from htq2_gtfs.processing.core import GTFSProcessor
    processor = GTFSProcessor(spark, config)

    # Load VP data for observed path and GPS tables
    if config.tables in ("base", "gps", "all"):
        fq_vp = f"{config.catalog}.{config.bronze_schema}.parsed_vehicle_positions"
        if spark.catalog.tableExists(fq_vp):
            vp_df = spark.table(fq_vp).drop("_run_id", "_parsed_timestamp")
            logger.info(f"Loaded VP data from {fq_vp}")

    # Build stops_df from static data for GPS tables
    if config.tables in ("gps", "all"):
        processor.load_static_data(date.today())
        stops_df = processor._build_stops_df()

    # Build static DataFrames for planned path (shapes, trips, stop_times)
    if config.tables in ("base", "all"):
        shapes_df, trips_df, stop_times_df = processor.build_static_spark_dfs(
            date.today()
        )
        logger.info("Loaded static DataFrames for planned path")

    # Step 3: Build tables based on --tables parameter
    builder = ViewBuilder(spark)

    if config.tables == "base":
        tables = builder.build_base_tables(
            journey_df, call_df, vp_df, shapes_df, trips_df, stop_times_df,
        )
    elif config.tables == "gps":
        tables = builder.build_gps_tables(call_df, vp_df, stops_df)
    else:  # "all"
        tables = builder.build_all(
            journey_df, call_df, vp_df, stops_df,
            shapes_df, trips_df, stop_times_df,
        )

    # Step 4: Write tables to Delta Lake (MERGE by PK)
    table_row_counts: dict[str, int] = {}
    for table_name, table_df in tables.items():
        if table_df is not None:
            count = write_table(
                df=table_df,
                config=config,
                table_name=table_name,
            )
            table_row_counts[table_name] = count

    # Step 5: Ensure table properties
    ensure_table_properties(spark, config)

    logger.info(
        f"Transform complete: {sum(table_row_counts.values()):,} total rows "
        f"across {len(table_row_counts)} tables (mode={config.tables})",
        extra={"run_id": run_id},
    )

    return {"table_row_counts": table_row_counts}


if __name__ == "__main__":
    main()
