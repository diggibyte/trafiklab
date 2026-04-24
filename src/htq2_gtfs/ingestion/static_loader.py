"""Entry point for the HTQ2 Static GTFS Loader (manual job).

Separate workflow — triggered manually when a new static GTFS ZIP
is uploaded to the Volume. Not part of the 15-min scheduled pipeline.

Reads skane_static_*.zip and skane_extra_*.zip from the Volume,
extracts CSV files INTO the Volume (not local /tmp — avoids
'cannot access shared' on job clusters), writes them to 6 bronze
Delta tables with OVERWRITE mode, then cleans up ALL files:
both extracted CSVs and source ZIPs.

v3.1: Fixed extraction path to use Volume instead of tempfile.

Usage (via python_wheel_task):
    htq2_gtfs load_static --catalog htq2_dev
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
import zipfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from htq2_gtfs.config import parse_static_loader_args
from htq2_gtfs.helpers.logging_config import get_logger

logger = get_logger(__name__)

# Mapping: bronze table name -> (zip_prefix, csv_filename_inside_zip)
STATIC_TABLE_MAP = {
    "static_routes":      ("skane_static_", "routes.txt"),
    "static_trips":       ("skane_static_", "trips.txt"),
    "static_stops":       ("skane_static_", "stops.txt"),
    "static_stop_times":  ("skane_static_", "stop_times.txt"),
    "static_shapes":      ("skane_static_", "shapes.txt"),
    "static_dvj_mapping": ("skane_extra_",  "trips_dated_vehicle_journey.txt"),
}


def _find_latest_zip(volume_path: str, prefix: str) -> str | None:
    """Find the latest ZIP file by date in filename.

    Filenames follow pattern: {prefix}YYYY-MM-DD.zip
    Returns full path to the latest file, or None.
    """
    try:
        candidates = sorted(
            [
                f
                for f in os.listdir(volume_path)
                if f.startswith(prefix) and f.endswith(".zip")
            ]
        )
        if not candidates:
            return None
        return os.path.join(volume_path, candidates[-1])
    except FileNotFoundError:
        return None


def _extract_and_read(
    spark: SparkSession,
    zip_path: str,
    csv_name: str,
    extract_dir: str,
) -> "pyspark.sql.DataFrame | None":
    """Extract a CSV from a ZIP into the Volume and read as Spark DataFrame.

    Extracts to a _tmp subdirectory inside the Volume (accessible by
    all Spark workers). Caller is responsible for cleaning up extract_dir.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        if csv_name not in zf.namelist():
            logger.warning(
                f"{csv_name} not found in {os.path.basename(zip_path)}"
            )
            return None
        zf.extract(csv_name, extract_dir)

    csv_path = os.path.join(extract_dir, csv_name)
    logger.info(f"  Extracted {csv_name} to {csv_path}")

    # Read from Volume path — accessible by all workers (no file: prefix)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "false")
        .csv(csv_path)
    )
    return df


def _cleanup_files(paths: list[str]) -> None:
    """Delete files and directories. Logs but does not raise on failure."""
    for path in paths:
        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
                logger.info(f"  Deleted directory: {path}")
            elif os.path.isfile(path):
                os.remove(path)
                logger.info(f"  Deleted file: {path}")
        except Exception as e:
            logger.warning(f"  Could not delete {path}: {e}")


def _print_summary(
    status: str,
    duration: float,
    results: dict[str, int],
    error_msg: str | None = None,
) -> None:
    """Print summary to stdout."""
    print("\n" + "=" * 70)
    print("  HTQ2 STATIC GTFS LOADER RESULTS")
    print("=" * 70)
    print(f"  Status:   {status.upper()}")
    print(f"  Duration: {duration:.1f}s")
    if error_msg:
        print(f"  Error:    {error_msg}")
    print()
    for tbl, cnt in sorted(results.items()):
        marker = " (SKIPPED)" if cnt == 0 else ""
        print(f"  {tbl:<35s} {cnt:>12,} rows{marker}")
    print(f"\n  Total: {sum(results.values()):,} rows across {len(results)} tables")
    print("=" * 70)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_static_loader_args()

    logger.info(
        f"HTQ2 Static Loader starting: catalog={config.catalog}, "
        f"schema={config.schema}"
    )

    spark = SparkSession.builder.getOrCreate()
    results: dict[str, int] = {}
    status = "success"
    error_msg = None
    files_to_cleanup: list[str] = []

    # Temp extraction directory INSIDE the Volume (accessible by all workers)
    extract_dir = os.path.join(config.static_path, "_tmp_extract")

    try:
        # Find latest ZIP files
        static_zip = _find_latest_zip(config.static_path, "skane_static_")
        extra_zip = _find_latest_zip(config.static_path, "skane_extra_")

        if not static_zip:
            raise FileNotFoundError(
                f"No skane_static_*.zip found in {config.static_path}"
            )
        if not extra_zip:
            raise FileNotFoundError(
                f"No skane_extra_*.zip found in {config.static_path}"
            )

        logger.info(f"Static ZIP: {os.path.basename(static_zip)}")
        logger.info(f"Extra ZIP:  {os.path.basename(extra_zip)}")

        # Track ZIPs for cleanup after successful load
        files_to_cleanup.append(static_zip)
        files_to_cleanup.append(extra_zip)

        # Create extraction directory in the Volume
        os.makedirs(extract_dir, exist_ok=True)
        files_to_cleanup.append(extract_dir)

        # Load each table
        for table_name, (zip_prefix, csv_name) in STATIC_TABLE_MAP.items():
            zip_path = static_zip if zip_prefix == "skane_static_" else extra_zip
            fq_table = f"{config.catalog}.{config.schema}.{table_name}"

            logger.info(f"Loading {csv_name} -> {fq_table}")

            df = _extract_and_read(spark, zip_path, csv_name, extract_dir)
            if df is None:
                results[table_name] = 0
                continue

            # Add metadata
            df = df.withColumn(
                "_source_zip", F.lit(os.path.basename(zip_path))
            )
            df = df.withColumn("_loaded_timestamp", F.current_timestamp())

            row_count = df.count()

            # OVERWRITE — each load is a complete static snapshot
            (
                df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(fq_table)
            )

            results[table_name] = row_count
            logger.info(f"  Loaded {row_count:,} rows to {fq_table}")

    except Exception as e:
        logger.error(f"Static loader failed: {e}", exc_info=True)
        status = "failed"
        error_msg = str(e)

    duration = time.time() - start_time

    # Cleanup: delete extracted CSVs AND source ZIPs
    if status == "success" and files_to_cleanup:
        logger.info("Cleaning up extracted files and source ZIPs...")
        _cleanup_files(files_to_cleanup)
    elif status == "failed":
        # Still clean up the extraction dir on failure, but keep ZIPs
        if os.path.isdir(extract_dir):
            _cleanup_files([extract_dir])
        logger.info("Kept source ZIPs (load failed — may need retry)")

    _print_summary(status, duration, results, error_msg)

    exit_output = {
        "status": status,
        "duration_seconds": round(duration, 1),
        "tables_loaded": len([v for v in results.values() if v > 0]),
        "total_rows": sum(results.values()),
    }

    if status == "failed":
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
