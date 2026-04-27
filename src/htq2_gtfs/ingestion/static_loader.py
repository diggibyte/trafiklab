"""Entry point for the HTQ2 Static GTFS Loader (manual job).

Fully self-contained: downloads static GTFS ZIPs from Trafiklab API,
saves to Volume, extracts CSVs, loads to 6 bronze Delta tables,
then cleans up.

v3.2: Added API download step — no longer requires manual ZIP upload.
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
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from htq2_gtfs.config import (
    SECRET_KEY_STATIC,
    SECRET_SCOPE,
    parse_static_loader_args,
)
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.ingestion.trafiklab_client import TrafiklabClient

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
    """Find the latest ZIP file by date in filename."""
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


def _download_static_zips(
    static_path: str,
    region: str = "skane",
) -> dict[str, str]:
    """Download static GTFS ZIPs from Trafiklab API to the Volume.

    Uses Databricks Secrets for the API key. Downloads both the main
    static ZIP and the extra ZIP (DVJId mapping).

    Returns:
        Dict mapping filename to full path of downloaded files.
    """
    spark = SparkSession.builder.getOrCreate()

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        static_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY_STATIC)
    except Exception as e:
        raise RuntimeError(
            f"Cannot get static API key from secrets scope '{SECRET_SCOPE}': {e}"
        ) from e

    os.makedirs(static_path, exist_ok=True)
    today = date.today().isoformat()

    client = TrafiklabClient(
        realtime_api_key="",  # Not needed for static
        static_api_key=static_key,
        region=region,
    )

    downloaded: dict[str, str] = {}

    try:
        static_data = client.fetch_all_static()

        for api_filename, content in static_data.items():
            # Rename with date: skane.zip -> skane_static_2026-04-27.zip
            if "_extra" in api_filename or api_filename.endswith("_extra.zip"):
                target_name = f"{region}_extra_{today}.zip"
            else:
                target_name = f"{region}_static_{today}.zip"

            target_path = os.path.join(static_path, target_name)

            with open(target_path, "wb") as f:
                f.write(content)

            downloaded[target_name] = target_path
            logger.info(
                f"Downloaded {target_name} ({len(content):,} bytes)"
            )
    finally:
        client.close()

    if not downloaded:
        raise RuntimeError(
            "No static files downloaded from Trafiklab API. "
            "Check API key and network connectivity."
        )

    return downloaded


def _extract_and_read(
    spark: SparkSession,
    zip_path: str,
    csv_name: str,
    extract_dir: str,
) -> "pyspark.sql.DataFrame | None":
    """Extract a CSV from a ZIP into the Volume and read as Spark DataFrame."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        if csv_name not in zf.namelist():
            logger.warning(
                f"{csv_name} not found in {os.path.basename(zip_path)}"
            )
            return None
        zf.extract(csv_name, extract_dir)

    csv_path = os.path.join(extract_dir, csv_name)
    logger.info(f"  Extracted {csv_name} to {csv_path}")

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
    """Entry point for Databricks python_wheel_task.

    Full flow: Download ZIPs from API -> Extract CSVs -> Load to Delta -> Cleanup
    """
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

    # Temp extraction directory INSIDE the Volume
    extract_dir = os.path.join(config.static_path, "_tmp_extract")

    try:
        # ── Step 1: Download ZIPs from Trafiklab API ──
        logger.info("Step 1: Downloading static GTFS ZIPs from Trafiklab API...")
        _download_static_zips(config.static_path, region="skane")

        # ── Step 2: Find latest ZIP files ──
        static_zip = _find_latest_zip(config.static_path, "skane_static_")
        extra_zip = _find_latest_zip(config.static_path, "skane_extra_")

        if not static_zip:
            raise FileNotFoundError(
                f"No skane_static_*.zip found in {config.static_path} "
                f"(download may have failed)"
            )
        if not extra_zip:
            raise FileNotFoundError(
                f"No skane_extra_*.zip found in {config.static_path} "
                f"(download may have failed)"
            )

        logger.info(f"Static ZIP: {os.path.basename(static_zip)}")
        logger.info(f"Extra ZIP:  {os.path.basename(extra_zip)}")

        # Track ZIPs for cleanup after successful load
        files_to_cleanup.append(static_zip)
        files_to_cleanup.append(extra_zip)

        # Create extraction directory in the Volume
        os.makedirs(extract_dir, exist_ok=True)
        files_to_cleanup.append(extract_dir)

        # ── Step 3: Extract and load each table ──
        logger.info("Step 3: Extracting and loading tables...")
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

    # ── Step 4: Cleanup ──
    if status == "success" and files_to_cleanup:
        logger.info("Step 4: Cleaning up extracted files and source ZIPs...")
        _cleanup_files(files_to_cleanup)
    elif status == "failed":
        if os.path.isdir(extract_dir):
            _cleanup_files([extract_dir])
        logger.info("Kept source ZIPs (load failed -- may need retry)")

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
