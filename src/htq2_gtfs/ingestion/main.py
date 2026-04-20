"""Entry point for the HTQ2 GTFS Ingestion task.

Fetches GTFS Realtime and/or Static data from Trafiklab.se
and writes raw files to a Unity Catalog Volume.

Usage (via python_wheel_task):
    htq2_gtfs ingest --catalog htq2_prod --run_mode realtime
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from typing import Any

from htq2_gtfs.config import (
    SECRET_KEY_REALTIME,
    SECRET_KEY_STATIC,
    SECRET_SCOPE,
    parse_ingest_args,
)
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.ingestion.static_manager import download_static_if_needed
from htq2_gtfs.ingestion.trafiklab_client import TrafiklabClient

logger = get_logger(__name__)


def _print_summary(result: dict) -> None:
    """Print a detailed summary to stdout so it appears in the job output."""
    print("\n" + "=" * 70)
    print("  HTQ2 INGESTION RESULTS")
    print("=" * 70)
    print(f"  Status:         {result['status'].upper()}")
    print(f"  Target date:    {result['target_date']}")
    print(f"  Duration:       {result.get('duration_seconds', 0)}s")
    print(f"  Realtime files: {result['realtime_files']}")
    print(f"  Static files:   {result['static_files']}")
    if result.get('errors'):
        print(f"  Errors:         {result['errors']}")
    print("=" * 70)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_ingest_args()

    logger.info(
        f"HTQ2 Ingestion starting: catalog={config.catalog}, "
        f"mode={config.run_mode}, region={config.region}"
    )

    # Get API keys from Databricks Secrets
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        # Fallback for local testing
        dbutils = None
        logger.warning("DBUtils not available — running in local mode")

    if dbutils:
        realtime_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY_REALTIME)
        static_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY_STATIC)
    else:
        # Local testing fallback
        realtime_key = os.environ.get("TRAFIKLAB_REALTIME_KEY", "")
        static_key = os.environ.get("TRAFIKLAB_STATIC_KEY", "")

    target_date = config.target_date or date.today()
    result = run_ingestion(config, realtime_key, static_key, target_date)

    duration = time.time() - start_time
    result["duration_seconds"] = round(duration, 1)

    # Always print detailed summary to stdout (visible in driver logs)
    _print_summary(result)

    logger.info(
        f"Ingestion complete in {duration:.1f}s",
        extra={"duration_seconds": duration, "status": result["status"]},
    )

    # IMPORTANT: On complete failure, sys.exit(1) BEFORE dbutils.notebook.exit()
    if result["status"] == "failed":
        logger.error(f"Ingestion FAILED: {result['errors']}")
        print(json.dumps(result, indent=2))
        import sys
        sys.exit(1)

    # Only reach here on success or partial_failure
    print(json.dumps(result, indent=2))
    if dbutils:
        dbutils.notebook.exit(json.dumps(result))


def run_ingestion(
    config,
    realtime_api_key: str,
    static_api_key: str,
    target_date: date,
) -> dict[str, Any]:
    """Execute the full ingestion workflow.

    Args:
        config: IngestConfig from CLI args.
        realtime_api_key: Trafiklab Realtime API key.
        static_api_key: Trafiklab Static API key.
        target_date: Date to process.

    Returns:
        Summary dict with status, file counts, etc.
    """
    result: dict[str, Any] = {
        "status": "success",
        "target_date": target_date.isoformat(),
        "realtime_files": 0,
        "static_files": 0,
        "errors": [],
    }

    with TrafiklabClient(realtime_api_key, static_api_key, config.region) as client:
        # Step 1: Static files (conditional — once per day)
        if config.run_mode in ("static", "both"):
            try:
                static_result = download_static_if_needed(client, config, target_date)
                result["static_files"] = len(static_result)
            except Exception as e:
                logger.error(f"Static download failed: {e}", exc_info=True)
                result["errors"].append(f"static: {e}")

        # Step 2: Realtime feeds (every run)
        if config.run_mode in ("realtime", "both"):
            try:
                realtime_result = _ingest_realtime(client, config, target_date)
                result["realtime_files"] = realtime_result
            except Exception as e:
                logger.error(f"Realtime download failed: {e}", exc_info=True)
                result["errors"].append(f"realtime: {e}")

    if result["errors"]:
        result["status"] = "partial_failure" if result["realtime_files"] > 0 else "failed"

    return result


def _ingest_realtime(
    client: TrafiklabClient,
    config,
    target_date: date,
) -> int:
    """Download and write realtime feeds to the Volume."""
    realtime_path = config.realtime_path
    os.makedirs(realtime_path, exist_ok=True)

    # Generate timestamp for filenames
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y-%m-%dT%H-%M-%SZ")

    feeds = client.fetch_all_realtime()
    saved_count = 0

    for feed_name, content in feeds.items():
        # Filename: 2025-04-16T08-30-00Z-skane-TripUpdates.pb
        filename = f"{ts}-{config.region}-{feed_name}"
        filepath = os.path.join(realtime_path, filename)

        with open(filepath, "wb") as f:
            f.write(content)

        saved_count += 1
        logger.info(
            f"Saved realtime feed: {filename} ({len(content):,} bytes)",
            extra={"file_count": saved_count},
        )

    logger.info(
        f"Realtime ingestion: {saved_count}/{len(feeds)} feeds saved to {realtime_path}",
        extra={"file_count": saved_count, "task": "ingest_realtime"},
    )

    return saved_count


if __name__ == "__main__":
    main()
