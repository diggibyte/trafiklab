"""Entry point for the HTQ2 GTFS Quality Gate task.

Layer 4 in the 8-task architecture. Runs data quality checks and
logs results. Exits with code 1 if any FAIL-level check triggers.

v2.0: Supports --tables parameter to scope checks:
  base: validate base + extension tables only
  gps:  validate GPS tables only
  all:  validate all 22 tables

Usage (via python_wheel_task):
    htq2_gtfs validate --catalog htq2_dev --tables all
"""

from __future__ import annotations

import json
import sys
from datetime import date

from pyspark.sql import SparkSession

from htq2_gtfs.config import parse_quality_args
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.quality.checks import run_all_checks
from htq2_gtfs.quality.reporter import write_quality_results

logger = get_logger(__name__)


def _print_summary(results: list, summary: dict) -> None:
    """Print a detailed summary to stdout so it appears in the job output."""
    print("\n" + "=" * 70)
    print(f"  HTQ2 QUALITY GATE RESULTS (--tables {summary.get('tables_mode', 'all')})")
    print("=" * 70)
    print(f"  PASS: {summary['pass']}  |  WARN: {summary['warn']}  |  FAIL: {summary['fail']}")
    print(f"  Status: {summary['status'].upper()}")
    print("=" * 70)

    for status in ["FAIL", "WARN", "PASS"]:
        items = [r for r in results if r.result == status]
        if items:
            print(f"\n  [{status}] ({len(items)} checks)")
            for r in items:
                print(f"    {r.name}: {r.details}")

    print("\n" + "=" * 70)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    config = parse_quality_args()
    spark = SparkSession.builder.getOrCreate()

    logger.info(
        f"Quality Gate starting: catalog={config.catalog}, "
        f"schema={config.schema}, tables={config.tables}"
    )

    # Run checks scoped to the --tables parameter
    target_tables = config.get_target_tables()
    results = run_all_checks(
        spark, config,
        operating_date=date.today(),
        target_tables=target_tables,
    )

    # Write results to Delta table
    try:
        write_quality_results(spark, config, results)
    except Exception as e:
        logger.error(f"Failed to write quality results: {e}")

    # Summarize
    fail_count = sum(1 for r in results if r.result == "FAIL")
    warn_count = sum(1 for r in results if r.result == "WARN")
    pass_count = sum(1 for r in results if r.result == "PASS")

    summary = {
        "total_checks": len(results),
        "pass": pass_count,
        "warn": warn_count,
        "fail": fail_count,
        "status": "failed" if fail_count > 0 else "passed",
        "tables_mode": config.tables,
        "details": [
            {"name": r.name, "result": r.result, "details": r.details}
            for r in results if r.result in ("FAIL", "WARN")
        ],
    }

    _print_summary(results, summary)

    for r in results:
        if r.result == "FAIL":
            logger.error(f"FAIL: {r.name} \u2014 {r.details}")
        elif r.result == "WARN":
            logger.warning(f"WARN: {r.name} \u2014 {r.details}")

    if fail_count > 0:
        logger.error(f"Quality Gate FAILED with {fail_count} failures")
        print(json.dumps(summary, indent=2))
        sys.exit(1)

    print(json.dumps(summary, indent=2))
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        dbutils.notebook.exit(json.dumps(summary))
    except ImportError:
        pass


if __name__ == "__main__":
    main()
