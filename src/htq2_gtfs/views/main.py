"""Entry point for the HTQ2 GTFS Views task (create_all_views).

Layer 3 in the 8-task architecture. Pure DDL — no data processing.
Creates or replaces all 14 SQL views that join base + extension tables.

Usage (via python_wheel_task):
    htq2_gtfs views --catalog htq2_dev --schema silver
"""

from __future__ import annotations

import json
import sys
import time

from pyspark.sql import SparkSession

from htq2_gtfs.config import parse_views_args
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.models import SQL_VIEW_REGISTRY, generate_view_ddl

logger = get_logger(__name__)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_views_args()
    spark = SparkSession.builder.getOrCreate()

    logger.info(
        f"Views task starting: catalog={config.catalog}, schema={config.schema}"
    )

    created = 0
    failed = 0
    errors: list[str] = []

    for view_name, view_def in SQL_VIEW_REGISTRY.items():
        try:
            ddl = generate_view_ddl(config.catalog, config.schema, view_def)
            spark.sql(ddl)
            created += 1
            logger.info(f"  \u2713 {config.catalog}.{config.schema}.{view_name}")
        except Exception as e:
            failed += 1
            errors.append(f"{view_name}: {e}")
            logger.error(f"  \u2717 Failed to create view {view_name}: {e}")

    duration = time.time() - start_time
    status = "success" if failed == 0 else "failed"

    print("\n" + "=" * 70)
    print("  HTQ2 VIEWS RESULTS")
    print("=" * 70)
    print(f"  Created: {created}/{len(SQL_VIEW_REGISTRY)} views")
    print(f"  Failed:  {failed}")
    print(f"  Duration: {duration:.1f}s")
    if errors:
        print(f"  Errors:")
        for err in errors:
            print(f"    - {err}")
    print("=" * 70)

    exit_output = {
        "status": status,
        "views_created": created,
        "views_failed": failed,
        "duration_seconds": round(duration, 1),
    }

    if failed > 0:
        logger.error(f"Views task FAILED: {failed} views could not be created")
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
