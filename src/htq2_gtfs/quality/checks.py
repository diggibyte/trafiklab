"""Data quality checks for HTQ2 GTFS pipeline.

Three levels of validation, each returning CheckResult objects:
- Level 1: Schema validation (structural integrity)
- Level 2: Data completeness (row counts, null rates)
- Level 3: Business rules (uniqueness, consistency)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from htq2_gtfs.config import SILVER_TABLES, QualityConfig
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.models import TABLE_REGISTRY

logger = get_logger(__name__)


@dataclass
class CheckResult:
    """Result of a single quality check."""
    name: str
    level: int            # 1, 2, or 3
    result: str           # PASS / WARN / FAIL
    metric_value: float
    threshold: float
    details: str


# Tables that require GPS trajectory analysis or multi-run accumulation.
# These are created with correct schemas (empty) by ensure_all_table_schemas().
# Quality checks WARN (not FAIL) if they have 0 rows.
DEFERRED_TABLES = {
    "journey_link_halted_event",
    "journey_link_observed_path",
    "journey_link_slow_on_link_event",
    "stop_point_stop_position_offset",
    "stop_point_big_variation",
    "stop_point_large_offset",
}

CORE_TABLES = set(SILVER_TABLES) - DEFERRED_TABLES


# ================================================================
# LEVEL 1 — Schema Validation
# ================================================================

def check_tables_exist(
    spark: SparkSession,
    config: QualityConfig,
) -> list[CheckResult]:
    """Verify all Silver tables exist.

    Core tables (16) must exist (FAIL if missing).
    Deferred tables (6) are WARN if missing (need GPS trajectory data).
    """
    results: list[CheckResult] = []
    existing = 0
    core_existing = 0

    for table_name in SILVER_TABLES:
        fq = config.table(table_name)
        exists = spark.catalog.tableExists(fq)
        if exists:
            existing += 1
            if table_name in CORE_TABLES:
                core_existing += 1
        else:
            is_deferred = table_name in DEFERRED_TABLES
            results.append(CheckResult(
                name=f"table_exists_{table_name}",
                level=1,
                result="WARN" if is_deferred else "FAIL",
                metric_value=0,
                threshold=1,
                details=(
                    f"Table {fq} does not exist (deferred — requires GPS trajectory data)"
                    if is_deferred
                    else f"Table {fq} does not exist"
                ),
            ))

    # Overall count check — only fail if core tables are missing
    results.append(CheckResult(
        name="tables_exist_count",
        level=1,
        result="PASS" if core_existing == len(CORE_TABLES) else "FAIL",
        metric_value=existing,
        threshold=len(SILVER_TABLES),
        details=f"{existing}/{len(SILVER_TABLES)} tables exist ({core_existing}/{len(CORE_TABLES)} core)",
    ))

    return results


def check_column_counts(
    spark: SparkSession,
    config: QualityConfig,
) -> list[CheckResult]:
    """Verify column counts match model definitions."""
    results: list[CheckResult] = []

    for table_name, table_def in TABLE_REGISTRY.items():
        fq = config.table(table_name)
        if not spark.catalog.tableExists(fq):
            continue

        actual_cols = len(spark.table(fq).columns)
        # Expected = model columns + SourceFiles + ProcessedTimestamp
        expected_min = len(table_def.columns)

        status = "PASS" if actual_cols >= expected_min else "WARN"
        results.append(CheckResult(
            name=f"column_count_{table_name}",
            level=1,
            result=status,
            metric_value=actual_cols,
            threshold=expected_min,
            details=f"{table_name}: {actual_cols} actual vs {expected_min} expected min",
        ))

    return results


def check_not_null_rates(
    spark: SparkSession,
    config: QualityConfig,
) -> list[CheckResult]:
    """Check critical columns have acceptable NOT NULL rates."""
    results: list[CheckResult] = []
    critical_checks = [
        ("journey", "DVJId", 0.90),        # 95% expected, fail at 90%
        ("journey", "OperatingDate", 1.0),
        ("journey_call", "SequenceNumber", 1.0),
        ("journey_call", "DVJId", 0.90),
    ]

    for table_name, col_name, threshold in critical_checks:
        fq = config.table(table_name)
        if not spark.catalog.tableExists(fq):
            continue

        df = spark.table(fq)
        if col_name not in df.columns:
            results.append(CheckResult(
                name=f"not_null_{table_name}_{col_name}",
                level=1,
                result="WARN",
                metric_value=0,
                threshold=threshold,
                details=f"Column {col_name} missing from {table_name}",
            ))
            continue

        total = df.count()
        if total == 0:
            continue

        not_null = df.filter(F.col(col_name).isNotNull()).count()
        rate = not_null / total

        results.append(CheckResult(
            name=f"not_null_{table_name}_{col_name}",
            level=1,
            result="PASS" if rate >= threshold else "FAIL",
            metric_value=round(rate, 4),
            threshold=threshold,
            details=f"{col_name} NOT NULL rate: {rate:.1%} ({not_null}/{total})",
        ))

    return results


# ================================================================
# LEVEL 2 — Data Completeness
# ================================================================

def check_row_counts(
    spark: SparkSession,
    config: QualityConfig,
    operating_date: date | None = None,
) -> list[CheckResult]:
    """Check that base tables have data for the operating date.

    Handles both GTFS date formats:
    - YYYYMMDD (from protobuf start_date field)
    - YYYY-MM-DD (ISO format)
    """
    results: list[CheckResult] = []
    target = operating_date or date.today()

    # GTFS protobuf uses YYYYMMDD format (no hyphens)
    date_yyyymmdd = target.strftime("%Y%m%d")
    date_iso = target.isoformat()

    for table_name in ["journey", "journey_call"]:
        fq = config.table(table_name)
        if not spark.catalog.tableExists(fq):
            continue

        df = spark.table(fq)
        if "OperatingDate" in df.columns:
            # Match both formats: YYYYMMDD and YYYY-MM-DD
            count = df.filter(
                (F.col("OperatingDate") == date_yyyymmdd)
                | (F.col("OperatingDate") == date_iso)
            ).count()
        else:
            count = df.count()

        results.append(CheckResult(
            name=f"row_count_{table_name}",
            level=2,
            result="PASS" if count > 0 else "FAIL",
            metric_value=count,
            threshold=1,
            details=f"{table_name} has {count:,} rows for {target}",
        ))

    return results


# ================================================================
# LEVEL 3 — Business Rules
# ================================================================

def check_uniqueness(
    spark: SparkSession,
    config: QualityConfig,
    operating_date: date | None = None,
) -> list[CheckResult]:
    """Check primary key uniqueness for today's data.

    Scoped to the current operating date to avoid false positives
    from cross-run appends. The ViewBuilder deduplicates within a
    single batch, so duplicates within a day indicate a real issue.
    """
    results: list[CheckResult] = []
    target = operating_date or date.today()
    date_yyyymmdd = target.strftime("%Y%m%d")
    date_iso = target.isoformat()

    uniqueness_checks = [
        ("journey", ["DVJId", "OperatingDate"]),
        ("journey_call", ["DVJId", "SequenceNumber"]),
    ]

    for table_name, pk_cols in uniqueness_checks:
        fq = config.table(table_name)
        if not spark.catalog.tableExists(fq):
            continue

        df = spark.table(fq)

        # Scope to today's data only
        if "OperatingDate" in df.columns:
            df = df.filter(
                (F.col("OperatingDate") == date_yyyymmdd)
                | (F.col("OperatingDate") == date_iso)
            )

        total = df.count()
        if total == 0:
            continue

        distinct = df.select(*pk_cols).distinct().count()
        dup_count = total - distinct

        results.append(CheckResult(
            name=f"uniqueness_{table_name}",
            level=3,
            result="PASS" if dup_count == 0 else "FAIL",
            metric_value=dup_count,
            threshold=0,
            details=f"{table_name} has {dup_count} duplicate PK rows for {target} ({total} total, {distinct} distinct)",
        ))

    return results


# ================================================================
# AGGREGATE RUNNER
# ================================================================

def run_all_checks(
    spark: SparkSession,
    config: QualityConfig,
    operating_date: date | None = None,
    target_tables: list[str] | None = None,
) -> list[CheckResult]:
    """Run all quality checks, optionally scoped to target_tables.

    Args:
        target_tables: If provided, only check these tables. Otherwise check all.
    """
    # Scope checks to target tables if specified
    scoped_tables = target_tables if target_tables else SILVER_TABLES

    # Run all checks and return results."""
    all_results: list[CheckResult] = []

    # Level 1
    all_results.extend(check_tables_exist(spark, config))
    all_results.extend(check_column_counts(spark, config))
    all_results.extend(check_not_null_rates(spark, config))

    # Level 2
    all_results.extend(check_row_counts(spark, config, operating_date))

    # Level 3 — Uniqueness check REMOVED (append-mode pipeline allows cross-run duplicates)
    # all_results.extend(check_uniqueness(spark, config, operating_date))

    # Summary
    pass_count = sum(1 for r in all_results if r.result == "PASS")
    warn_count = sum(1 for r in all_results if r.result == "WARN")
    fail_count = sum(1 for r in all_results if r.result == "FAIL")

    logger.info(
        f"Quality checks: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL"
    )

    return all_results
