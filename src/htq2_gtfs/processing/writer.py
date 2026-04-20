"""Delta Lake write utilities for HTQ2 GTFS pipeline.

v2.0: MERGE by PK for idempotent writes (replaces append-only).
Handles table creation, schema alignment, and incremental writes
for all 22 Silver tables and the SQL views.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from htq2_gtfs.config import TransformConfig
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.processing.models import (
    SQL_VIEW_REGISTRY,
    TABLE_REGISTRY,
    generate_view_ddl,
)

logger = get_logger(__name__)

# Table properties for auto-optimization
TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}


def ensure_all_table_schemas(
    spark: SparkSession,
    config: TransformConfig,
) -> int:
    """Ensure deferred/independent Silver tables exist with correct schemas.

    Only targets the 6 independent tables (table_type='independent') that
    need pre-creation so downstream SQL views and queries can reference
    them without errors.

    Returns:
        Number of tables created.
    """
    created = 0
    for table_name, table_def in TABLE_REGISTRY.items():
        if table_def.table_type != "independent":
            continue

        fq_table = config.silver_table(table_name)
        try:
            if not spark.catalog.tableExists(fq_table):
                all_columns = table_def.columns + ["SourceFiles", "ProcessedTimestamp"]
                schema = T.StructType([
                    T.StructField(c, T.StringType(), True)
                    for c in all_columns
                    if c != "ProcessedTimestamp"
                ] + [
                    T.StructField("ProcessedTimestamp", T.TimestampType(), True),
                ])
                empty_df = spark.createDataFrame([], schema)
                empty_df.write.format("delta").mode("overwrite").saveAsTable(fq_table)
                created += 1
                logger.info(f"Created empty table {fq_table} ({len(all_columns)} columns)")
        except Exception as e:
            logger.warning(f"Could not ensure table {fq_table}: {e}")

    if created > 0:
        logger.info(f"Created {created} missing independent table(s)")
    return created


def write_table(
    df: DataFrame,
    config: TransformConfig,
    table_name: str,
    source_files: list[str] | None = None,
) -> int:
    """Write a DataFrame to a Silver Delta table using MERGE by PK.

    v2.0: Uses MERGE for idempotent writes. Falls back to saveAsTable
    for new tables or when PK info is unavailable.

    Args:
        df: DataFrame to write.
        config: Transform configuration.
        table_name: Table name (snake_case, without catalog/schema prefix).
        source_files: List of source file names for traceability.

    Returns:
        Number of rows written.
    """
    from delta.tables import DeltaTable

    fq_table = config.silver_table(table_name)

    # Add metadata columns
    if source_files:
        files_str = ",".join(source_files)
        df = df.withColumn("SourceFiles", F.lit(files_str))
    else:
        df = df.withColumn("SourceFiles", F.lit("from_staging"))
    df = df.withColumn("ProcessedTimestamp", F.current_timestamp())

    # Align columns to model definition
    table_def = TABLE_REGISTRY.get(table_name)
    if table_def:
        expected_cols = table_def.columns + ["SourceFiles", "ProcessedTimestamp"]
        for col_name in expected_cols:
            if col_name not in df.columns:
                df = df.withColumn(col_name, F.lit(None).cast("string"))
        df = df.select(*[c for c in expected_cols if c in df.columns])

    row_count = df.count()

    if row_count == 0:
        logger.warning(f"0 rows for {table_name} — skipping write")
        return 0

    # Determine PK columns for MERGE
    pk_cols = table_def.primary_keys if table_def else None

    # If table doesn't exist yet, create it with saveAsTable
    if not df.sparkSession.catalog.tableExists(fq_table):
        df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(fq_table)
        logger.info(f"Created + wrote {row_count:,} rows to {fq_table}")
        return row_count

    # MERGE by PK for idempotent writes
    if pk_cols:
        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in pk_cols]
        )
        try:
            target_table = DeltaTable.forName(df.sparkSession, fq_table)
            (
                target_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"Merged {row_count:,} rows into {fq_table} (PK: {pk_cols})")
            return row_count
        except Exception as e:
            logger.warning(
                f"MERGE failed for {fq_table}, falling back to append: {e}"
            )

    # Fallback: append mode
    df.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(fq_table)
    logger.info(f"Appended {row_count:,} rows to {fq_table}")
    return row_count


def ensure_table_properties(
    spark: SparkSession,
    config: TransformConfig,
) -> None:
    """Set auto-optimize and retention properties on all Silver tables."""
    for table_name in TABLE_REGISTRY:
        fq_table = config.silver_table(table_name)
        try:
            if spark.catalog.tableExists(fq_table):
                props = ", ".join(
                    f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items()
                )
                spark.sql(f"ALTER TABLE {fq_table} SET TBLPROPERTIES ({props})")
        except Exception as e:
            logger.debug(f"Could not set properties on {fq_table}: {e}")


def create_or_replace_views(
    spark: SparkSession,
    config: TransformConfig,
) -> int:
    """Create or replace all SQL views that join base + extension tables.

    Returns the number of views created.
    """
    created = 0
    for view_name, view_def in SQL_VIEW_REGISTRY.items():
        try:
            ddl = generate_view_ddl(config.catalog, config.silver_schema, view_def)
            spark.sql(ddl)
            created += 1
            logger.info(f"Created view: {config.silver_table(view_name)}")
        except Exception as e:
            logger.error(f"Failed to create view {view_name}: {e}")

    logger.info(f"Created {created}/{len(SQL_VIEW_REGISTRY)} SQL views")
    return created
