"""Entry point for the HTQ2 GTFS Prep task (parse_and_enrich).

Layer 2 in the pipeline architecture. Responsibilities:
1. Read protobuf .pb files from Volume using AUTO LOADER (exactly-once)
2. Parse protobuf and enrich from static GTFS lookups
3. Inline validation (fail-fast before Silver)
4. APPEND to durable staging tables: bronze.parsed_journey, bronze.parsed_call

v3.1.8: Auto Loader writes raw binary to staging, parsing in main process.
Fixes foreachBatch worker process issue on job clusters (Spark Connect).

Usage (via python_wheel_task):
    htq2_gtfs prep --catalog htq2_dev --checkpoint_path dbfs:/htq2/checkpoints/autoloader
"""

from __future__ import annotations

import json
import sys
import time
import uuid
from datetime import date, datetime, timezone
from typing import Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from htq2_gtfs.config import parse_prep_args, PrepConfig, METADATA_TABLE
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.helpers.file_utils import FILENAME_TS_PATTERN
from htq2_gtfs.processing.core import GTFSProcessor, JOURNEY_SCHEMA, CALL_SCHEMA, VP_SCHEMA
from htq2_gtfs.prep.validator import validate_prep_output

logger = get_logger(__name__)

# Raw binary staging table name
RAW_PB_TABLE = "_autoloader_raw_pb"


def _ensure_staging_tables(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create staging tables if they don't exist."""
    j_fields = list(JOURNEY_SCHEMA.fields) + [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_parsed_timestamp", T.TimestampType(), False),
    ]
    fq_journey = f"{catalog}.{schema}.parsed_journey"
    if not spark.catalog.tableExists(fq_journey):
        spark.createDataFrame([], T.StructType(j_fields)).write.format("delta").saveAsTable(fq_journey)
        logger.info(f"Created staging table {fq_journey}")

    c_fields = list(CALL_SCHEMA.fields) + [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_parsed_timestamp", T.TimestampType(), False),
    ]
    fq_call = f"{catalog}.{schema}.parsed_call"
    if not spark.catalog.tableExists(fq_call):
        spark.createDataFrame([], T.StructType(c_fields)).write.format("delta").saveAsTable(fq_call)
        logger.info(f"Created staging table {fq_call}")

    vp_fields = list(VP_SCHEMA.fields) + [
        T.StructField("_run_id", T.StringType(), False),
        T.StructField("_parsed_timestamp", T.TimestampType(), False),
    ]
    fq_vp = f"{catalog}.{schema}.parsed_vehicle_positions"
    if not spark.catalog.tableExists(fq_vp):
        spark.createDataFrame([], T.StructType(vp_fields)).write.format("delta").saveAsTable(fq_vp)
        logger.info(f"Created staging table {fq_vp}")


def _ingest_raw_pb(spark: SparkSession, config: PrepConfig, run_id: str) -> int:
    """Use Auto Loader to discover and write new .pb files to raw staging.

    Returns the number of new files ingested (0 = nothing new).
    Auto Loader checkpoint guarantees exactly-once file processing.
    """
    fq_raw = f"{config.catalog}.{config.bronze_schema}.{RAW_PB_TABLE}"

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.pb")
        .option("cloudFiles.schemaLocation", config.schema_checkpoint)
        .load(config.realtime_path)
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_filename", F.element_at(F.split(F.col("path"), "/"), -1))
    )

    query = (
        raw_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.data_checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(fq_raw)
    )
    query.awaitTermination()

    # Count how many files were loaded in this run
    new_count = spark.table(fq_raw).filter(F.col("_run_id") == run_id).count()
    logger.info(f"Auto Loader ingested {new_count} new .pb files")
    return new_count


def _parse_raw_files(
    spark: SparkSession,
    config: PrepConfig,
    processor: GTFSProcessor,
    run_id: str,
) -> dict:
    """Parse protobuf from raw binary staging table.

    Reads files for this run_id, parses protobuf on the driver,
    writes structured data to staging tables.
    Returns stats dict.
    """
    from google.transit import gtfs_realtime_pb2

    fq_raw = f"{config.catalog}.{config.bronze_schema}.{RAW_PB_TABLE}"
    raw_files = spark.table(fq_raw).filter(F.col("_run_id") == run_id).collect()

    stats = {"files": len(raw_files), "journeys": 0, "calls": 0, "vp": 0, "errors": 0}
    journey_rows: list[tuple] = []
    call_rows: list[tuple] = []
    vp_rows: list[tuple] = []

    # Group raw files into timestamped sets so we can apply cross-feed enrichment.
    # Filenames normally look like: 2025-04-16T08-30-00Z-skane-TripUpdates.pb
    groups: dict[str, dict[str, Any]] = {}

    for row in raw_files:
        content = bytes(row["content"])
        filename = row["_filename"]

        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(content)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Error parsing protobuf {filename}: {e}")
            continue

        match = FILENAME_TS_PATTERN.match(filename)
        if match:
            ts_key, _region, feed_name = match.groups()
            if feed_name == "SeviceAlerts.pb":
                feed_name = "ServiceAlerts.pb"
        else:
            # Fallback: group by header timestamp (best-effort)
            header_ts = int(feed.header.timestamp) if feed.header.timestamp else 0
            ts_key = (
                datetime.utcfromtimestamp(header_ts).strftime("%Y-%m-%dT%H-%M-%SZ")
                if header_ts
                else "unknown"
            )
            if "TripUpdates" in filename:
                feed_name = "TripUpdates.pb"
            elif "VehiclePositions" in filename:
                feed_name = "VehiclePositions.pb"
            elif "ServiceAlerts" in filename or "SeviceAlerts" in filename:
                feed_name = "ServiceAlerts.pb"
            else:
                feed_name = "unknown.pb"

        groups.setdefault(ts_key, {})[feed_name] = feed

    parsed_ts = datetime.now(timezone.utc)

    # Parse and enrich per timestamp group
    for ts_key in sorted(groups.keys()):
        feeds = groups[ts_key]
        journeys: list[dict] = []
        calls: list[dict] = []

        snapshot_ts = None
        # Prefer TripUpdates header timestamp for fallback OperatingDate
        header_feed = feeds.get("TripUpdates.pb") or feeds.get("VehiclePositions.pb") or feeds.get("ServiceAlerts.pb")
        if header_feed and getattr(header_feed, "header", None) and header_feed.header.timestamp:
            snapshot_ts = datetime.utcfromtimestamp(int(header_feed.header.timestamp))
        operating_date = snapshot_ts.strftime("%Y-%m-%d") if snapshot_ts else None
        operating_weekday = snapshot_ts.strftime("%A") if snapshot_ts else None
        snapshot_dt_str = snapshot_ts.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if snapshot_ts else None

        try:
            tu_feed = feeds.get("TripUpdates.pb")
            if tu_feed:
                j, c = processor._parse_trip_updates(tu_feed, ts_key)
                journeys.extend(j)
                calls.extend(c)

            vp_feed = feeds.get("VehiclePositions.pb")
            if vp_feed:
                # Enrich journey/call objects in-place
                processor._enrich_from_vehicle_positions(vp_feed, journeys, calls)
                # Also stage raw VP rows for Phase 2 GPS tables
                for vp in processor._extract_vehicle_positions(vp_feed):
                    row_vals = [vp.get(f.name) for f in VP_SCHEMA.fields]
                    vp_rows.append(tuple(row_vals + [run_id, parsed_ts]))

            sa_feed = feeds.get("ServiceAlerts.pb")
            if sa_feed and journeys:
                processor._enrich_from_service_alerts(sa_feed, journeys, calls)

            # Backfill OperatingDate/WeekDay + assignment timestamps when absent
            for j in journeys:
                if operating_date and not j.get("OperatingDate"):
                    j["OperatingDate"] = operating_date
                if operating_weekday and not j.get("OperatingWeekDay"):
                    j["OperatingWeekDay"] = operating_weekday
                if snapshot_dt_str and not j.get("VehicleAssignedFromDateTime"):
                    j["VehicleAssignedFromDateTime"] = snapshot_dt_str
                    j["VehicleAssignedUptoDateTime"] = snapshot_dt_str

            for c in calls:
                if operating_date and not c.get("OperatingDate"):
                    c["OperatingDate"] = operating_date
                if operating_weekday and not c.get("OperatingWeekDay"):
                    c["OperatingWeekDay"] = operating_weekday
                if snapshot_dt_str and not c.get("VehicleAssignedFromDateTime"):
                    c["VehicleAssignedFromDateTime"] = snapshot_dt_str
                    c["VehicleAssignedUptoDateTime"] = snapshot_dt_str

            for j in journeys:
                norm = processor._normalize_journey(j)
                row_vals = [norm.get(f.name) for f in JOURNEY_SCHEMA.fields]
                journey_rows.append(tuple(row_vals + [run_id, parsed_ts]))

            for c in calls:
                norm = processor._normalize_call(c)
                row_vals = [norm.get(f.name) for f in CALL_SCHEMA.fields]
                call_rows.append(tuple(row_vals + [run_id, parsed_ts]))

        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Error parsing grouped feeds {ts_key}: {e}", exc_info=True)

    # Write to staging tables
    if journey_rows:
        j_schema = T.StructType(list(JOURNEY_SCHEMA.fields) + [
            T.StructField("_run_id", T.StringType(), False),
            T.StructField("_parsed_timestamp", T.TimestampType(), False),
        ])
        spark.createDataFrame(journey_rows, schema=j_schema).write.format("delta") \
            .mode("append").saveAsTable(f"{config.catalog}.{config.bronze_schema}.parsed_journey")
        stats["journeys"] = len(journey_rows)
        logger.info(f"Wrote {len(journey_rows):,} journeys")

    if call_rows:
        c_schema = T.StructType(list(CALL_SCHEMA.fields) + [
            T.StructField("_run_id", T.StringType(), False),
            T.StructField("_parsed_timestamp", T.TimestampType(), False),
        ])
        spark.createDataFrame(call_rows, schema=c_schema).write.format("delta") \
            .mode("append").saveAsTable(f"{config.catalog}.{config.bronze_schema}.parsed_call")
        stats["calls"] = len(call_rows)
        logger.info(f"Wrote {len(call_rows):,} calls")

    if vp_rows:
        vp_schema = T.StructType(list(VP_SCHEMA.fields) + [
            T.StructField("_run_id", T.StringType(), False),
            T.StructField("_parsed_timestamp", T.TimestampType(), False),
        ])
        spark.createDataFrame(vp_rows, schema=vp_schema).write.format("delta") \
            .mode("append").saveAsTable(f"{config.catalog}.{config.bronze_schema}.parsed_vehicle_positions")
        stats["vp"] = len(vp_rows)
        logger.info(f"Wrote {len(vp_rows):,} VP rows")

    return stats


def _write_run_metadata(
    spark: SparkSession, config: PrepConfig,
    run_id: str, stats: dict, duration: float,
    status: str = "success", error_message: Optional[str] = None,
) -> None:
    """Write run metadata for downstream filtering."""
    from htq2_gtfs.processing.watermark import METADATA_SCHEMA
    table_name = config.silver_table(METADATA_TABLE)
    row_data = [(
        run_id, datetime.now(timezone.utc), date.today(),
        [], stats.get("files", 0),
        {"parsed_journey": stats.get("journeys", 0), "parsed_call": stats.get("calls", 0)},
        duration, status, error_message,
    )]
    spark.createDataFrame(row_data, schema=METADATA_SCHEMA).write.format("delta") \
        .mode("append").saveAsTable(table_name)


def main() -> None:
    """Entry point for Databricks python_wheel_task."""
    start_time = time.time()
    config = parse_prep_args()
    run_id = str(uuid.uuid4())[:8]

    logger.info(f"HTQ2 Prep (Auto Loader) starting: run_id={run_id}, catalog={config.catalog}")
    spark = SparkSession.builder.getOrCreate()
    stats = {}
    status = "success"
    error_msg = None

    try:
        # Step 0: Ensure staging tables exist
        _ensure_staging_tables(spark, config.catalog, config.bronze_schema)

        # Step 1: Auto Loader ingests new .pb files to raw binary table
        logger.info(f"Auto Loader source: {config.realtime_path}")
        logger.info(f"Checkpoint: {config.checkpoint_path}")
        new_files = _ingest_raw_pb(spark, config, run_id)

        if new_files == 0:
            logger.info("No new files to process — exiting")
            stats = {"files": 0, "journeys": 0, "calls": 0, "vp": 0, "errors": 0}
        else:
            # Step 2: Load static enrichment data
            processor = GTFSProcessor(spark, config)
            processor.load_static_data(date.today())

            # Step 3: Parse protobuf in main process (not foreachBatch!)
            stats = _parse_raw_files(spark, config, processor, run_id)

            # Step 4: Validate output
            if stats["journeys"] > 0:
                fq_j = f"{config.catalog}.{config.bronze_schema}.parsed_journey"
                fq_c = f"{config.catalog}.{config.bronze_schema}.parsed_call"
                validate_prep_output(
                    spark.table(fq_j).filter(F.col("_run_id") == run_id),
                    spark.table(fq_c).filter(F.col("_run_id") == run_id),
                )

    except Exception as e:
        logger.error(f"Prep failed: {e}", exc_info=True)
        status = "failed"
        error_msg = str(e)

    duration = time.time() - start_time

    try:
        _write_run_metadata(spark, config, run_id, stats, duration, status, error_msg)
    except Exception as e:
        logger.error(f"Failed to write metadata: {e}")

    # Print summary
    print("\n" + "=" * 70)
    print("  HTQ2 PREP (Auto Loader) v3.1.8")
    print("=" * 70)
    print(f"  Run ID:       {run_id}")
    print(f"  Status:       {status.upper()}")
    print(f"  Duration:     {duration:.1f}s")
    print(f"  Files:        {stats.get('files', 0)}")
    print(f"  Journeys:     {stats.get('journeys', 0):,}")
    print(f"  Calls:        {stats.get('calls', 0):,}")
    print(f"  VP rows:      {stats.get('vp', 0):,}")
    print(f"  Errors:       {stats.get('errors', 0)}")
    if error_msg:
        print(f"  Error:        {error_msg}")
    print("=" * 70)

    exit_output = {
        "status": status, "run_id": run_id,
        "duration_seconds": round(duration, 1),
        "files_processed": stats.get("files", 0),
        "parsed_journey_rows": stats.get("journeys", 0),
        "parsed_call_rows": stats.get("calls", 0),
        "parsed_vp_rows": stats.get("vp", 0),
        "mode": "autoloader",
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