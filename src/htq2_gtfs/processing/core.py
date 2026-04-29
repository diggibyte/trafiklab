"""GTFS Protobuf Parser and Static Data Loader.

Port of Synapse gtfs_processor_core_delta.py.
Key change: Creates Spark DataFrames immediately after parsing —
no pandas intermediate. Protobuf parsing still runs on the driver
(files are <300KB each).
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from datetime import date, datetime, timezone
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from htq2_gtfs.config import TransformConfig
from htq2_gtfs.helpers.file_utils import RealtimeFileSet, find_best_static_file
from htq2_gtfs.helpers.logging_config import get_logger

logger = get_logger(__name__)


# ── Explicit schemas to avoid CANNOT_DETERMINE_TYPE ──────────────────────────
# When protobuf fields are absent across all records, Spark can't infer
# the type from an all-None column. Defining StructTypes up front avoids
# void/NullType columns that crash Photon in DBR 17.x.

JOURNEY_SCHEMA = T.StructType([
    T.StructField("DVJId", T.StringType(), True),
    T.StructField("OperatingDate", T.StringType(), True),
    T.StructField("OperatingWeekDay", T.StringType(), True),
    T.StructField("JourneyNumber", T.StringType(), True),
    T.StructField("LineNumber", T.StringType(), True),
    T.StructField("DirectionNumber", T.IntegerType(), True),
    T.StructField("VehicleNumber", T.StringType(), True),
    T.StructField("JourneyStartTime", T.StringType(), True),
    T.StructField("DestinationName", T.StringType(), True),
    T.StructField("MonitoredYesNo", T.StringType(), True),
    T.StructField("Coord_Latitude", T.FloatType(), True),
    T.StructField("Coord_Longitude", T.FloatType(), True),
    T.StructField("RunByVehicleGid", T.StringType(), True),
    # --- New enrichment fields (matching Synapse) ---
    T.StructField("ExtendedLineDesignation", T.StringType(), True),
    T.StructField("JourneyOriginName", T.StringType(), True),
    T.StructField("JourneyEndStopName", T.StringType(), True),
    T.StructField("BlockNumber", T.StringType(), True),
    T.StructField("StartMunicipalityName", T.StringType(), True),
    T.StructField("VehicleAssignedFromDateTime", T.StringType(), True),
    T.StructField("VehicleAssignedUptoDateTime", T.StringType(), True),
    T.StructField("InProgressFromDateTime", T.StringType(), True),
    T.StructField("InProgressUptoDateTime", T.StringType(), True),
    T.StructField("AssignedDriverGid", T.StringType(), True),
    T.StructField("JourneyStartDateTime", T.StringType(), True),
    T.StructField("JourneyLastStopSequenceNumber", T.IntegerType(), True),
    T.StructField("ReinforcedDVJId", T.StringType(), True),
    T.StructField("UsesJourneyPatternId", T.StringType(), True),
    T.StructField("JourneyGid", T.StringType(), True),
    # --- Internal fields ---
    T.StructField("_is_cancelled", T.BooleanType(), True),
    T.StructField("_source_timestamp", T.StringType(), True),
])

CALL_SCHEMA = T.StructType([
    T.StructField("DVJId", T.StringType(), True),
    T.StructField("OperatingDate", T.StringType(), True),
    T.StructField("OperatingWeekDay", T.StringType(), True),
    T.StructField("SequenceNumber", T.IntegerType(), True),
    T.StructField("StopAreaNumber", T.StringType(), True),
    T.StructField("StopName", T.StringType(), True),
    T.StructField("JourneyNumber", T.StringType(), True),
    T.StructField("LineNumber", T.StringType(), True),
    T.StructField("DirectionNumber", T.IntegerType(), True),
    T.StructField("ArrivalDelaySeconds", T.IntegerType(), True),
    T.StructField("ObservedArrivalDateTime", T.StringType(), True),
    T.StructField("DepartureDelaySeconds", T.IntegerType(), True),
    T.StructField("ObservedDepartureDateTime", T.StringType(), True),
    # --- Journey-level propagation (matching Synapse) ---
    T.StructField("DefaultTransportModeCode", T.StringType(), True),
    T.StructField("TransportAuthorityNumber", T.IntegerType(), True),
    T.StructField("ExtendedLineDesignation", T.StringType(), True),
    T.StructField("JourneyStartTime", T.StringType(), True),
    T.StructField("DatedJourneyStateName", T.StringType(), True),
    T.StructField("JourneyOriginName", T.StringType(), True),
    T.StructField("JourneyEndStopName", T.StringType(), True),
    T.StructField("OperatorName", T.StringType(), True),
    T.StructField("BlockNumber", T.StringType(), True),
    T.StructField("VehicleNumber", T.StringType(), True),
    T.StructField("StartMunicipalityName", T.StringType(), True),
    T.StructField("DestinationName", T.StringType(), True),
    T.StructField("VehicleAssignedFromDateTime", T.StringType(), True),
    T.StructField("VehicleAssignedUptoDateTime", T.StringType(), True),
    T.StructField("RunByVehicleGid", T.StringType(), True),
    T.StructField("InProgressFromDateTime", T.StringType(), True),
    T.StructField("InProgressUptoDateTime", T.StringType(), True),
    T.StructField("AssignedDriverGid", T.StringType(), True),
    T.StructField("JourneyStartDateTime", T.StringType(), True),
    T.StructField("JourneyLastStopSequenceNumber", T.IntegerType(), True),
    T.StructField("ReinforcedDVJId", T.StringType(), True),
    T.StructField("UsesJourneyPatternId", T.StringType(), True),
    T.StructField("JourneyGid", T.StringType(), True),
    # --- Call-specific enrichment (matching Synapse) ---
    T.StructField("MunicipalityName", T.StringType(), True),
    T.StructField("TimingPointYesNo", T.IntegerType(), True),
    T.StructField("ArrivalStateName", T.StringType(), True),
    T.StructField("TimetabledLatestArrivalTime", T.StringType(), True),
    T.StructField("ObservedArrivalTime", T.StringType(), True),
    T.StructField("DepartureStateName", T.StringType(), True),
    T.StructField("TimetabledEarliestDepartureTime", T.StringType(), True),
    T.StructField("ObservedDepartureTime", T.StringType(), True),
    T.StructField("TimetabledEarliestDepartureHour", T.StringType(), True),
    T.StructField("TimetabledEarliestDepartureCalendarWeekDay", T.StringType(), True),
    T.StructField("StopPointLocalNumber", T.IntegerType(), True),
    T.StructField("JourneyPatternPointNumber", T.IntegerType(), True),
    T.StructField("TimetabledLatestArrivalDateTime", T.StringType(), True),
    T.StructField("TargetArrivalDateTime", T.StringType(), True),
    T.StructField("TimetabledEarliestDepartureDateTime", T.StringType(), True),
    T.StructField("TargetDepartureDateTime", T.StringType(), True),
    T.StructField("DistanceToNextStop", T.FloatType(), True),
    T.StructField("DistanceFromPreviousStop", T.FloatType(), True),
    T.StructField("Coord_Latitude", T.FloatType(), True),
    T.StructField("Coord_Longitude", T.FloatType(), True),
    T.StructField("ArrivalStateNumber", T.IntegerType(), True),
    T.StructField("DepartureStateNumber", T.IntegerType(), True),
    T.StructField("JPPGid", T.StringType(), True),
    T.StructField("PreviousJPPGid", T.StringType(), True),
    T.StructField("StopAreaGid", T.StringType(), True),
    # --- Internal ---
    T.StructField("_source_timestamp", T.StringType(), True),
])

# ── VehiclePositions schema for Phase 2 GPS tables ────────────────────────────
VP_SCHEMA = T.StructType([
    T.StructField("DVJId", T.StringType(), True),
    T.StructField("trip_id", T.StringType(), True),
    T.StructField("vehicle_id", T.StringType(), True),
    T.StructField("latitude", T.DoubleType(), True),
    T.StructField("longitude", T.DoubleType(), True),
    T.StructField("bearing", T.DoubleType(), True),
    T.StructField("speed", T.DoubleType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("OperatingDate", T.StringType(), True),
])

STOPS_SCHEMA = T.StructType([
    T.StructField("stop_id", T.StringType(), True),
    T.StructField("stop_name", T.StringType(), True),
    T.StructField("stop_lat", T.DoubleType(), True),
    T.StructField("stop_lon", T.DoubleType(), True),
])


class GTFSProcessor:
    """Parses GTFS-RT protobuf feeds and builds Spark DataFrames.

    Responsibilities:
    1. Loading and caching static GTFS data (stops, routes, DVJId mapping)
    2. Parsing protobuf feeds (TripUpdates, VehiclePositions, ServiceAlerts)
    3. Creating base Journey and JourneyCall DataFrames with DVJId resolution

    All output is Spark DataFrames — no pandas.
    """

    def __init__(self, spark: SparkSession, config: TransformConfig) -> None:
        self.spark = spark
        self.config = config
        self._static_data: dict[str, list[dict]] = {}
        self._dvj_mapping: dict[str, str] = {}
        self._trip_enrichment: dict[str, dict] = {}
        self._stop_lookup: dict[str, dict] = {}

    # ================================================================
    # STATIC DATA LOADING
    # ================================================================

    def load_static_data(self, target_date: date) -> None:
        """Load static GTFS data (stops, routes, DVJId mapping).

        Preferred source: bronze Delta tables produced by the static loader
        job (static_routes/static_trips/static_stops/static_stop_times/
        static_shapes/static_dvj_mapping).

        Fallback source: ZIP files in the Volume.

        The DVJId mapping is loaded into a Python dict for fast O(1)
        lookups during protobuf parsing.
        """
        bronze_schema = getattr(self.config, "bronze_schema", "bronze")
        catalog = self.config.catalog

        fq_trips = f"{catalog}.{bronze_schema}.static_trips"
        fq_routes = f"{catalog}.{bronze_schema}.static_routes"
        fq_stops = f"{catalog}.{bronze_schema}.static_stops"
        fq_stop_times = f"{catalog}.{bronze_schema}.static_stop_times"
        fq_dvj = f"{catalog}.{bronze_schema}.static_dvj_mapping"

        have_static_tables = all(
            self.spark.catalog.tableExists(t)
            for t in [fq_trips, fq_routes, fq_stops, fq_stop_times]
        )
        have_dvj_table = self.spark.catalog.tableExists(fq_dvj)

        if have_static_tables:
            try:
                self._build_enrichment_lookups_from_tables(
                    routes_df=self.spark.table(fq_routes),
                    trips_df=self.spark.table(fq_trips),
                    stops_df=self.spark.table(fq_stops),
                    stop_times_df=self.spark.table(fq_stop_times),
                )
                logger.info(
                    f"Loaded static enrichment from bronze tables: {fq_trips}, {fq_routes}, {fq_stops}, {fq_stop_times}"
                )
            except Exception as e:
                self._trip_enrichment = {}
                self._stop_lookup = {}
                logger.warning(f"Failed to load static enrichment from bronze tables: {e}", exc_info=True)

            if have_dvj_table:
                try:
                    self._dvj_mapping = self._load_dvj_mapping_from_table(self.spark.table(fq_dvj))
                    logger.info(
                        f"Loaded DVJId mapping from bronze table {fq_dvj}: {len(self._dvj_mapping):,} entries",
                        extra={"row_count": len(self._dvj_mapping)},
                    )
                except Exception as e:
                    self._dvj_mapping = {}
                    logger.warning(f"Failed to load DVJId mapping from bronze table {fq_dvj}: {e}", exc_info=True)
            else:
                self._dvj_mapping = {}
                logger.warning(f"Bronze DVJ mapping table missing: {fq_dvj}")

            return

        static_path = self.config.static_path

        # Load main static ZIP
        main_zip = find_best_static_file(
            static_path, target_date,
            file_prefix=f"{self.config.region}_static_",
        )
        if main_zip:
            self._static_data = self._parse_static_zip(main_zip)
            self._build_enrichment_lookups()
            logger.info(f"Loaded static data from {main_zip}")
        else:
            self._static_data = {}
            self._trip_enrichment = {}
            logger.warning("No static ZIP found — operating without static enrichment")

        # Load extra ZIP for DVJId mapping
        extra_zip = find_best_static_file(
            static_path, target_date,
            file_prefix=f"{self.config.region}_extra_",
        )
        if extra_zip:
            self._dvj_mapping = self._load_dvj_mapping(extra_zip)
            logger.info(
                f"Loaded DVJId mapping: {len(self._dvj_mapping):,} entries",
                extra={"row_count": len(self._dvj_mapping)},
            )
        else:
            self._dvj_mapping = {}
            logger.warning("No extra ZIP found — DVJId mapping unavailable")


    def _parse_static_zip(self, zip_path: str) -> dict[str, list[dict]]:
        """Parse a static GTFS ZIP into dict of filename -> list of row dicts."""
        result = {}
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith(".txt"):
                    with zf.open(name) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
                        result[name] = list(reader)
        return result


    def _build_enrichment_lookups_from_tables(
        self,
        routes_df: DataFrame,
        trips_df: DataFrame,
        stops_df: DataFrame,
        stop_times_df: DataFrame,
    ) -> None:
        """Build static enrichment lookups from bronze static Delta tables."""
        self._trip_enrichment = {}
        self._stop_lookup = {}

        for r in (
            stops_df.select(
                F.col("stop_id").cast("string").alias("stop_id"),
                F.col("stop_name").cast("string").alias("stop_name"),
                F.col("stop_lat").cast("double").alias("stop_lat"),
                F.col("stop_lon").cast("double").alias("stop_lon"),
            )
            .where(F.col("stop_id").isNotNull())
            .dropDuplicates(["stop_id"])
            .collect()
        ):
            self._stop_lookup[r["stop_id"]] = {
                "stop_name": r["stop_name"] or "",
                "stop_lat": float(r["stop_lat"]) if r["stop_lat"] is not None else None,
                "stop_lon": float(r["stop_lon"]) if r["stop_lon"] is not None else None,
            }

        route_rows = (
            routes_df.select(
                F.col("route_id").cast("string").alias("route_id"),
                F.col("route_short_name").cast("string").alias("route_short_name"),
                F.col("route_long_name").cast("string").alias("route_long_name"),
            )
            .where(F.col("route_id").isNotNull())
            .dropDuplicates(["route_id"])
            .collect()
        )
        route_short = {r["route_id"]: (r["route_short_name"] or None) for r in route_rows}
        route_long = {r["route_id"]: (r["route_long_name"] or None) for r in route_rows}

        st = (
            stop_times_df.select(
                F.col("trip_id").cast("string").alias("trip_id"),
                F.col("stop_id").cast("string").alias("stop_id"),
                F.col("stop_sequence").cast("int").alias("stop_sequence"),
                F.col("departure_time").cast("string").alias("departure_time"),
                F.col("arrival_time").cast("string").alias("arrival_time"),
            )
            .where(F.col("trip_id").isNotNull() & F.col("stop_sequence").isNotNull())
        )

        w_asc = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").asc())
        w_desc = Window.partitionBy("trip_id").orderBy(F.col("stop_sequence").desc())

        first_stop = (
            st.withColumn("_rn", F.row_number().over(w_asc))
            .where(F.col("_rn") == 1)
            .select(
                "trip_id",
                F.col("stop_id").alias("origin_stop_id"),
                F.coalesce(F.col("departure_time"), F.col("arrival_time")).alias("first_departure_time"),
            )
        )
        last_stop = (
            st.withColumn("_rn", F.row_number().over(w_desc))
            .where(F.col("_rn") == 1)
            .select("trip_id", F.col("stop_id").alias("end_stop_id"))
        )

        trip_stops = (
            first_stop.join(last_stop, "trip_id", "left")
            .join(
                stops_df.select(
                    F.col("stop_id").cast("string").alias("origin_stop_id"),
                    F.col("stop_name").cast("string").alias("origin_stop_name"),
                ).dropDuplicates(["origin_stop_id"]),
                "origin_stop_id",
                "left",
            )
            .join(
                stops_df.select(
                    F.col("stop_id").cast("string").alias("end_stop_id"),
                    F.col("stop_name").cast("string").alias("end_stop_name"),
                ).dropDuplicates(["end_stop_id"]),
                "end_stop_id",
                "left",
            )
            .select("trip_id", "first_departure_time", "origin_stop_name", "end_stop_name")
        )

        # Select available columns from trips_df (block_id may not exist in all GTFS feeds)
        trip_cols = [
            F.col("trip_id").cast("string").alias("trip_id"),
            F.col("route_id").cast("string").alias("route_id"),
            F.col("direction_id").cast("string").alias("direction_id"),
            F.col("trip_headsign").cast("string").alias("trip_headsign"),
            (F.col("block_id").cast("string") if "block_id" in trips_df.columns else F.lit(None).cast("string")).alias("block_id"),
            F.col("trip_short_name").cast("string").alias("trip_short_name"),
        ]
        enriched = (
            trips_df.select(*trip_cols)
            .where(F.col("trip_id").isNotNull())
            .join(trip_stops, "trip_id", "left")
        )

        for r in enriched.collect():
            trip_id = r["trip_id"]
            route_id = r["route_id"]
            direction_id_str = r["direction_id"] or ""
            headsign = r["trip_headsign"] or ""
            block_id = r["block_id"] or ""
            trip_short_name = r["trip_short_name"] or ""

            origin_name = r["origin_stop_name"] or None
            end_name = r["end_stop_name"] or None

            self._trip_enrichment[trip_id] = {
                "LineNumber": route_short.get(route_id),
                "DirectionNumber": (int(direction_id_str) if direction_id_str.isdigit() else None),
                "JourneyStartTime": r["first_departure_time"] or None,
                "DestinationName": headsign if headsign else None,
                "route_id": route_id,
                "ExtendedLineDesignation": route_long.get(route_id) or None,
                "BlockNumber": block_id if block_id else None,
                "TripShortName": trip_short_name if trip_short_name else None,
                "JourneyOriginName": origin_name,
                "JourneyEndStopName": end_name or (headsign if headsign else None),
            }

        logger.info(
            f"Built static enrichment lookups from bronze tables: {len(self._trip_enrichment):,} trips, "
            f"{len(route_short):,} routes, {len(self._stop_lookup):,} stops",
            extra={"enrichment_trips": len(self._trip_enrichment)},
        )


    def _load_dvj_mapping_from_table(self, dvj_df: DataFrame) -> dict[str, str]:
        """Load DVJ mapping from bronze table static_dvj_mapping."""
        mapping: dict[str, str] = {}
        df = dvj_df.select(
            F.col("trip_id").cast("string").alias("trip_id"),
            F.col("dated_vehicle_journey_gid").cast("string").alias("dated_vehicle_journey_gid"),
        ).where(F.col("trip_id").isNotNull() & F.col("dated_vehicle_journey_gid").isNotNull())

        for row in df.toLocalIterator():
            mapping[row["trip_id"]] = row["dated_vehicle_journey_gid"]

        return mapping


    def _build_enrichment_lookups(self) -> None:
        """Build trip_id → enrichment lookups from static GTFS data.

        Creates a dict mapping trip_id to (route_short_name, direction_id,
        first_departure_time, trip_headsign) from trips.txt, routes.txt,
        and stop_times.txt.
        """
        self._trip_enrichment: dict[str, dict] = {}
        self._stop_lookup: dict[str, dict] = {}

        if not self._static_data:
            return

        # Build route_id → route_short_name lookup from routes.txt
        route_lookup: dict[str, str] = {}
        for row in self._static_data.get("routes.txt", []):
            route_id = row.get("route_id", "")
            short_name = row.get("route_short_name", "")
            if route_id and short_name:
                route_lookup[route_id] = short_name

        # Build trip_id → first departure time from stop_times.txt
        first_departure: dict[str, str] = {}
        for row in self._static_data.get("stop_times.txt", []):
            tid = row.get("trip_id", "")
            dep = row.get("departure_time", "") or row.get("arrival_time", "")
            if tid and dep and tid not in first_departure:
                first_departure[tid] = dep

        # Build route_id → route_long_name lookup
        route_long_name_lookup: dict[str, str] = {}
        for row in self._static_data.get("routes.txt", []):
            route_id = row.get("route_id", "")
            long_name = row.get("route_long_name", "")
            if route_id:
                route_long_name_lookup[route_id] = long_name

        # Build stop_id → stop info lookup from stops.txt (BEFORE trip enrichment)
        for row in self._static_data.get("stops.txt", []):
            stop_id = row.get("stop_id", "").strip()
            if stop_id:
                try:
                    lat = float(row.get("stop_lat") or 0)
                    lon = float(row.get("stop_lon") or 0)
                except (ValueError, TypeError):
                    lat, lon = 0.0, 0.0
                self._stop_lookup[stop_id] = {
                    "stop_name": row.get("stop_name", ""),
                    "stop_lat": lat if lat != 0.0 else None,
                    "stop_lon": lon if lon != 0.0 else None,
                }

        # Build trip_id → list of stop_ids (ordered by stop_sequence) for origin/end
        trip_stop_ids: dict[str, list[tuple[int, str]]] = {}
        for row in self._static_data.get("stop_times.txt", []):
            tid = row.get("trip_id", "")
            sid = row.get("stop_id", "")
            seq_str = row.get("stop_sequence", "0")
            seq = int(seq_str) if seq_str.isdigit() else 0
            if tid and sid:
                trip_stop_ids.setdefault(tid, []).append((seq, sid))
        # Sort each trip's stops by sequence
        for tid in trip_stop_ids:
            trip_stop_ids[tid].sort(key=lambda x: x[0])

        # Build trip_id → enrichment dict from trips.txt
        for row in self._static_data.get("trips.txt", []):
            trip_id = row.get("trip_id", "")
            if not trip_id:
                continue

            route_id = row.get("route_id", "")
            direction_id_str = row.get("direction_id", "")
            headsign = row.get("trip_headsign", "")
            block_id = row.get("block_id", "")
            trip_short_name = row.get("trip_short_name", "")

            # Derive origin/end stop names from stop_times + stops lookup
            origin_name = None
            end_name = None
            stop_list = trip_stop_ids.get(trip_id, [])
            if stop_list:
                first_sid = stop_list[0][1]
                last_sid = stop_list[-1][1]
                origin_info = self._stop_lookup.get(first_sid, {})
                end_info = self._stop_lookup.get(last_sid, {})
                origin_name = origin_info.get("stop_name") or None
                end_name = end_info.get("stop_name") or None

            self._trip_enrichment[trip_id] = {
                "LineNumber": route_lookup.get(route_id),
                "DirectionNumber": (
                    int(direction_id_str) if direction_id_str.isdigit() else None
                ),
                "JourneyStartTime": first_departure.get(trip_id),
                "DestinationName": headsign if headsign else None,
                # New enrichment fields
                "route_id": route_id,
                "ExtendedLineDesignation": route_long_name_lookup.get(route_id) or None,
                "BlockNumber": block_id if block_id else None,
                "TripShortName": trip_short_name if trip_short_name else None,
                "JourneyOriginName": origin_name,
                "JourneyEndStopName": end_name or (headsign if headsign else None),
            }


        logger.info(
            f"Built static enrichment lookups: {len(self._trip_enrichment):,} trips, "
            f"{len(route_lookup):,} routes, {len(self._stop_lookup):,} stops",
            extra={"enrichment_trips": len(self._trip_enrichment)},
        )


    def _load_dvj_mapping(self, zip_path: str) -> dict[str, str]:
        """Load trips_dated_vehicle_journey.txt into a trip_id -> DVJId dict.

        This is ~2.4M rows. Loaded on the driver as a Python dict for
        O(1) lookups during protobuf parsing.
        """
        mapping: dict[str, str] = {}
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                with zf.open("trips_dated_vehicle_journey.txt") as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
                    for row in reader:
                        trip_id = row.get("trip_id", "").strip()
                        dvj_id = row.get("dated_vehicle_journey_gid", "").strip()
                        if trip_id and dvj_id:
                            mapping[trip_id] = dvj_id
        except (KeyError, zipfile.BadZipFile) as e:
            logger.error(f"Failed to load DVJId mapping: {e}")

        return mapping

    # ================================================================
    # REALTIME PARSING
    # ================================================================

    def parse_realtime_files(
        self,
        file_sets: list[RealtimeFileSet],
    ) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """Parse protobuf files and create base Journey + JourneyCall DataFrames.

        Also extracts raw VehiclePositions for Phase 2 GPS tables and
        builds a stops DataFrame from static data.

        Args:
            file_sets: List of complete realtime file sets to process.

        Returns:
            Tuple of (journey_df, journey_call_df, vp_df, stops_df).
        """
        from google.transit import gtfs_realtime_pb2

        all_journeys: list[dict[str, Any]] = []
        all_calls: list[dict[str, Any]] = []
        all_vp: list[dict[str, Any]] = []

        for file_set in file_sets:
            if not file_set.is_complete:
                logger.warning(f"Skipping incomplete file set: {file_set.timestamp}")
                continue

            # Parse TripUpdates (primary source for Journey + JourneyCall)
            tu_path = file_set.files.get("TripUpdates.pb")
            if tu_path:
                tu_feed = gtfs_realtime_pb2.FeedMessage()
                with open(tu_path, "rb") as f:
                    tu_feed.ParseFromString(f.read())
                j, c = self._parse_trip_updates(tu_feed, file_set.timestamp)
                all_journeys.extend(j)
                all_calls.extend(c)

            # Parse VehiclePositions (enriches with GPS, vehicle info)
            vp_path = file_set.files.get("VehiclePositions.pb")
            if vp_path:
                vp_feed = gtfs_realtime_pb2.FeedMessage()
                with open(vp_path, "rb") as f:
                    vp_feed.ParseFromString(f.read())
                self._enrich_from_vehicle_positions(vp_feed, all_journeys, all_calls)
                # Also extract raw VP data for Phase 2 GPS tables
                all_vp.extend(self._extract_vehicle_positions(vp_feed))

            # Parse ServiceAlerts (cancellations, deviations)
            sa_path = file_set.files.get("ServiceAlerts.pb")
            if sa_path:
                sa_feed = gtfs_realtime_pb2.FeedMessage()
                with open(sa_path, "rb") as f:
                    sa_feed.ParseFromString(f.read())
                self._enrich_from_service_alerts(sa_feed, all_journeys, all_calls)

        # Convert to Spark DataFrames with EXPLICIT schemas (no inference)
        if all_journeys:
            normalized = [self._normalize_journey(j) for j in all_journeys]
            journey_df = self.spark.createDataFrame(normalized, schema=JOURNEY_SCHEMA)
        else:
            journey_df = self.spark.createDataFrame([], schema=JOURNEY_SCHEMA)

        if all_calls:
            normalized = [self._normalize_call(c) for c in all_calls]
            call_df = self.spark.createDataFrame(normalized, schema=CALL_SCHEMA)
        else:
            call_df = self.spark.createDataFrame([], schema=CALL_SCHEMA)

        # Build VehiclePositions DataFrame for Phase 2 GPS tables
        if all_vp:
            vp_df = self.spark.createDataFrame(all_vp, schema=VP_SCHEMA)
        else:
            vp_df = self.spark.createDataFrame([], schema=VP_SCHEMA)

        # Build stops DataFrame from static data
        stops_df = self._build_stops_df()

        logger.info(
            f"Parsed {len(all_journeys)} journeys, {len(all_calls)} calls, "
            f"{len(all_vp)} vehicle positions from {len(file_sets)} file sets",
            extra={"row_count": len(all_journeys) + len(all_calls)},
        )

        return journey_df, call_df, vp_df, stops_df

    def _extract_vehicle_positions(
        self,
        feed: Any,
    ) -> list[dict[str, Any]]:
        """Extract ALL vehicle positions from a VehiclePositions feed.

        Returns a list of dicts with: DVJId, trip_id, vehicle_id,
        latitude, longitude, bearing, speed, timestamp, OperatingDate.
        Used by Phase 2 GPS table builders.
        """
        positions: list[dict[str, Any]] = []
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            trip_id = vp.trip.trip_id if vp.trip.trip_id else None
            if not trip_id:
                continue

            dvj_id = self._dvj_mapping.get(trip_id)

            # Derive OperatingDate from the trip's start_date or timestamp
            op_date = None
            if vp.trip.start_date:
                raw = vp.trip.start_date
                op_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}" if len(raw) == 8 else raw
            elif vp.timestamp:
                dt = datetime.fromtimestamp(vp.timestamp, tz=timezone.utc)
                op_date = dt.strftime("%Y-%m-%d")

            lat = float(vp.position.latitude) if vp.HasField("position") else None
            lon = float(vp.position.longitude) if vp.HasField("position") else None
            bearing = float(vp.position.bearing) if vp.HasField("position") and vp.position.bearing else None
            speed_val = float(vp.position.speed) if vp.HasField("position") and vp.position.speed else None
            # Convert speed from m/s to kph
            speed_kph = round(speed_val * 3.6, 1) if speed_val is not None else None

            positions.append({
                "DVJId": dvj_id,
                "trip_id": trip_id,
                "vehicle_id": vp.vehicle.id if vp.vehicle.HasField("id") else None,
                "latitude": lat,
                "longitude": lon,
                "bearing": bearing,
                "speed": speed_kph,
                "timestamp": int(vp.timestamp) if vp.timestamp else None,
                "OperatingDate": op_date,
            })

        return positions

    def _build_stops_df(self) -> DataFrame:
        """Build a Spark DataFrame of stop coordinates from static data.

        Used for Phase 2 StopPoint GPS analysis tables.
        """
        stops_rows = self._static_data.get("stops.txt", [])
        if not stops_rows:
            return self.spark.createDataFrame([], schema=STOPS_SCHEMA)

        cleaned = []
        for row in stops_rows:
            try:
                lat = float(row.get("stop_lat", 0))
                lon = float(row.get("stop_lon", 0))
                if lat == 0 and lon == 0:
                    continue
                cleaned.append({
                    "stop_id": row.get("stop_id", ""),
                    "stop_name": row.get("stop_name", ""),
                    "stop_lat": lat,
                    "stop_lon": lon,
                })
            except (ValueError, TypeError):
                continue

        if cleaned:
            return self.spark.createDataFrame(cleaned, schema=STOPS_SCHEMA)
        return self.spark.createDataFrame([], schema=STOPS_SCHEMA)

    # ================================================================
    # STATIC DATAFRAME BUILDERS (for ViewBuilder planned/observed paths)
    # ================================================================

    def build_static_spark_dfs(
        self, target_date: date,
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        """Build shapes, trips, stop_times as Spark DataFrames from static ZIP.

        Extracts files to the volume's extracted/ directory and reads
        via spark.read.csv for efficient distributed processing.
        Does NOT require load_static_data() to be called first.

        Returns:
            Tuple of (shapes_df, trips_df, stop_times_df).
        """
        SHAPES_SCHEMA = T.StructType([
            T.StructField("shape_id", T.StringType(), True),
            T.StructField("shape_pt_lat", T.DoubleType(), True),
            T.StructField("shape_pt_lon", T.DoubleType(), True),
            T.StructField("shape_pt_sequence", T.IntegerType(), True),
            T.StructField("shape_dist_traveled", T.DoubleType(), True),
        ])
        TRIPS_SCHEMA = T.StructType([
            T.StructField("route_id", T.StringType(), True),
            T.StructField("service_id", T.StringType(), True),
            T.StructField("trip_id", T.StringType(), True),
            T.StructField("trip_headsign", T.StringType(), True),
            T.StructField("trip_short_name", T.StringType(), True),
            T.StructField("direction_id", T.IntegerType(), True),
            T.StructField("shape_id", T.StringType(), True),
        ])
        STOP_TIMES_SCHEMA = T.StructType([
            T.StructField("trip_id", T.StringType(), True),
            T.StructField("arrival_time", T.StringType(), True),
            T.StructField("departure_time", T.StringType(), True),
            T.StructField("stop_id", T.StringType(), True),
            T.StructField("stop_sequence", T.IntegerType(), True),
            T.StructField("stop_headsign", T.StringType(), True),
            T.StructField("pickup_type", T.IntegerType(), True),
            T.StructField("drop_off_type", T.IntegerType(), True),
            T.StructField("shape_dist_traveled", T.DoubleType(), True),
            T.StructField("timepoint", T.IntegerType(), True),
        ])

        empty = (
            self.spark.createDataFrame([], SHAPES_SCHEMA),
            self.spark.createDataFrame([], TRIPS_SCHEMA),
            self.spark.createDataFrame([], STOP_TIMES_SCHEMA),
        )

        static_path = self.config.static_path
        main_zip = find_best_static_file(
            static_path, target_date,
            file_prefix=f"{self.config.region}_static_",
        )

        if not main_zip:
            logger.warning("No static ZIP found — returning empty static DataFrames")
            return empty

        # Extract needed files to volume extracted/ directory
        zip_name = os.path.basename(main_zip).replace(".zip", "")
        extract_dir = os.path.join(static_path, "extracted", zip_name)
        os.makedirs(extract_dir, exist_ok=True)

        needed_files = ["shapes.txt", "trips.txt", "stop_times.txt"]
        with zipfile.ZipFile(main_zip, "r") as zf:
            for name in needed_files:
                target_path = os.path.join(extract_dir, name)
                if not os.path.exists(target_path):
                    zf.extract(name, extract_dir)
                    logger.info(f"Extracted {name} to {extract_dir}")

        shapes_df = self.spark.read.csv(
            os.path.join(extract_dir, "shapes.txt"),
            header=True, schema=SHAPES_SCHEMA,
        )
        trips_df = self.spark.read.csv(
            os.path.join(extract_dir, "trips.txt"),
            header=True, schema=TRIPS_SCHEMA,
        )
        stop_times_df = self.spark.read.csv(
            os.path.join(extract_dir, "stop_times.txt"),
            header=True, schema=STOP_TIMES_SCHEMA,
        )

        logger.info(
            f"Built static Spark DataFrames from {os.path.basename(main_zip)}: "
            f"shapes, trips, stop_times"
        )
        return shapes_df, trips_df, stop_times_df


    # ================================================================
    # PROTOBUF PARSING METHODS
    # ================================================================

    def _parse_trip_updates(
        self,
        feed: Any,  # gtfs_realtime_pb2.FeedMessage
        timestamp: str,
    ) -> tuple[list[dict], list[dict]]:
        """Extract journey and call records from a TripUpdates feed."""
        journeys: list[dict] = []
        calls: list[dict] = []

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            tu = entity.trip_update
            trip = tu.trip
            trip_id = trip.trip_id if trip.trip_id else None

            # Resolve DVJId from static mapping (trips_dated_vehicle_journey.txt).
            # This mapping is expected to exist via the daily static "extra" ZIP.
            dvj_id = (
                (self._dvj_mapping.get(trip_id) if self._dvj_mapping else None)
                if trip_id
                else None
            )

            # Static enrichment fallback for fields missing from GTFS-RT
            enrich = self._trip_enrichment.get(trip_id, {}) if self._trip_enrichment else {}

            # Derive OperatingWeekDay from OperatingDate (e.g. "Thursday")
            op_date_raw = trip.start_date if trip.start_date else None
            op_weekday = None
            op_date_fmt = None  # YYYY-MM-DD format
            if op_date_raw and len(op_date_raw) == 8:
                try:
                    dt = datetime.strptime(op_date_raw, "%Y%m%d")
                    op_weekday = dt.strftime("%A")
                    op_date_fmt = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # Common enrichment values
            route_id = trip.route_id if trip.route_id else enrich.get("route_id")
            line_number = trip.route_id if trip.route_id else enrich.get("LineNumber")
            direction_number = (
                enrich.get("DirectionNumber")
                if enrich.get("DirectionNumber") is not None
                else (trip.direction_id if trip.direction_id is not None else None)
            )
            vehicle_label = tu.vehicle.label if tu.vehicle.HasField("label") else None
            vehicle_gid = tu.vehicle.id if tu.vehicle.HasField("id") else None
            vehicle_id = vehicle_label or vehicle_gid  # Use label, fallback to id
            start_time = trip.start_time if trip.start_time else enrich.get("JourneyStartTime")
            base_timestamp = tu.timestamp if tu.timestamp else None

            # Journey-level derived fields (matching Synapse)
            journey_start_datetime = None
            if start_time and op_date_fmt:
                journey_start_datetime = f"{op_date_fmt} {start_time}"

            vehicle_assigned_dt = None
            if base_timestamp:
                vehicle_assigned_dt = datetime.fromtimestamp(
                    base_timestamp, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")

            num_stops = len(tu.stop_time_update)

            # JourneyNumber: prefer static trip_short_name, fallback to trip_id.
            # Note: VP + ServiceAlerts matching uses GTFS-RT trip_id; store that
            # in JourneyGid so enrichment can reliably join even when a public
            # short-name is present (or absent).
            journey_number = enrich.get("TripShortName") or trip_id

            journey_gid = trip_id or dvj_id
            reinforced_dvj_id = trip_id or dvj_id
            uses_jpp_id = f"1{journey_gid}" if journey_gid else None
            # ExtendedLineDesignation: use None when blank, not empty string
            extended_line = enrich.get("ExtendedLineDesignation") or None

            # Build journey record
            journey = {
                "DVJId": dvj_id,
                "OperatingDate": op_date_fmt or op_date_raw,
                "OperatingWeekDay": op_weekday,
                "JourneyNumber": journey_number,
                "LineNumber": line_number,
                "DirectionNumber": direction_number,
                "VehicleNumber": vehicle_id,
                "JourneyStartTime": start_time,
                "DestinationName": enrich.get("DestinationName"),
                "MonitoredYesNo": "Yes",
                "RunByVehicleGid": vehicle_gid,
                # New enrichment fields
                "ExtendedLineDesignation": extended_line,
                "JourneyOriginName": enrich.get("JourneyOriginName"),
                "JourneyEndStopName": enrich.get("JourneyEndStopName"),
                "BlockNumber": enrich.get("BlockNumber"),
                "StartMunicipalityName": None,  # No source in GTFS (Synapse also often None)
                "VehicleAssignedFromDateTime": vehicle_assigned_dt,
                "VehicleAssignedUptoDateTime": vehicle_assigned_dt,
                "InProgressFromDateTime": journey_start_datetime,
                "InProgressUptoDateTime": journey_start_datetime,
                "AssignedDriverGid": vehicle_gid,
                "JourneyStartDateTime": journey_start_datetime,
                "JourneyLastStopSequenceNumber": num_stops,
                "ReinforcedDVJId": reinforced_dvj_id,
                "UsesJourneyPatternId": uses_jpp_id,
                "JourneyGid": journey_gid,
                "_is_cancelled": False,
                "_source_timestamp": timestamp,
            }
            journeys.append(journey)

            # Build call records
            previous_jpp_gid = None
            for idx, stu in enumerate(tu.stop_time_update, start=1):
                stop_id = stu.stop_id if stu.stop_id else None
                stop_seq = stu.stop_sequence if stu.stop_sequence else idx
                # Enrich from stops.txt
                stop_info = self._stop_lookup.get(stop_id, {}) if stop_id else {}
                coord_lat = stop_info.get("stop_lat")   # already float or None
                coord_lon = stop_info.get("stop_lon")   # already float or None

                # Observed/timetabled times
                obs_arr_dt = None
                obs_dep_dt = None
                obs_arr_time = None
                obs_dep_time = None
                arr_delay = None
                dep_delay = None
                tt_arr_time = None
                tt_dep_time = None
                tt_arr_dt = None
                tt_dep_dt = None

                if stu.HasField("arrival"):
                    if stu.arrival.HasField("delay"):
                        arr_delay = stu.arrival.delay
                    if stu.arrival.HasField("time"):
                        arr_dt = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
                        obs_arr_dt = arr_dt.isoformat()
                        obs_arr_time = arr_dt.strftime("%H:%M:%S")
                        if arr_delay is not None:
                            tt_arr_obj = datetime.fromtimestamp(
                                stu.arrival.time - arr_delay, tz=timezone.utc
                            )
                            tt_arr_time = tt_arr_obj.strftime("%H:%M:%S")
                            tt_arr_dt = tt_arr_obj.strftime("%Y-%m-%d %H:%M:%S")

                if stu.HasField("departure"):
                    if stu.departure.HasField("delay"):
                        dep_delay = stu.departure.delay
                    if stu.departure.HasField("time"):
                        dep_dt = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
                        obs_dep_dt = dep_dt.isoformat()
                        obs_dep_time = dep_dt.strftime("%H:%M:%S")
                        if dep_delay is not None:
                            tt_dep_obj = datetime.fromtimestamp(
                                stu.departure.time - dep_delay, tz=timezone.utc
                            )
                            tt_dep_time = tt_dep_obj.strftime("%H:%M:%S")
                            tt_dep_dt = tt_dep_obj.strftime("%Y-%m-%d %H:%M:%S")

                # State derivation
                arr_state = "ARRIVED" if arr_delay is not None else "SCHEDULED"
                dep_state = "DEPARTED" if dep_delay is not None else "SCHEDULED"
                arr_state_num = {"SCHEDULED": 1, "ARRIVED": 6}.get(arr_state, 1)
                dep_state_num = {"SCHEDULED": 1, "DEPARTED": 9}.get(dep_state, 1)

                jpp_gid = f"JPP_{stop_id}_{stop_seq}" if stop_id else None

                call = {
                    "DVJId": dvj_id,
                    "OperatingDate": op_date_fmt or op_date_raw,
                    "OperatingWeekDay": op_weekday,
                    "SequenceNumber": stop_seq,
                    "StopAreaNumber": stop_id,
                    "StopName": stop_info.get("stop_name") or None,
                    "JourneyNumber": journey_number,
                    "LineNumber": line_number,
                    "DirectionNumber": direction_number,
                    "ArrivalDelaySeconds": arr_delay,
                    "ObservedArrivalDateTime": obs_arr_dt,
                    "DepartureDelaySeconds": dep_delay,
                    "ObservedDepartureDateTime": obs_dep_dt,
                    # Journey-level propagation
                    "DefaultTransportModeCode": "BUS",
                    "TransportAuthorityNumber": 12,
                    "ExtendedLineDesignation": extended_line,
                    "JourneyStartTime": start_time,
                    "DatedJourneyStateName": "IN_PROGRESS",
                    "JourneyOriginName": enrich.get("JourneyOriginName"),
                    "JourneyEndStopName": enrich.get("JourneyEndStopName"),
                    "OperatorName": "Skånetrafiken",
                    "BlockNumber": enrich.get("BlockNumber"),
                    "VehicleNumber": vehicle_id,
                    "StartMunicipalityName": None,
                    "DestinationName": enrich.get("DestinationName"),
                    "VehicleAssignedFromDateTime": vehicle_assigned_dt,
                    "VehicleAssignedUptoDateTime": vehicle_assigned_dt,
                    "RunByVehicleGid": vehicle_gid,
                    "InProgressFromDateTime": journey_start_datetime,
                    "InProgressUptoDateTime": journey_start_datetime,
                    "AssignedDriverGid": vehicle_gid,
                    "JourneyStartDateTime": journey_start_datetime,
                    "JourneyLastStopSequenceNumber": num_stops,
                    "ReinforcedDVJId": reinforced_dvj_id,
                    "UsesJourneyPatternId": uses_jpp_id,
                    "JourneyGid": journey_gid,
                    # Call-specific enrichment
                    "MunicipalityName": "Skåne",
                    "TimingPointYesNo": 0,
                    "ArrivalStateName": arr_state,
                    "TimetabledLatestArrivalTime": tt_arr_time,
                    "ObservedArrivalTime": obs_arr_time,
                    "DepartureStateName": dep_state,
                    "TimetabledEarliestDepartureTime": tt_dep_time,
                    "ObservedDepartureTime": obs_dep_time,
                    "TimetabledEarliestDepartureHour": (
                        tt_dep_time.split(":")[0] if tt_dep_time else None
                    ),
                    "TimetabledEarliestDepartureCalendarWeekDay": op_weekday,
                    "StopPointLocalNumber": stop_seq,
                    "JourneyPatternPointNumber": stop_seq,
                    "TimetabledLatestArrivalDateTime": tt_arr_dt,
                    "TargetArrivalDateTime": obs_arr_dt,
                    "TimetabledEarliestDepartureDateTime": tt_dep_dt,
                    "TargetDepartureDateTime": obs_dep_dt,
                    "DistanceToNextStop": None,
                    "DistanceFromPreviousStop": None,
                    "Coord_Latitude": coord_lat,
                    "Coord_Longitude": coord_lon,
                    "ArrivalStateNumber": arr_state_num,
                    "DepartureStateNumber": dep_state_num,
                    "JPPGid": jpp_gid,
                    "PreviousJPPGid": previous_jpp_gid,
                    "StopAreaGid": f"SA_{stop_id}" if stop_id else None,
                    "_source_timestamp": timestamp,
                }

                calls.append(call)
                previous_jpp_gid = jpp_gid

        return journeys, calls

    def _enrich_from_vehicle_positions(
        self,
        feed: Any,
        journeys: list[dict],
        calls: list[dict],
    ) -> None:
        """Enrich journey/call data with GPS coordinates and vehicle info.

        Computes MIN/MAX vp.timestamp per trip_id for InProgress and
        VehicleAssigned datetime fields, matching the Synapse spec.
        """
        # Build lookups from the VehiclePositions feed.
        #
        # NOTE: Some producers omit TripDescriptor.trip_id in TripUpdates but
        # include it in VehiclePositions. When that happens JourneyGid can be
        # backfilled by matching on vehicle id/label within the same snapshot.
        #
        # Accumulate min/max timestamps across all VP entities for each trip.
        vp_lookup: dict[str, dict] = {}
        vp_ts_min: dict[str, int] = {}
        vp_ts_max: dict[str, int] = {}
        vehicle_to_trip: dict[str, str] = {}

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            trip_id = vp.trip.trip_id if vp.trip.trip_id else None
            if not trip_id:
                continue

            # Track min/max timestamp for InProgress/VehicleAssigned fields
            ts = int(vp.timestamp) if vp.timestamp else None
            if ts is not None:
                if trip_id not in vp_ts_min or ts < vp_ts_min[trip_id]:
                    vp_ts_min[trip_id] = ts
                if trip_id not in vp_ts_max or ts > vp_ts_max[trip_id]:
                    vp_ts_max[trip_id] = ts

            # Last-seen vehicle position wins for lat/lon and vehicle IDs
            vehicle_label = vp.vehicle.label if vp.vehicle.HasField("label") else None
            vehicle_gid = vp.vehicle.id if vp.vehicle.HasField("id") else None
            if vehicle_gid:
                vehicle_to_trip[vehicle_gid] = trip_id
            if vehicle_label:
                vehicle_to_trip[vehicle_label] = trip_id

            vp_lookup[trip_id] = {
                "Coord_Latitude": vp.position.latitude if vp.HasField("position") else None,
                "Coord_Longitude": vp.position.longitude if vp.HasField("position") else None,
                "VehicleNumber": vehicle_label,
                "RunByVehicleGid": vehicle_gid,
                "AssignedDriverGid": vehicle_gid,
            }

        # Enrich journeys
        for journey in journeys:
            # Prefer the GTFS-RT/static trip_id fields for matching.
            trip_id = journey.get("JourneyGid") or journey.get("ReinforcedDVJId")

            # If JourneyGid is missing (common when TripUpdates omit trip_id),
            # backfill it using the vehicle id/label from VehiclePositions.
            if not journey.get("JourneyGid"):
                vehicle_key = (
                    journey.get("RunByVehicleGid")
                    or journey.get("AssignedDriverGid")
                    or journey.get("VehicleNumber")
                )
                if vehicle_key:
                    inferred = vehicle_to_trip.get(vehicle_key)
                    if inferred:
                        journey["JourneyGid"] = inferred
                        journey["ReinforcedDVJId"] = journey.get("ReinforcedDVJId") or inferred
                        trip_id = inferred

                        if journey.get("DVJId") is None and self._dvj_mapping:
                            journey["DVJId"] = self._dvj_mapping.get(inferred)

                        enrich = self._trip_enrichment.get(inferred, {}) if self._trip_enrichment else {}
                        for k in (
                            "LineNumber",
                            "ExtendedLineDesignation",
                            "JourneyStartTime",
                            "DestinationName",
                            "JourneyOriginName",
                            "JourneyEndStopName",
                            "BlockNumber",
                            "JourneyStartDateTime",
                        ):
                            if journey.get(k) is None and enrich.get(k) is not None:
                                journey[k] = enrich.get(k)

            # As a last resort, allow matching on JourneyNumber when it equals trip_id.
            if not trip_id:
                trip_id = journey.get("JourneyNumber")

            if not trip_id:
                continue

            if trip_id in vp_lookup:
                vp = vp_lookup[trip_id]
                for key, val in vp.items():
                    if val is not None and journey.get(key) is None:
                        journey[key] = val

            # Set InProgress and VehicleAssigned from MIN/MAX VP timestamps
            if trip_id in vp_ts_min:
                dt_from = datetime.fromtimestamp(
                    vp_ts_min[trip_id], tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                dt_upto = datetime.fromtimestamp(
                    vp_ts_max[trip_id], tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                journey["InProgressFromDateTime"] = dt_from
                journey["InProgressUptoDateTime"] = dt_upto
                journey["VehicleAssignedFromDateTime"] = dt_from
                journey["VehicleAssignedUptoDateTime"] = dt_upto

        # Enrich calls (primarily for downstream call-level GPS/APC builders).
        for call in calls:
            trip_id = call.get("JourneyGid") or call.get("ReinforcedDVJId")

            if not call.get("JourneyGid"):
                vehicle_key = (
                    call.get("RunByVehicleGid")
                    or call.get("AssignedDriverGid")
                    or call.get("VehicleNumber")
                )
                if vehicle_key:
                    inferred = vehicle_to_trip.get(vehicle_key)
                    if inferred:
                        call["JourneyGid"] = inferred
                        call["ReinforcedDVJId"] = call.get("ReinforcedDVJId") or inferred
                        trip_id = inferred
                        if call.get("DVJId") is None and self._dvj_mapping:
                            call["DVJId"] = self._dvj_mapping.get(inferred)

            if not trip_id:
                trip_id = call.get("JourneyNumber")

            if not trip_id:
                continue


    def _enrich_from_service_alerts(
        self,
        feed: Any,
        journeys: list[dict],
        calls: list[dict],
    ) -> None:
        """Enrich data with cancellation and deviation info from ServiceAlerts."""
        # Build trip_id -> alert lookup
        alerts: dict[str, list[dict]] = {}
        for entity in feed.entity:
            if not entity.HasField("alert"):
                continue
            alert = entity.alert
            for informed in alert.informed_entity:
                trip_id = informed.trip.trip_id if informed.HasField("trip") else None
                if trip_id:
                    if trip_id not in alerts:
                        alerts[trip_id] = []
                    alerts[trip_id].append({
                        "cause": alert.cause if alert.cause else None,
                        "effect": alert.effect if alert.effect else None,
                        "header": (
                            alert.header_text.translation[0].text
                            if alert.header_text.translation
                            else None
                        ),
                    })

        # Flag cancelled journeys
        for journey in journeys:
            trip_id = (
                journey.get("JourneyGid")
                or journey.get("ReinforcedDVJId")
                or journey.get("JourneyNumber")
            )
            if trip_id and trip_id in alerts:
                for a in alerts[trip_id]:
                    # GTFS-RT Effect.NO_SERVICE = cancelled
                    if a.get("effect") == 1:  # NO_SERVICE enum
                        journey["_is_cancelled"] = True
                        break

    # ================================================================
    # STATIC ENRICHMENT
    # ================================================================

    def _normalize_journey(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Ensure all journey schema fields are present with correct defaults."""
        return {f.name: raw.get(f.name) for f in JOURNEY_SCHEMA.fields}

    def _normalize_call(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Ensure all call schema fields are present with correct defaults."""
        return {f.name: raw.get(f.name) for f in CALL_SCHEMA.fields}