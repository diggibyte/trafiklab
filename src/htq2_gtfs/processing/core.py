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

        Reads from ZIP files in the Volume. The DVJId mapping from
        trips_dated_vehicle_journey.txt is loaded into a Python dict
        for fast O(1) lookups during protobuf parsing.
        """
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
                self._stop_lookup[stop_id] = {
                    "stop_name": row.get("stop_name", ""),
                    "stop_lat": row.get("stop_lat"),
                    "stop_lon": row.get("stop_lon"),
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
                "ExtendedLineDesignation": route_long_name_lookup.get(route_id, ""),
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
            trip_id = trip.trip_id

            # Resolve DVJId from static mapping
            dvj_id = self._dvj_mapping.get(trip_id) if self._dvj_mapping else None

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
                trip.direction_id if trip.HasField("direction_id")
                else enrich.get("DirectionNumber")
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

            # Build journey record
            journey = {
                "DVJId": dvj_id,
                "OperatingDate": op_date_raw,
                "OperatingWeekDay": op_weekday,
                "JourneyNumber": trip_id,
                "LineNumber": line_number,
                "DirectionNumber": direction_number,
                "VehicleNumber": vehicle_id,
                "JourneyStartTime": start_time,
                "DestinationName": enrich.get("DestinationName"),
                "MonitoredYesNo": "Yes",
                "RunByVehicleGid": vehicle_gid,
                # New enrichment fields
                "ExtendedLineDesignation": enrich.get("ExtendedLineDesignation"),
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
                "ReinforcedDVJId": trip_id,
                "UsesJourneyPatternId": f"1{trip_id}" if trip_id else None,
                "JourneyGid": trip_id,
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
                coord_lat = None
                coord_lon = None
                try:
                    if stop_info.get("stop_lat"):
                        coord_lat = float(stop_info["stop_lat"])
                    if stop_info.get("stop_lon"):
                        coord_lon = float(stop_info["stop_lon"])
                except (ValueError, TypeError):
                    pass

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
                    "OperatingDate": op_date_raw,
                    "OperatingWeekDay": op_weekday,
                    "SequenceNumber": stop_seq,
                    "StopAreaNumber": stop_id,
                    "StopName": stop_info.get("stop_name") or None,
                    "JourneyNumber": trip_id,
                    "LineNumber": line_number,
                    "DirectionNumber": direction_number,
                    "ArrivalDelaySeconds": arr_delay,
                    "ObservedArrivalDateTime": obs_arr_dt,
                    "DepartureDelaySeconds": dep_delay,
                    "ObservedDepartureDateTime": obs_dep_dt,
                    # Journey-level propagation
                    "DefaultTransportModeCode": "BUS",
                    "TransportAuthorityNumber": 12,
                    "ExtendedLineDesignation": enrich.get("ExtendedLineDesignation"),
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
                    "ReinforcedDVJId": trip_id,
                    "UsesJourneyPatternId": f"1{trip_id}" if trip_id else None,
                    "JourneyGid": trip_id,
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
        """Enrich journey/call data with GPS coordinates and vehicle info."""
        # Build a trip_id -> vehicle position lookup
        vp_lookup: dict[str, dict] = {}
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            trip_id = vp.trip.trip_id if vp.trip.trip_id else None
            if trip_id:
                vp_lookup[trip_id] = {
                    "Coord_Latitude": vp.position.latitude if vp.HasField("position") else None,
                    "Coord_Longitude": vp.position.longitude if vp.HasField("position") else None,
                    "VehicleNumber": vp.vehicle.label if vp.vehicle.HasField("label") else None,
                    "RunByVehicleGid": vp.vehicle.id if vp.vehicle.HasField("id") else None,
                }

        # Enrich journeys
        for journey in journeys:
            trip_id = journey.get("JourneyNumber")
            if trip_id and trip_id in vp_lookup:
                vp = vp_lookup[trip_id]
                for key, val in vp.items():
                    if val is not None and (journey.get(key) is None):
                        journey[key] = val

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
            trip_id = journey.get("JourneyNumber")
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

