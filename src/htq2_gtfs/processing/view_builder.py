"""Normalized View Builder — Builds all 22 Delta tables from base DataFrames.

Port of Synapse GTFSViewBuilderNormalized. Key difference:
all calculations are pure PySpark (no pandas).

Architecture:
- 2 base tables: Journey, JourneyCall
- 14 extension tables: extract specific calculated columns
- 6 independent tables: JourneyLink and StopPoint analyses
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.helpers.spark_helpers import (
    delay_minutes_col,
    delay_seconds_col,
    detection_completeness_col,
    detection_completeness_state_col,
    haversine_col,
    is_first_stop_col,
    is_last_stop_col,
    punctuality_code_col,
    punctuality_col,
    speed_kph_col,
)

logger = get_logger(__name__)


class ViewBuilder:
    """Builds all 22 normalized tables from base Journey + JourneyCall DataFrames.

    Usage:
        builder = ViewBuilder(spark)
        tables = builder.build_all(journey_df, journey_call_df, vp_df, stops_df)
        # tables is a dict of table_name -> DataFrame
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def build_all(
        self,
        journey_df: DataFrame,
        journey_call_df: DataFrame,
        vp_df: DataFrame = None,
        stops_df: DataFrame = None,
    ) -> dict[str, DataFrame]:
        """Build all 22 tables. Returns dict of table_name -> DataFrame.

        Args:
            journey_df: Base journey DataFrame.
            journey_call_df: Base journey call DataFrame.
            vp_df: VehiclePositions GPS DataFrame (Phase 2).
            stops_df: Static stops DataFrame with coordinates (Phase 2).
        """
        tables: dict[str, DataFrame] = {}

        # Cache base DataFrames (read by multiple builders)
        journey_df.cache()
        journey_call_df.cache()

        try:
            # Base tables (pass through with dedup)
            tables["journey"] = self._build_journey(journey_df)
            tables["journey_call"] = self._build_journey_call(journey_call_df)

            # JourneyCall extensions
            tables["journey_call_apc"] = self._build_call_apc(journey_call_df)
            tables["journey_call_delay"] = self._build_call_delay(journey_call_df)
            tables["journey_call_statistics"] = self._build_call_statistics(journey_call_df)
            tables["journey_call_stop_and_link_duration"] = self._build_call_stop_link_duration(journey_call_df)
            tables["journey_call_cancelled_or_journey"] = self._build_call_cancelled(journey_call_df, journey_df)
            tables["journey_call_deviation_on"] = self._build_call_deviation(journey_call_df)
            tables["journey_call_missing_passage"] = self._build_call_missing_passage(journey_call_df)
            tables["journey_call_on_extra_journey"] = self._build_call_extra_journey(journey_call_df, journey_df)

            # Journey extensions
            tables["journey_start_delay"] = self._build_journey_start_delay(journey_call_df)
            tables["journey_block_first_start_delay"] = self._build_block_first_start_delay(journey_call_df, journey_df)
            tables["journey_missing"] = self._build_journey_missing(journey_df)
            tables["journey_observed_path"] = self._build_observed_path(journey_df, vp_df)
            tables["journey_observed_path_extended"] = self._build_observed_path_extended(journey_df, vp_df)
            tables["journey_planned_path"] = self._build_planned_path(journey_df)

            # Independent tables — Phase 2 GPS analysis
            tables["journey_link_observed_path"] = self._build_link_observed_path(journey_call_df, vp_df)
            tables["journey_link_halted_event"] = self._build_link_halted(journey_call_df, vp_df)
            tables["journey_link_slow_on_link_event"] = self._build_link_slow_event(journey_call_df, vp_df)
            tables["stop_point_stop_position_offset"] = self._build_stop_position_offset(vp_df, stops_df)
            tables["stop_point_big_variation"] = self._build_stop_big_variation(vp_df, stops_df)
            tables["stop_point_large_offset"] = self._build_stop_large_offset(vp_df, stops_df)

        finally:
            journey_df.unpersist()
            journey_call_df.unpersist()

        logger.info(
            f"Built {len(tables)} tables",
            extra={"table": "all", "row_count": sum(t.count() for t in tables.values() if t is not None)},
        )
        return tables


    def build_base_tables(
        self,
        journey_df: DataFrame,
        journey_call_df: DataFrame,
    ) -> dict[str, DataFrame]:
        """Build base + extension tables (16 total).

        Used when --tables=base. Reads from staging tables.
        """
        tables: dict[str, DataFrame] = {}

        journey_df.cache()
        journey_call_df.cache()

        try:
            # Base tables
            tables["journey"] = self._build_journey(journey_df)
            tables["journey_call"] = self._build_journey_call(journey_call_df)

            # JourneyCall extensions
            tables["journey_call_apc"] = self._build_call_apc(journey_call_df)
            tables["journey_call_delay"] = self._build_call_delay(journey_call_df)
            tables["journey_call_statistics"] = self._build_call_statistics(journey_call_df)
            tables["journey_call_stop_and_link_duration"] = self._build_call_stop_link_duration(journey_call_df)
            tables["journey_call_cancelled_or_journey"] = self._build_call_cancelled(journey_call_df, journey_df)
            tables["journey_call_deviation_on"] = self._build_call_deviation(journey_call_df)
            tables["journey_call_missing_passage"] = self._build_call_missing_passage(journey_call_df)
            tables["journey_call_on_extra_journey"] = self._build_call_extra_journey(journey_call_df, journey_df)

            # Journey extensions
            tables["journey_start_delay"] = self._build_journey_start_delay(journey_call_df)
            tables["journey_block_first_start_delay"] = self._build_block_first_start_delay(journey_call_df, journey_df)
            tables["journey_missing"] = self._build_journey_missing(journey_df)
            tables["journey_observed_path"] = self._build_observed_path(journey_df)
            tables["journey_observed_path_extended"] = self._build_observed_path_extended(journey_df)
            tables["journey_planned_path"] = self._build_planned_path(journey_df)

        finally:
            journey_df.unpersist()
            journey_call_df.unpersist()

        logger.info(f"Built {len(tables)} base+extension tables")
        return tables

    def build_gps_tables(
        self,
        journey_call_df: DataFrame,
        vp_df: DataFrame = None,
        stops_df: DataFrame = None,
    ) -> dict[str, DataFrame]:
        """Build 6 GPS tables (journey_link_* + stop_point_*).

        Used when --tables=gps. Reads from staging tables.
        """
        tables: dict[str, DataFrame] = {}

        tables["journey_link_observed_path"] = self._build_link_observed_path(journey_call_df, vp_df)
        tables["journey_link_halted_event"] = self._build_link_halted(journey_call_df, vp_df)
        tables["journey_link_slow_on_link_event"] = self._build_link_slow_event(journey_call_df, vp_df)
        tables["stop_point_stop_position_offset"] = self._build_stop_position_offset(vp_df, stops_df)
        tables["stop_point_big_variation"] = self._build_stop_big_variation(vp_df, stops_df)
        tables["stop_point_large_offset"] = self._build_stop_large_offset(vp_df, stops_df)

        logger.info(f"Built {len(tables)} GPS tables")
        return tables

    # ================================================================
    # BASE TABLE BUILDERS
    # ================================================================

    def _build_journey(self, df: DataFrame) -> DataFrame:
        """Build journey table from raw parsed data."""
        result = df.drop("_source_timestamp", "_is_cancelled")

        # ── Hardcoded columns for Skånetrafiken ──────────────────────────
        result = (
            result
            .withColumn("TransportAuthorityNumber", F.lit(12))
            .withColumn("OperatorName", F.lit("Skånetrafiken"))
            .withColumn("DefaultTransportModeCode", F.lit("Bus"))
            .withColumn("DatedJourneyStateName", F.lit("In_progress"))
            .withColumn("DatedJourneyStateNumber", F.lit(2))
            .withColumn("UTCOffsetMinutes", F.lit(60))
            .withColumn("ExposedInPrintMediaYesNo", F.lit(1))
            .withColumn("MonitoredYesNo", F.lit(1))
            .withColumn("PlannedStateNumber", F.lit(2))
            .withColumn("IsExtraYesNo", F.lit(0))
            .withColumn(
                "DirectionName",
                F.when(F.col("DirectionNumber") == 0, F.lit("Utgående"))
                .when(F.col("DirectionNumber") == 1, F.lit("Inkommande"))
                .otherwise(F.lit(None).cast("string")),
            )
        )
        return result

    def _build_journey_call(self, df: DataFrame) -> DataFrame:
        """Build journey_call table from raw parsed data.

        Includes inter-stop distance calculation (ported from Synapse
        _calculate_stop_distances). Uses haversine between consecutive
        stops ordered by (DVJId, SequenceNumber).
        """
        result = df.drop("_source_timestamp")

        # ── Hardcoded columns for Skånetrafiken ──────────────────────────
        result = (
            result
            .withColumn("StopTransportAuthorityNumber", F.lit(12))
            .withColumn("DatedJourneyStateNumber", F.lit(2))
            .withColumn("UTCOffsetMinutes", F.lit(60))
            .withColumn("ExposedInPrintMediaYesNo", F.lit(1))
            .withColumn("MonitoredYesNo", F.lit(1))
            .withColumn("PlannedStateNumber", F.lit(2))
            .withColumn("IsExtraYesNo", F.lit(0))
            .withColumn(
                "DirectionName",
                F.when(F.col("DirectionNumber") == 0, F.lit("Utgående"))
                .when(F.col("DirectionNumber") == 1, F.lit("Inkommande"))
                .otherwise(F.lit(None).cast("string")),
            )
        )

        # ── Stop distances (Synapse: _calculate_stop_distances) ──────────
        # Haversine between consecutive stops within the same journey.
        R = 6371000.0  # Earth radius in meters
        w = Window.partitionBy("DVJId").orderBy(F.col("SequenceNumber").cast("int"))

        # Get prev/next stop coordinates via Window lag/lead
        result = (
            result
            .withColumn("_prev_lat", F.lag("Coord_Latitude").over(w))
            .withColumn("_prev_lon", F.lag("Coord_Longitude").over(w))
            .withColumn("_next_lat", F.lead("Coord_Latitude").over(w))
            .withColumn("_next_lon", F.lead("Coord_Longitude").over(w))
        )

        def _haversine(lat1, lon1, lat2, lon2):
            """Haversine column expression returning distance in meters."""
            dlat = F.radians(lat2 - lat1)
            dlon = F.radians(lon2 - lon1)
            a = (
                F.pow(F.sin(dlat / 2), 2)
                + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2))
                * F.pow(F.sin(dlon / 2), 2)
            )
            return F.round(F.lit(2 * R) * F.asin(F.sqrt(a)), 2)

        result = result.withColumn(
            "DistanceFromPreviousStop",
            F.when(
                F.col("_prev_lat").isNotNull() & F.col("Coord_Latitude").isNotNull(),
                _haversine(F.col("_prev_lat"), F.col("_prev_lon"),
                           F.col("Coord_Latitude"), F.col("Coord_Longitude")),
            ).cast(T.FloatType()),
        )
        result = result.withColumn(
            "DistanceToNextStop",
            F.when(
                F.col("_next_lat").isNotNull() & F.col("Coord_Latitude").isNotNull(),
                _haversine(F.col("Coord_Latitude"), F.col("Coord_Longitude"),
                           F.col("_next_lat"), F.col("_next_lon")),
            ).cast(T.FloatType()),
        )
        result = result.drop("_prev_lat", "_prev_lon", "_next_lat", "_next_lon")

        return result

    # ================================================================
    # JOURNEY CALL EXTENSION BUILDERS
    # ================================================================

    def _build_call_apc(self, df: DataFrame) -> DataFrame:
        """APC extension: passenger counting data."""
        # APC data comes from VehiclePositions; stub with nulls if not present
        return df.select(
            "DVJId", "SequenceNumber",
            F.col("OnboardCount").cast(T.IntegerType()) if "OnboardCount" in df.columns else F.lit(None).cast("string").alias("OnboardCount"),
            F.col("AlightingCount").cast(T.IntegerType()) if "AlightingCount" in df.columns else F.lit(None).cast("string").alias("AlightingCount"),
            F.col("BoardingCount").cast(T.IntegerType()) if "BoardingCount" in df.columns else F.lit(None).cast("string").alias("BoardingCount"),
            F.col("CapacitySeatingsCount").cast(T.IntegerType()) if "CapacitySeatingsCount" in df.columns else F.lit(None).cast("string").alias("CapacitySeatingsCount"),
            F.col("CapacityStandingsCount").cast(T.IntegerType()) if "CapacityStandingsCount" in df.columns else F.lit(None).cast("string").alias("CapacityStandingsCount"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_delay(self, df: DataFrame) -> DataFrame:
        """Delay extension: arrival/departure delay and punctuality."""
        arr_delay = F.col("ArrivalDelaySeconds").cast(T.LongType()) if "ArrivalDelaySeconds" in df.columns else F.lit(None).cast(T.LongType()).alias("ArrivalDelaySeconds")
        dep_delay = F.col("DepartureDelaySeconds").cast(T.LongType()) if "DepartureDelaySeconds" in df.columns else F.lit(None).cast(T.LongType()).alias("DepartureDelaySeconds")

        return df.select(
            "DVJId", "SequenceNumber",
            arr_delay.alias("ArrivalDelaySeconds"),
            delay_minutes_col(arr_delay).alias("ArrivalDelayMinutes"),
            punctuality_col(arr_delay).alias("ArrivalPunctualityText"),
            dep_delay.alias("DepartureDelaySeconds"),
            delay_minutes_col(dep_delay).alias("DepartureDelayMinutes"),
            punctuality_col(dep_delay).alias("DeparturePunctualityText"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_statistics(self, df: DataFrame) -> DataFrame:
        """Statistics extension: punctuality codes, detection completeness."""
        arr_delay = F.col("ArrivalDelaySeconds") if "ArrivalDelaySeconds" in df.columns else F.lit(None).cast("string")
        dep_delay = F.col("DepartureDelaySeconds") if "DepartureDelaySeconds" in df.columns else F.lit(None).cast("string")
        completeness = detection_completeness_col("ArrivalStateName", "DepartureStateName") if "ArrivalStateName" in df.columns else F.lit(None).cast("string")

        return df.select(
            "DVJId", "SequenceNumber",
            is_first_stop_col("SequenceNumber").alias("IsFirstStop"),
            is_last_stop_col("SequenceNumber", "JourneyLastStopSequenceNumber").alias("IsLastStop") if "JourneyLastStopSequenceNumber" in df.columns else F.lit("No").alias("IsLastStop"),
            punctuality_code_col(arr_delay).alias("ArrivalPunctualityCode"),
            punctuality_col(arr_delay).alias("ArrivalPunctualityText"),
            punctuality_code_col(dep_delay).alias("DeparturePunctualityCode"),
            punctuality_col(dep_delay).alias("DeparturePunctualityText"),
            detection_completeness_state_col(completeness).alias("DetectionCompletenessState") if completeness is not None else F.lit(None).cast("string").alias("DetectionCompletenessState"),
            (completeness if completeness is not None else F.lit(None).cast("string")).alias("DetectionCompleteness"),
            F.lit("1").cast(T.StringType()).alias("SignonCompletenessState"),
            F.lit("Complete").cast(T.StringType()).alias("SignOnDetectionCompleteness"),
            F.lit(None).cast(T.DoubleType()).alias("LastLinkObservedAvgSpeedKph"),
            (
                F.unix_timestamp(F.col("ObservedDepartureDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
                - F.unix_timestamp(F.col("ObservedArrivalDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX")
            ).cast(T.LongType()).alias("ObservedStopDurationSeconds") if "ObservedArrivalDateTime" in df.columns else F.lit(None).cast(T.LongType()).alias("ObservedStopDurationSeconds"),
            punctuality_code_col(arr_delay).alias("TargetArrivalPunctualityCode"),
            punctuality_col(arr_delay).alias("TargetArrivalPunctualityText"),
            punctuality_code_col(dep_delay).alias("TargetDeparturePunctualityCode"),
            punctuality_col(dep_delay).alias("TargetDeparturePunctualityText"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_stop_link_duration(self, df: DataFrame) -> DataFrame:
        """Stop and link duration extension.

        Ported from Synapse calculate_duration_columns(). Computes:
        - Observed/planned stop durations (dep - arr at same stop)
        - Observed/planned run durations (curr arr - prev dep)
        - Duration since first departure in the journey
        - Cumulative (accumulated) sums per journey
        - Average speed between stops (haversine distance / run time)
        """
        R = 6371000.0  # Earth radius in meters
        w = Window.partitionBy("DVJId").orderBy(F.col("SequenceNumber").cast("int"))
        w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        # Select needed columns + dedup
        cols_needed = ["DVJId", "SequenceNumber",
                       "ObservedArrivalDateTime", "ObservedDepartureDateTime",
                       "TimetabledLatestArrivalDateTime", "TimetabledEarliestDepartureDateTime",
                       "Coord_Latitude", "Coord_Longitude",
                       "ArrivalDelaySeconds", "DepartureDelaySeconds"]
        select_cols = [c for c in cols_needed if c in df.columns]
        result = df.select(*select_cols).dropDuplicates(["DVJId", "SequenceNumber"])

        # Cast datetime strings to unix seconds for arithmetic
        result = (
            result
            .withColumn("_obs_arr_ts", F.unix_timestamp(F.col("ObservedArrivalDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
            .withColumn("_obs_dep_ts", F.unix_timestamp(F.col("ObservedDepartureDateTime"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
            .withColumn("_tt_arr_ts", F.unix_timestamp(F.col("TimetabledLatestArrivalDateTime")))
            .withColumn("_tt_dep_ts", F.unix_timestamp(F.col("TimetabledEarliestDepartureDateTime")))
        )

        # ── 1. Stop durations (time spent at current stop) ──
        result = result.withColumn(
            "ObservedStopDurationSeconds",
            (F.col("_obs_dep_ts") - F.col("_obs_arr_ts")).cast(T.LongType()),
        )
        result = result.withColumn(
            "PlannedStopDurationSeconds",
            (F.col("_tt_dep_ts") - F.col("_tt_arr_ts")).cast(T.LongType()),
        )

        # ── 2. Run durations (time between prev departure and current arrival) ──
        result = result.withColumn("_prev_obs_dep_ts", F.lag("_obs_dep_ts").over(w))
        result = result.withColumn("_prev_tt_dep_ts", F.lag("_tt_dep_ts").over(w))

        result = result.withColumn(
            "ObservedRunDurationFromPrevStopSeconds",
            (F.col("_obs_arr_ts") - F.col("_prev_obs_dep_ts")).cast(T.LongType()),
        )
        result = result.withColumn(
            "PlannedRunDurationFromPrevStopSeconds",
            (F.col("_tt_arr_ts") - F.col("_prev_tt_dep_ts")).cast(T.LongType()),
        )

        # ── 3. Difference between observed and planned run duration ──
        result = result.withColumn(
            "DiffRunDurationSeconds",
            (F.col("ObservedRunDurationFromPrevStopSeconds")
             - F.col("PlannedRunDurationFromPrevStopSeconds")).cast(T.LongType()),
        )

        # ── 4. Duration since first departure in the journey ──
        result = result.withColumn(
            "_first_dep_ts",
            F.first("_obs_dep_ts", ignorenulls=True).over(
                Window.partitionBy("DVJId").orderBy(F.col("SequenceNumber").cast("int"))
                .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            ),
        )
        result = result.withColumn(
            "ObservedDurationSinceFirstDepartureSeconds",
            (F.col("_obs_dep_ts") - F.col("_first_dep_ts")).cast(T.LongType()),
        )

        # ── 5. Accumulated (cumulative) sums per journey ──
        result = result.withColumn(
            "AccObservedStopDurationSeconds",
            F.sum("ObservedStopDurationSeconds").over(w_cum).cast(T.LongType()),
        )
        result = result.withColumn(
            "AccObservedStopDurationSinceFirstDepartureSeconds",
            F.sum("ObservedStopDurationSeconds").over(w_cum).cast(T.LongType()),
        )
        result = result.withColumn(
            "AccObservedRunDurationSeconds",
            F.sum("ObservedRunDurationFromPrevStopSeconds").over(w_cum).cast(T.LongType()),
        )
        result = result.withColumn(
            "AccPlannedRunDurationSeconds",
            F.sum("PlannedRunDurationFromPrevStopSeconds").over(w_cum).cast(T.LongType()),
        )
        result = result.withColumn(
            "AccDiffRunDurationSeconds",
            F.sum("DiffRunDurationSeconds").over(w_cum).cast(T.LongType()),
        )

        # ── 6. Average speed between stops (haversine / run duration) ──
        result = result.withColumn("_prev_lat", F.lag("Coord_Latitude").over(w))
        result = result.withColumn("_prev_lon", F.lag("Coord_Longitude").over(w))

        dlat = F.radians(F.col("Coord_Latitude") - F.col("_prev_lat"))
        dlon = F.radians(F.col("Coord_Longitude") - F.col("_prev_lon"))
        a = (
            F.pow(F.sin(dlat / 2), 2)
            + F.cos(F.radians(F.col("_prev_lat")))
            * F.cos(F.radians(F.col("Coord_Latitude")))
            * F.pow(F.sin(dlon / 2), 2)
        )
        dist_m = F.lit(2 * R) * F.asin(F.sqrt(a))

        result = result.withColumn(
            "ObservedAvgSpeedKph",
            F.when(
                (F.col("ObservedRunDurationFromPrevStopSeconds").isNotNull())
                & (F.col("ObservedRunDurationFromPrevStopSeconds") > 0)
                & (F.col("_prev_lat").isNotNull()),
                F.round(dist_m / F.col("ObservedRunDurationFromPrevStopSeconds") * 3.6, 1),
            ).cast(T.DoubleType()),
        )

        # ── 7. Detection completeness (from arrival + departure state) ──
        result = result.withColumn(
            "DetectionCompletenessState",
            F.lit("1").cast(T.StringType()),
        )
        result = result.withColumn(
            "DetectionCompleteness",
            F.lit("Complete").cast(T.StringType()),
        )

        # ── 8. Halts (from VP data — not available here, stays NULL) ──
        result = result.withColumn("HaltsOnLinkDurationSeconds", F.lit(None).cast(T.LongType()))
        result = result.withColumn("AccHaltsOnLinkDurationSeconds", F.lit(None).cast(T.LongType()))

        # ── 9. APC counts (no hardware — stays NULL) ──
        result = result.withColumn("OnboardCount", F.lit(None).cast(T.IntegerType()))
        result = result.withColumn("AlightingCount", F.lit(None).cast(T.IntegerType()))
        result = result.withColumn("BoardingCount", F.lit(None).cast(T.IntegerType()))

        # ── 10. Delay pass-through ──
        if "ArrivalDelaySeconds" in df.columns:
            result = result.withColumn("ArrivalDelaySeconds", F.col("ArrivalDelaySeconds"))
        else:
            result = result.withColumn("ArrivalDelaySeconds", F.lit(None).cast(T.LongType()))
        if "DepartureDelaySeconds" in df.columns:
            result = result.withColumn("DepartureDelaySeconds", F.col("DepartureDelaySeconds"))
        else:
            result = result.withColumn("DepartureDelaySeconds", F.lit(None).cast(T.LongType()))

        # Drop internal temp columns
        result = result.drop(
            "_obs_arr_ts", "_obs_dep_ts", "_tt_arr_ts", "_tt_dep_ts",
            "_prev_obs_dep_ts", "_prev_tt_dep_ts", "_first_dep_ts",
            "_prev_lat", "_prev_lon",
            "ObservedArrivalDateTime", "ObservedDepartureDateTime",
            "TimetabledLatestArrivalDateTime", "TimetabledEarliestDepartureDateTime",
            "Coord_Latitude", "Coord_Longitude",
        )

        return result

    def _build_call_cancelled(self, df: DataFrame, journey_df: DataFrame) -> DataFrame:
        """Cancellation extension."""
        return df.select(
            "DVJId", "SequenceNumber",
            F.lit("No").alias("CallCancelledYesNo"),
            F.lit("No").alias("JourneyCancelledYesNo"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_deviation(self, df: DataFrame) -> DataFrame:
        """Deviation extension — placeholder for ServiceAlerts enrichment."""
        from htq2_gtfs.processing.models import JOURNEY_CALL_DEVIATION_EXT
        result = df.select("DVJId", "SequenceNumber")
        for col_name in JOURNEY_CALL_DEVIATION_EXT:
            result = result.withColumn(col_name, F.lit(None).cast(T.StringType()))
        return result.dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_missing_passage(self, df: DataFrame) -> DataFrame:
        """Missing passage detection."""
        return df.select(
            "DVJId", "SequenceNumber",
            F.lit(None).cast(T.StringType()).alias("ReasonType"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    def _build_call_extra_journey(self, df: DataFrame, journey_df: DataFrame) -> DataFrame:
        """Extra journey detection."""
        return df.select(
            "DVJId", "SequenceNumber",
            F.when(F.col("DVJId").isNull(), F.lit("ExtraTripId")).otherwise(F.lit(None).cast("string")).alias("ReinforcementType"),
        ).dropDuplicates(["DVJId", "SequenceNumber"])

    # ================================================================
    # JOURNEY EXTENSION BUILDERS
    # ================================================================

    def _build_journey_start_delay(self, call_df: DataFrame) -> DataFrame:
        """Start delay: departure delay at first stop."""
        first_stops = call_df.filter(F.col("SequenceNumber") == 1)
        dep_delay = F.col("DepartureDelaySeconds") if "DepartureDelaySeconds" in first_stops.columns else F.lit(None).cast("string")
        return first_stops.select(
            "DVJId", "OperatingDate",
            dep_delay.alias("DepartureDelaySeconds"),
            delay_minutes_col(dep_delay).alias("DepartureDelayMinutes"),
            punctuality_col(dep_delay).alias("DeparturePunctualityText"),
        ).dropDuplicates(["DVJId", "OperatingDate"])

    def _build_block_first_start_delay(self, call_df: DataFrame, journey_df: DataFrame) -> DataFrame:
        """Block first start delay: delay of the first journey in each block."""
        # Simplified: same as start delay for now
        return self._build_journey_start_delay(call_df)

    def _build_journey_missing(self, journey_df: DataFrame) -> DataFrame:
        """Missing journey detection."""
        return journey_df.select(
            "DVJId", "OperatingDate",
            F.when(
                F.col("_is_cancelled") == True, F.lit("Cancelled")
            ).otherwise(F.lit(None).cast("string")).alias("ReasonTypeName") if "_is_cancelled" in journey_df.columns
            else F.lit(None).cast(T.StringType()).alias("ReasonTypeName"),
        ).dropDuplicates(["DVJId", "OperatingDate"])

    def _build_observed_path(self, journey_df: DataFrame, vp_df: DataFrame = None) -> DataFrame:
        """Observed path — GPS breadcrumb trail per journey.
        
        Ported from Synapse _build_journey_observed_path_view().
        Extracts GPS position records from VehiclePositions per DVJId.
        """
        from htq2_gtfs.processing.models import JOURNEY_OBSERVED_PATH_EXT
        
        # If no VP data, return stub (one row per journey, all nulls)
        if vp_df is None or vp_df.limit(1).count() == 0:
            result = journey_df.select("DVJId", "OperatingDate")
            for col_name in JOURNEY_OBSERVED_PATH_EXT:
                result = result.withColumn(col_name, F.lit(None).cast(T.StringType()))
            return result.dropDuplicates(["DVJId", "OperatingDate"])
        
        # Extract GPS positions from VP data (one row per GPS point per journey)
        w_vp = Window.partitionBy("DVJId").orderBy("timestamp")
        
        observed = (
            vp_df
            .filter(F.col("DVJId").isNotNull() & F.col("latitude").isNotNull())
            .withColumn("PositionDateTime",
                F.from_unixtime(F.col("timestamp").cast("long")).cast("string"))
            .withColumn("Latitude", F.col("latitude").cast("string"))
            .withColumn("Longitude", F.col("longitude").cast("string"))
            .withColumn("SpeedKph",
                F.when(F.col("speed").isNotNull(), (F.col("speed") * 3.6).cast("string")))
            .withColumn("DirectionDegree",
                F.when(F.col("bearing").isNotNull(), F.col("bearing").cast("string"))
                 .otherwise(F.lit(None).cast("string")))
            .withColumn("NextLatitude", F.lead("latitude").over(w_vp).cast("string"))
            .withColumn("NextLongitude", F.lead("longitude").over(w_vp).cast("string"))
            .withColumn("RunByOperatorNumber", F.lit(None).cast("string"))
            .withColumn("RunByVehicleNumber", F.lit(None).cast("string"))
            .withColumn("Note", F.lit(None).cast("string"))
            .select("DVJId", "OperatingDate", *JOURNEY_OBSERVED_PATH_EXT)
        )
        
        return observed

    def _build_observed_path_extended(self, journey_df: DataFrame, vp_df: DataFrame = None) -> DataFrame:
        """Extended observed path with cumulative position offset.
        
        Ported from Synapse: cumulative haversine distance from journey start.
        """
        # If no VP data, return stub
        if vp_df is None or vp_df.limit(1).count() == 0:
            return journey_df.select(
                "DVJId", "OperatingDate",
                F.lit(None).cast(T.DoubleType()).alias("RelativePositionOffsetMeters"),
            ).dropDuplicates(["DVJId", "OperatingDate"])
        
        w_vp = Window.partitionBy("DVJId").orderBy("timestamp")
        
        # Compute cumulative haversine distance
        extended = (
            vp_df
            .filter(F.col("DVJId").isNotNull() & F.col("latitude").isNotNull())
            .withColumn("_prev_lat", F.lag("latitude").over(w_vp))
            .withColumn("_prev_lon", F.lag("longitude").over(w_vp))
            .withColumn("_segment_dist",
                F.when(F.col("_prev_lat").isNotNull(),
                    haversine_col(
                        F.col("_prev_lat").cast("double"), F.col("_prev_lon").cast("double"),
                        F.col("latitude").cast("double"), F.col("longitude").cast("double"),
                    )
                ).otherwise(F.lit(0.0))
            )
            .withColumn("RelativePositionOffsetMeters",
                F.sum("_segment_dist").over(
                    Window.partitionBy("DVJId").orderBy("timestamp")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                )
            )
            .select("DVJId", "OperatingDate", "RelativePositionOffsetMeters")
        )
        
        return extended

    def _build_planned_path(self, journey_df: DataFrame, shapes_df: DataFrame = None, trips_df: DataFrame = None) -> DataFrame:
        """Planned path — route geometry from shapes.txt per journey.
        
        Ported from Synapse _build_journey_planned_path_view().
        Maps shape points to journeys via trip_id → shape_id.
        Computes EndOfLinkSequenceNumber, cumulative OnLinkOffsetMeters, and Next* coordinates.
        """
        from htq2_gtfs.processing.models import JOURNEY_PLANNED_PATH_EXT
        
        # If no shapes data, return stub (one row per journey, all nulls)
        if shapes_df is None or shapes_df.limit(1).count() == 0:
            result = journey_df.select("DVJId", "OperatingDate")
            for col_name in JOURNEY_PLANNED_PATH_EXT:
                result = result.withColumn(col_name, F.lit(None).cast(T.StringType()))
            return result.dropDuplicates(["DVJId", "OperatingDate"])
        
        if trips_df is None or trips_df.limit(1).count() == 0:
            result = journey_df.select("DVJId", "OperatingDate")
            for col_name in JOURNEY_PLANNED_PATH_EXT:
                result = result.withColumn(col_name, F.lit(None).cast(T.StringType()))
            return result.dropDuplicates(["DVJId", "OperatingDate"])
        
        # 1. Get trip_id → shape_id mapping
        trip_shape = trips_df.select(
            F.col("trip_id").cast("string"),
            F.col("shape_id").cast("string"),
        ).dropDuplicates(["trip_id"])
        
        # 2. Get DVJId → trip_id (JourneyGid) from journey data
        dvj_trip = journey_df.select(
            "DVJId", "OperatingDate",
            F.col("JourneyGid").cast("string").alias("trip_id"),
        ).filter(F.col("trip_id").isNotNull())
        
        # 3. Join to get DVJId → shape_id
        dvj_shape = dvj_trip.join(trip_shape, "trip_id", "inner").select("DVJId", "OperatingDate", "shape_id")
        
        # 4. Join with shapes data
        w_shape = Window.partitionBy("shape_id").orderBy("shape_pt_sequence")
        
        path_points = (
            dvj_shape
            .join(shapes_df, "shape_id", "inner")
            .withColumn("Latitude", F.col("shape_pt_lat").cast("string"))
            .withColumn("Longitude", F.col("shape_pt_lon").cast("string"))
            .withColumn("NextLatitude", F.lead("shape_pt_lat").over(
                Window.partitionBy("DVJId").orderBy("shape_pt_sequence")
            ).cast("string"))
            .withColumn("NextLongitude", F.lead("shape_pt_lon").over(
                Window.partitionBy("DVJId").orderBy("shape_pt_sequence")
            ).cast("string"))
            .withColumn("OnLinkOffsetMeters", F.col("shape_dist_traveled").cast("string"))
            .withColumn("EndOfLinkSequenceNumber", F.lit(None).cast("string"))
            .withColumn("SpeedLimitKmPerHour", F.lit(None).cast("string"))
            .select("DVJId", "OperatingDate", *JOURNEY_PLANNED_PATH_EXT)
        )
        
        return path_points


    # ================================================================
    # INDEPENDENT TABLE BUILDERS (JourneyLink + StopPoint)
    # ================================================================
    # PHASE 2: GPS-BASED TABLE BUILDERS
    # ================================================================
    # These tables are populated from VehiclePositions .pb files.
    # JourneyLink tables: GPS points assigned to links (between stops).
    # StopPoint tables: GPS points matched to stops, aggregated daily.

    def _build_link_observed_path(self, call_df: DataFrame, vp_df: DataFrame = None) -> DataFrame:
        """JourneyLink_ObservedPath -- GPS breadcrumbs segmented by link.

        A 'link' is the segment between two consecutive stops.
        Each VP GPS point is assigned to the link whose stop departure/arrival
        window contains that GPS timestamp.
        """
        from htq2_gtfs.processing.models import JOURNEY_LINK_OBSERVED_PATH_COLUMNS
        empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in JOURNEY_LINK_OBSERVED_PATH_COLUMNS])

        if vp_df is None or vp_df.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        # Only keep VP points that matched a journey (have DVJId)
        vp_matched = vp_df.filter(F.col("DVJId").isNotNull())
        if vp_matched.isEmpty():
            return self.spark.createDataFrame([], empty_schema)

        # Get stop sequences with observed times per journey
        stops = (
            call_df
            .filter(F.col("DVJId").isNotNull())
            .select("DVJId", "OperatingDate", "SequenceNumber",
                    "ObservedDepartureDateTime", "ObservedArrivalDateTime")
            .withColumn("SequenceNumber", F.col("SequenceNumber").cast("int"))
            .orderBy("DVJId", "SequenceNumber")
        )

        # Self-join stops to get link boundaries (stop N -> stop N+1)
        s1 = stops.alias("s1")
        s2 = stops.alias("s2")
        links = (
            s1.join(
                s2,
                on=[
                    F.col("s1.DVJId") == F.col("s2.DVJId"),
                    F.col("s2.SequenceNumber") == F.col("s1.SequenceNumber") + 1,
                ],
                how="inner",
            )
            .select(
                F.col("s1.DVJId").alias("DVJId"),
                F.col("s1.OperatingDate").alias("OperatingDate"),
                F.col("s1.SequenceNumber").alias("FromSeq"),
                F.col("s2.SequenceNumber").alias("EndOfLinkSequenceNumber"),
                F.col("s1.ObservedDepartureDateTime").alias("LinkStartTime"),
                F.col("s2.ObservedArrivalDateTime").alias("LinkEndTime"),
            )
        )

        # Cast VP timestamp to comparable format
        vp_with_ts = vp_matched.withColumn(
            "pos_ts",
            F.from_unixtime("timestamp").cast("string")
        )

        # Join GPS points to links on DVJId + timestamp within link window
        link_gps = (
            vp_with_ts.alias("v")
            .join(links.alias("l"), on=[F.col("v.DVJId") == F.col("l.DVJId")], how="inner")
            .filter(
                (F.col("v.pos_ts") >= F.col("l.LinkStartTime")) &
                (F.col("v.pos_ts") <= F.col("l.LinkEndTime"))
            )
            .select(
                F.col("v.DVJId").alias("DVJId"),
                F.col("l.OperatingDate").alias("OperatingDate"),
                F.col("l.FromSeq").cast("string").alias("SequenceNumber"),
                F.monotonically_increasing_id().cast("string").alias("LinkNumber"),
                F.col("l.EndOfLinkSequenceNumber").cast("string").alias("EndOfLinkSequenceNumber"),
            )
        )

        if link_gps.isEmpty():
            return self.spark.createDataFrame([], empty_schema)

        for col_name in JOURNEY_LINK_OBSERVED_PATH_COLUMNS:
            if col_name not in link_gps.columns:
                link_gps = link_gps.withColumn(col_name, F.lit(None).cast("string"))
        return link_gps.select(*JOURNEY_LINK_OBSERVED_PATH_COLUMNS)

    def _build_link_halted(self, call_df: DataFrame, vp_df: DataFrame = None) -> DataFrame:
        """JourneyLink_HaltedEvent -- halt detection (speed < 3 kph away from stops)."""
        from htq2_gtfs.processing.models import JOURNEY_LINK_HALTED_EVENT_COLUMNS
        empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in JOURNEY_LINK_HALTED_EVENT_COLUMNS])

        if vp_df is None or vp_df.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        halted_vp = vp_df.filter(
            (F.col("DVJId").isNotNull()) &
            (F.col("speed").isNotNull()) &
            (F.col("speed") < 3.0) &
            (F.col("latitude").isNotNull())
        )
        if halted_vp.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        # Compute bird distance (haversine between consecutive stops) per link
        from pyspark.sql.window import Window as W
        w_stops = W.partitionBy("DVJId").orderBy("SequenceNumber")
        link_dist = (
            call_df.select("DVJId", "SequenceNumber", "Coord_Latitude", "Coord_Longitude")
            .withColumn("_next_lat", F.lead("Coord_Latitude").over(w_stops))
            .withColumn("_next_lon", F.lead("Coord_Longitude").over(w_stops))
            .filter(F.col("_next_lat").isNotNull())
            .withColumn("_bird_dist", haversine_col(
                F.col("Coord_Latitude").cast("double"), F.col("Coord_Longitude").cast("double"),
                F.col("_next_lat").cast("double"), F.col("_next_lon").cast("double"),
            ))
            .groupBy("DVJId")
            .agg(F.first("_bird_dist").alias("_bird_distance"))
        )

        halt_records = (
            halted_vp
            .groupBy("DVJId", "OperatingDate")
            .agg(
                F.lit("1").alias("SequenceNumber"),
                F.lit("1").alias("LinkNumber"),
                F.lit("0").cast("string").alias("HaltedDurationSeconds"),
                F.min("latitude").cast("string").alias("HaltAreaMinLatitude"),
                F.min("longitude").cast("string").alias("HaltAreaMinLongitude"),
                F.max("latitude").cast("string").alias("HaltAreaMaxLatitude"),
                F.max("longitude").cast("string").alias("HaltAreaMaxLongitude"),
                F.avg("latitude").cast("string").alias("HaltAreaCenterLatitude"),
                F.avg("longitude").cast("string").alias("HaltAreaCenterLongitude"),
            )
        )
        # Join bird distance from call_df
        halt_records = halt_records.join(link_dist, "DVJId", "left")
        halt_records = halt_records.withColumn(
            "BirdDistanceFromPrevJPPMeters", F.col("_bird_distance").cast("string")
        ).withColumn(
            "BirdDistanceToNextJPPMeters", F.col("_bird_distance").cast("string")
        ).drop("_bird_distance")

        for col_name in JOURNEY_LINK_HALTED_EVENT_COLUMNS:
            if col_name not in halt_records.columns:
                halt_records = halt_records.withColumn(col_name, F.lit(None).cast("string"))
        return halt_records.select(*JOURNEY_LINK_HALTED_EVENT_COLUMNS)

    def _build_link_slow_event(self, call_df: DataFrame, vp_df: DataFrame = None) -> DataFrame:
        """JourneyLink_SlowOnLinkEvent -- slow speed detection (3-15 kph)."""
        from htq2_gtfs.processing.models import JOURNEY_LINK_SLOW_EVENT_COLUMNS
        empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in JOURNEY_LINK_SLOW_EVENT_COLUMNS])

        if vp_df is None or vp_df.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        slow_vp = vp_df.filter(
            (F.col("DVJId").isNotNull()) &
            (F.col("speed").isNotNull()) &
            (F.col("speed") >= 3.0) &
            (F.col("speed") < 15.0) &
            (F.col("latitude").isNotNull())
        )
        if slow_vp.limit(1).count() == 0:
            return self.spark.createDataFrame([], empty_schema)

        # Compute bird distance (haversine between consecutive stops) per link
        from pyspark.sql.window import Window as W
        w_stops = W.partitionBy("DVJId").orderBy("SequenceNumber")
        link_dist = (
            call_df.select("DVJId", "SequenceNumber", "Coord_Latitude", "Coord_Longitude")
            .withColumn("_next_lat", F.lead("Coord_Latitude").over(w_stops))
            .withColumn("_next_lon", F.lead("Coord_Longitude").over(w_stops))
            .filter(F.col("_next_lat").isNotNull())
            .withColumn("_bird_dist", haversine_col(
                F.col("Coord_Latitude").cast("double"), F.col("Coord_Longitude").cast("double"),
                F.col("_next_lat").cast("double"), F.col("_next_lon").cast("double"),
            ))
            .groupBy("DVJId")
            .agg(F.first("_bird_dist").alias("_bird_distance"))
        )

        slow_records = (
            slow_vp
            .groupBy("DVJId", "OperatingDate")
            .agg(
                F.lit("1").alias("SequenceNumber"),
                F.lit("1").alias("LinkNumber"),
                F.lit("0").cast("string").alias("SlowDurationSeconds"),
                F.min("latitude").cast("string").alias("SlowAreaMinLatitude"),
                F.min("longitude").cast("string").alias("SlowAreaMinLongitude"),
                F.max("latitude").cast("string").alias("SlowAreaMaxLatitude"),
                F.max("longitude").cast("string").alias("SlowAreaMaxLongitude"),
                F.avg("latitude").cast("string").alias("SlowAreaCenterLatitude"),
                F.avg("longitude").cast("string").alias("SlowAreaCenterLongitude"),
            )
        )
        # Join bird distance from call_df
        slow_records = slow_records.join(link_dist, "DVJId", "left")
        slow_records = slow_records.withColumn(
            "BirdDistanceFromPrevJPPMeters", F.col("_bird_distance").cast("string")
        ).withColumn(
            "BirdDistanceToNextJPPMeters", F.col("_bird_distance").cast("string")
        ).drop("_bird_distance")

        for col_name in JOURNEY_LINK_SLOW_EVENT_COLUMNS:
            if col_name not in slow_records.columns:
                slow_records = slow_records.withColumn(col_name, F.lit(None).cast("string"))
        return slow_records.select(*JOURNEY_LINK_SLOW_EVENT_COLUMNS)

    def _build_stop_position_offset(self, vp_df: DataFrame = None, stops_df: DataFrame = None) -> DataFrame:
        """StopPoint_StopPositionOffset -- GPS position offset from planned stop location.

        Matches vehicle GPS positions to nearby stops (within 200m),
        aggregates per stop to compute offset, spread, and corrected coords.
        """
        from htq2_gtfs.processing.models import STOP_POINT_POSITION_OFFSET_COLUMNS
        empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in STOP_POINT_POSITION_OFFSET_COLUMNS])

        if vp_df is None or vp_df.limit(1).count() == 0 or stops_df is None or stops_df.isEmpty():
            return self.spark.createDataFrame([], empty_schema)

        vp_valid = vp_df.filter(
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        )
        if vp_valid.isEmpty():
            return self.spark.createDataFrame([], empty_schema)

        R = 6371000  # Earth radius in meters
        MATCHING_RADIUS = 200  # meters

        # Cross-join VP positions with stops (both are small datasets)
        matched = (
            vp_valid.alias("v")
            .crossJoin(stops_df.alias("s"))
            .withColumn("dlat", F.radians(F.col("v.latitude") - F.col("s.stop_lat")))
            .withColumn("dlon", F.radians(F.col("v.longitude") - F.col("s.stop_lon")))
            .withColumn("lat1_rad", F.radians(F.col("v.latitude")))
            .withColumn("lat2_rad", F.radians(F.col("s.stop_lat")))
            .withColumn(
                "a",
                F.pow(F.sin(F.col("dlat") / 2), 2) +
                F.cos(F.col("lat1_rad")) * F.cos(F.col("lat2_rad")) *
                F.pow(F.sin(F.col("dlon") / 2), 2)
            )
            .withColumn("distance_m", F.lit(2 * R) * F.asin(F.sqrt(F.col("a"))))
            .filter(F.col("distance_m") < MATCHING_RADIUS)
        )

        if matched.isEmpty():
            return self.spark.createDataFrame([], empty_schema)

        result = (
            matched
            .groupBy("s.stop_id", "s.stop_name", "s.stop_lat", "s.stop_lon", "v.OperatingDate")
            .agg(
                F.count("*").alias("ObservationCount"),
                F.avg("distance_m").alias("MeanOffsetMeters_raw"),
                F.max("distance_m").alias("SpreadMeters_raw"),
                F.avg("v.latitude").alias("CorrectedLatitude_raw"),
                F.avg("v.longitude").alias("CorrectedLongitude_raw"),
                F.expr("percentile_approx((v.latitude - s.stop_lat) * 111000, 0.5)").alias("LatMedianErr_raw"),
                F.expr("percentile_approx((v.longitude - s.stop_lon) * 111000 * cos(radians(s.stop_lat)), 0.5)").alias("LonMedianErr_raw"),
            )
            .filter(F.col("ObservationCount") >= 2)
            .withColumn("StopName", F.col("stop_name"))
            .withColumn("StopTransportAuthorityNumber",
                        F.when(F.length("stop_id") >= 7, F.substring("stop_id", 6, 2)).cast("string"))
            .withColumn("StopAreaNumber",
                        F.when(F.length("stop_id") >= 13, F.substring("stop_id", 8, 6)).cast("string"))
            .withColumn("StopPointLocalNumber",
                        F.when(F.length("stop_id") >= 16, F.substring("stop_id", 14, 3)).cast("string"))
            .withColumn("JourneyPatternPointNumber", F.col("stop_id"))
            .withColumn("ObservationCount", F.col("ObservationCount").cast("string"))
            .withColumn("MeanOffsetMeters", F.round("MeanOffsetMeters_raw").cast("string"))
            .withColumn("SpreadMeters", F.round("SpreadMeters_raw").cast("string"))
            .withColumn("CurrentLatitude", F.col("stop_lat").cast("string"))
            .withColumn("CorrectedLatitude", F.round("CorrectedLatitude_raw", 6).cast("string"))
            .withColumn("CurrentLongitude", F.col("stop_lon").cast("string"))
            .withColumn("CorrectedLongitude", F.round("CorrectedLongitude_raw", 6).cast("string"))
            .withColumn("OperatingDate", F.col("OperatingDate"))
            .withColumn("JPPGid", F.concat(F.lit("9011012"), F.col("stop_id")))
            .withColumn("LatitudeMedianErrMeters", F.round("LatMedianErr_raw").cast("string"))
            .withColumn("LongitudeMedianErrMeters", F.round("LonMedianErr_raw").cast("string"))
        )

        return result.select(*STOP_POINT_POSITION_OFFSET_COLUMNS)

    def _build_stop_big_variation(self, vp_df: DataFrame = None, stops_df: DataFrame = None) -> DataFrame:
        """StopPoint_BigVariation -- stops where position spread > 50m."""
        from htq2_gtfs.processing.models import STOP_POINT_BIG_VARIATION_COLUMNS
        BIG_VARIATION_THRESHOLD = 50

        offset_df = self._build_stop_position_offset(vp_df, stops_df)
        if offset_df.isEmpty():
            empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in STOP_POINT_BIG_VARIATION_COLUMNS])
            return self.spark.createDataFrame([], empty_schema)

        return offset_df.filter(F.col("SpreadMeters").cast("double") > BIG_VARIATION_THRESHOLD)

    def _build_stop_large_offset(self, vp_df: DataFrame = None, stops_df: DataFrame = None) -> DataFrame:
        """StopPoint_LargeOffset -- stops where mean offset > 30m."""
        from htq2_gtfs.processing.models import STOP_POINT_LARGE_OFFSET_COLUMNS
        LARGE_OFFSET_THRESHOLD = 30

        offset_df = self._build_stop_position_offset(vp_df, stops_df)
        if offset_df.isEmpty():
            empty_schema = T.StructType([T.StructField(c, T.StringType(), True) for c in STOP_POINT_LARGE_OFFSET_COLUMNS])
            return self.spark.createDataFrame([], empty_schema)

        return offset_df.filter(F.col("MeanOffsetMeters").cast("double") > LARGE_OFFSET_THRESHOLD)
