"""HTQ2 GTFS Data Models — Column definitions and table schemas.

Direct port of the normalized architecture from Synapse
gtfs_models_delta.py. These column lists are the **schema contract**
between this pipeline and downstream HTQ2 BI_EDA consumers.

Do NOT modify column names or order without updating the
corresponding C# view specifications.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ================================================================
# BASE COLUMN DEFINITIONS
# ================================================================

# Journey base columns (36 columns)
JOURNEY_BASE_COLUMNS: list[str] = [
    "OperatingDate", "OperatingWeekDay", "DefaultTransportModeCode",
    "TransportAuthorityNumber", "LineNumber", "ExtendedLineDesignation",
    "DirectionNumber", "JourneyNumber", "JourneyStartTime",
    "DatedJourneyStateName", "JourneyOriginName", "JourneyEndStopName",
    "OperatorName", "BlockNumber", "VehicleNumber",
    "StartMunicipalityName", "DestinationName",
    "VehicleAssignedFromDateTime", "VehicleAssignedUptoDateTime", "RunByVehicleGid",
    "InProgressFromDateTime", "InProgressUptoDateTime", "AssignedDriverGid",
    "DirectionName", "JourneyStartDateTime", "DatedJourneyStateNumber",
    "UTCOffsetMinutes", "JourneyLastStopSequenceNumber",
    "ExposedInPrintMediaYesNo", "MonitoredYesNo", "PlannedStateNumber",
    "IsExtraYesNo", "ReinforcedDVJId", "UsesJourneyPatternId",
    "JourneyGid", "DVJId",
]

# JourneyCall base columns (67 columns) = Journey + stop-specific
JOURNEY_CALL_BASE_COLUMNS: list[str] = JOURNEY_BASE_COLUMNS + [
    # Stop information
    "SequenceNumber", "StopName", "MunicipalityName", "TimingPointYesNo",
    # Arrival
    "ArrivalStateName", "TimetabledLatestArrivalTime", "ObservedArrivalTime",
    # Departure
    "DepartureStateName", "TimetabledEarliestDepartureTime", "ObservedDepartureTime",
    "TimetabledEarliestDepartureHour", "TimetabledEarliestDepartureCalendarWeekDay",
    # Stop details
    "StopTransportAuthorityNumber", "StopAreaNumber", "StopPointLocalNumber",
    "JourneyPatternPointNumber",
    # DateTime
    "TimetabledLatestArrivalDateTime", "ObservedArrivalDateTime", "TargetArrivalDateTime",
    "TimetabledEarliestDepartureDateTime", "ObservedDepartureDateTime", "TargetDepartureDateTime",
    # Distance
    "DistanceToNextStop", "DistanceFromPreviousStop",
    # Coordinates
    "Coord_Latitude", "Coord_Longitude",
    # Additional
    "ArrivalStateNumber", "DepartureStateNumber", "JPPGid", "PreviousJPPGid", "StopAreaGid",
]


# ================================================================
# EXTENSION COLUMN DEFINITIONS (additional columns only)
# ================================================================

JOURNEY_CALL_APC_EXT: list[str] = [
    "OnboardCount", "AlightingCount", "BoardingCount",
    "CapacitySeatingsCount", "CapacityStandingsCount",
]

JOURNEY_CALL_DELAY_EXT: list[str] = [
    "ArrivalDelaySeconds", "ArrivalDelayMinutes", "ArrivalPunctualityText",
    "DepartureDelaySeconds", "DepartureDelayMinutes", "DeparturePunctualityText",
]

JOURNEY_CALL_STATISTICS_EXT: list[str] = [
    "IsFirstStop", "IsLastStop",
    "ArrivalPunctualityCode", "ArrivalPunctualityText",
    "DeparturePunctualityCode", "DeparturePunctualityText",
    "DetectionCompletenessState", "DetectionCompleteness",
    "SignonCompletenessState", "SignOnDetectionCompleteness",
    "LastLinkObservedAvgSpeedKph", "ObservedStopDurationSeconds",
    "TargetArrivalPunctualityCode", "TargetArrivalPunctualityText",
    "TargetDeparturePunctualityCode", "TargetDeparturePunctualityText",
]

JOURNEY_CALL_STOP_LINK_DURATION_EXT: list[str] = [
    "DetectionCompletenessState", "DetectionCompleteness",
    "ObservedAvgSpeedKph", "ObservedStopDurationSeconds", "PlannedStopDurationSeconds",
    "ObservedRunDurationFromPrevStopSeconds", "PlannedRunDurationFromPrevStopSeconds",
    "DiffRunDurationSeconds", "ObservedDurationSinceFirstDepartureSeconds",
    "AccObservedStopDurationSeconds", "AccObservedStopDurationSinceFirstDepartureSeconds",
    "AccObservedRunDurationSeconds", "AccPlannedRunDurationSeconds",
    "AccDiffRunDurationSeconds", "HaltsOnLinkDurationSeconds",
    "AccHaltsOnLinkDurationSeconds",
    "OnboardCount", "AlightingCount", "BoardingCount",
    "ArrivalDelaySeconds", "DepartureDelaySeconds",
]

JOURNEY_CALL_CANCELLED_EXT: list[str] = [
    "CallCancelledYesNo", "JourneyCancelledYesNo",
]

JOURNEY_CALL_DEVIATION_EXT: list[str] = [
    "DMVId", "DCId", "CaseNo", "OperationAction", "SystemName",
    "PartOfMasterCaseNo", "Reason", "RevokedFromDateTime",
    "CreatedDate", "CreatedTime", "CreatedHour", "CreatedWeekDayNumber",
    "CreatedWeekDay", "IsReplacedById", "VersionNumber",
    "PublishFromDateTime", "PublishUpToDateTime", "ReportedByUserName",
    "OnStopAreaName", "OnStopPointDesignation", "OnLineDesignation",
    "OnTechnicalLineNumber", "OnDirectionCode", "OnDirectionName",
    "OnJourneyNumber", "OnSequenceNumber", "Header", "Summary",
    "Details", "ShortText", "PublicNote", "InternalNote",
    "VersionCreatedDateTime",
]

JOURNEY_CALL_MISSING_PASSAGE_EXT: list[str] = ["ReasonType"]

JOURNEY_CALL_EXTRA_JOURNEY_EXT: list[str] = ["ReinforcementType"]

JOURNEY_START_DELAY_EXT: list[str] = [
    "DepartureDelaySeconds", "DepartureDelayMinutes", "DeparturePunctualityText",
]

JOURNEY_BLOCK_FIRST_START_DELAY_EXT: list[str] = [
    "DepartureDelaySeconds", "DepartureDelayMinutes", "DeparturePunctualityText",
]

JOURNEY_MISSING_EXT: list[str] = ["ReasonTypeName"]

JOURNEY_OBSERVED_PATH_EXT: list[str] = [
    "RunByOperatorNumber", "RunByVehicleNumber", "PositionDateTime",
    "Latitude", "Longitude", "SpeedKph", "DirectionDegree",
    "NextLatitude", "NextLongitude", "Note",
]

JOURNEY_OBSERVED_PATH_EXTENDED_EXT: list[str] = [
    "RelativePositionOffsetMeters",
]

JOURNEY_PLANNED_PATH_EXT: list[str] = [
    "EndOfLinkSequenceNumber", "Latitude", "Longitude",
    "NextLatitude", "NextLongitude", "OnLinkOffsetMeters", "SpeedLimitKmPerHour",
]

# Independent tables (have their own full column sets)
JOURNEY_LINK_HALTED_EVENT_COLUMNS: list[str] = [
    "DVJId", "OperatingDate", "SequenceNumber", "LinkNumber",
    "HaltedDurationSeconds",
    "HaltAreaMinLatitude", "HaltAreaMinLongitude",
    "HaltAreaMaxLatitude", "HaltAreaMaxLongitude",
    "HaltAreaCenterLatitude", "HaltAreaCenterLongitude",
    "BirdDistanceFromPrevJPPMeters", "BirdDistanceToNextJPPMeters",
]

JOURNEY_LINK_OBSERVED_PATH_COLUMNS: list[str] = [
    "DVJId", "OperatingDate", "SequenceNumber", "LinkNumber",
    "EndOfLinkSequenceNumber",
]

JOURNEY_LINK_SLOW_EVENT_COLUMNS: list[str] = [
    "DVJId", "OperatingDate", "SequenceNumber", "LinkNumber",
    "SlowDurationSeconds",
    "SlowAreaMinLatitude", "SlowAreaMinLongitude",
    "SlowAreaMaxLatitude", "SlowAreaMaxLongitude",
    "SlowAreaCenterLatitude", "SlowAreaCenterLongitude",
    "BirdDistanceFromPrevJPPMeters", "BirdDistanceToNextJPPMeters",
]

STOP_POINT_BIG_VARIATION_COLUMNS: list[str] = [
    "StopName", "StopTransportAuthorityNumber", "StopAreaNumber",
    "StopPointLocalNumber", "JourneyPatternPointNumber",
    "ObservationCount", "MeanOffsetMeters", "SpreadMeters",
    "CurrentLatitude", "CorrectedLatitude",
    "CurrentLongitude", "CorrectedLongitude",
    "OperatingDate", "JPPGid",
    "LatitudeMedianErrMeters", "LongitudeMedianErrMeters",
]

STOP_POINT_LARGE_OFFSET_COLUMNS: list[str] = [
    "StopName", "StopTransportAuthorityNumber", "StopAreaNumber",
    "StopPointLocalNumber", "JourneyPatternPointNumber",
    "ObservationCount", "MeanOffsetMeters", "SpreadMeters",
    "CurrentLatitude", "CorrectedLatitude",
    "CurrentLongitude", "CorrectedLongitude",
    "OperatingDate", "JPPGid",
    "LatitudeMedianErrMeters", "LongitudeMedianErrMeters",
]

STOP_POINT_POSITION_OFFSET_COLUMNS: list[str] = [
    "StopName", "StopTransportAuthorityNumber", "StopAreaNumber",
    "StopPointLocalNumber", "JourneyPatternPointNumber",
    "ObservationCount", "MeanOffsetMeters", "SpreadMeters",
    "CurrentLatitude", "CorrectedLatitude",
    "CurrentLongitude", "CorrectedLongitude",
    "OperatingDate", "JPPGid",
    "LatitudeMedianErrMeters", "LongitudeMedianErrMeters",
]


# ================================================================
# TABLE DEFINITION REGISTRY
# ================================================================

@dataclass(frozen=True)
class TableDef:
    """Definition of a normalized Delta table."""
    name: str
    description: str
    columns: list[str]
    primary_keys: list[str]
    table_type: str  # 'base', 'extension', or 'independent'
    parent_table: Optional[str] = None


# Map from snake_case table name -> TableDef
TABLE_REGISTRY: dict[str, TableDef] = {
    # Base tables
    "journey": TableDef(
        name="journey", description="Base journey data",
        columns=JOURNEY_BASE_COLUMNS,
        primary_keys=["DVJId", "OperatingDate"], table_type="base",
    ),
    "journey_call": TableDef(
        name="journey_call", description="Base journey call data",
        columns=JOURNEY_CALL_BASE_COLUMNS,
        primary_keys=["DVJId", "SequenceNumber"], table_type="base",
    ),
    # JourneyCall extensions
    "journey_call_apc": TableDef(
        name="journey_call_apc", description="APC data",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_APC_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_delay": TableDef(
        name="journey_call_delay", description="Delay analysis",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_DELAY_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_statistics": TableDef(
        name="journey_call_statistics", description="Statistical analysis",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_STATISTICS_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_stop_and_link_duration": TableDef(
        name="journey_call_stop_and_link_duration", description="Stop and link durations",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_STOP_LINK_DURATION_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_cancelled_or_journey": TableDef(
        name="journey_call_cancelled_or_journey", description="Cancellations",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_CANCELLED_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_deviation_on": TableDef(
        name="journey_call_deviation_on", description="Deviations",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_DEVIATION_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_missing_passage": TableDef(
        name="journey_call_missing_passage", description="Missing passages",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_MISSING_PASSAGE_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    "journey_call_on_extra_journey": TableDef(
        name="journey_call_on_extra_journey", description="Extra journeys",
        columns=["DVJId", "SequenceNumber"] + JOURNEY_CALL_EXTRA_JOURNEY_EXT,
        primary_keys=["DVJId", "SequenceNumber"], table_type="extension",
        parent_table="journey_call",
    ),
    # Journey extensions
    "journey_start_delay": TableDef(
        name="journey_start_delay", description="Start delay",
        columns=["DVJId", "OperatingDate"] + JOURNEY_START_DELAY_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    "journey_block_first_start_delay": TableDef(
        name="journey_block_first_start_delay", description="Block first start delay",
        columns=["DVJId", "OperatingDate"] + JOURNEY_BLOCK_FIRST_START_DELAY_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    "journey_missing": TableDef(
        name="journey_missing", description="Missing journeys",
        columns=["DVJId", "OperatingDate"] + JOURNEY_MISSING_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    "journey_observed_path": TableDef(
        name="journey_observed_path", description="Observed path",
        columns=["DVJId", "OperatingDate"] + JOURNEY_OBSERVED_PATH_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    "journey_observed_path_extended": TableDef(
        name="journey_observed_path_extended", description="Observed path extended",
        columns=["DVJId", "OperatingDate"] + JOURNEY_OBSERVED_PATH_EXTENDED_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    "journey_planned_path": TableDef(
        name="journey_planned_path", description="Planned path",
        columns=["DVJId", "OperatingDate"] + JOURNEY_PLANNED_PATH_EXT,
        primary_keys=["DVJId", "OperatingDate"], table_type="extension",
        parent_table="journey",
    ),
    # Independent tables
    "journey_link_halted_event": TableDef(
        name="journey_link_halted_event", description="Link halted events",
        columns=JOURNEY_LINK_HALTED_EVENT_COLUMNS,
        primary_keys=["DVJId", "SequenceNumber", "LinkNumber"],
        table_type="independent",
    ),
    "journey_link_observed_path": TableDef(
        name="journey_link_observed_path", description="Link observed paths",
        columns=JOURNEY_LINK_OBSERVED_PATH_COLUMNS,
        primary_keys=["DVJId", "SequenceNumber", "LinkNumber"],
        table_type="independent",
    ),
    "journey_link_slow_on_link_event": TableDef(
        name="journey_link_slow_on_link_event", description="Link slow events",
        columns=JOURNEY_LINK_SLOW_EVENT_COLUMNS,
        primary_keys=["DVJId", "SequenceNumber", "LinkNumber"],
        table_type="independent",
    ),
    "stop_point_stop_position_offset": TableDef(
        name="stop_point_stop_position_offset", description="Stop position offsets",
        columns=STOP_POINT_POSITION_OFFSET_COLUMNS,
        primary_keys=["OperatingDate", "StopAreaNumber", "StopPointLocalNumber"],
        table_type="independent",
    ),
    "stop_point_big_variation": TableDef(
        name="stop_point_big_variation", description="Stop big variations",
        columns=STOP_POINT_BIG_VARIATION_COLUMNS,
        primary_keys=["OperatingDate", "StopAreaNumber", "StopPointLocalNumber"],
        table_type="independent",
    ),
    "stop_point_large_offset": TableDef(
        name="stop_point_large_offset", description="Stop large offsets",
        columns=STOP_POINT_LARGE_OFFSET_COLUMNS,
        primary_keys=["OperatingDate", "StopAreaNumber", "StopPointLocalNumber"],
        table_type="independent",
    ),
}


# ================================================================
# SQL VIEW DEFINITIONS
# ================================================================

@dataclass(frozen=True)
class ViewDef:
    """SQL VIEW joining base + extension tables."""
    name: str
    base_table: str
    extension_table: str
    join_keys: list[str]


SQL_VIEW_REGISTRY: dict[str, ViewDef] = {
    "v_journey_call_apc": ViewDef(
        name="v_journey_call_apc", base_table="journey_call",
        extension_table="journey_call_apc", join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_delay": ViewDef(
        name="v_journey_call_delay", base_table="journey_call",
        extension_table="journey_call_delay", join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_statistics": ViewDef(
        name="v_journey_call_statistics", base_table="journey_call",
        extension_table="journey_call_statistics", join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_stop_and_link_duration": ViewDef(
        name="v_journey_call_stop_and_link_duration", base_table="journey_call",
        extension_table="journey_call_stop_and_link_duration",
        join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_cancelled_or_journey": ViewDef(
        name="v_journey_call_cancelled_or_journey", base_table="journey_call",
        extension_table="journey_call_cancelled_or_journey",
        join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_deviation_on": ViewDef(
        name="v_journey_call_deviation_on", base_table="journey_call",
        extension_table="journey_call_deviation_on",
        join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_missing_passage": ViewDef(
        name="v_journey_call_missing_passage", base_table="journey_call",
        extension_table="journey_call_missing_passage",
        join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_call_on_extra_journey": ViewDef(
        name="v_journey_call_on_extra_journey", base_table="journey_call",
        extension_table="journey_call_on_extra_journey",
        join_keys=["DVJId", "SequenceNumber"],
    ),
    "v_journey_start_delay": ViewDef(
        name="v_journey_start_delay", base_table="journey",
        extension_table="journey_start_delay", join_keys=["DVJId", "OperatingDate"],
    ),
    "v_journey_block_first_start_delay": ViewDef(
        name="v_journey_block_first_start_delay", base_table="journey",
        extension_table="journey_block_first_start_delay",
        join_keys=["DVJId", "OperatingDate"],
    ),
    "v_journey_missing": ViewDef(
        name="v_journey_missing", base_table="journey",
        extension_table="journey_missing", join_keys=["DVJId", "OperatingDate"],
    ),
    "v_journey_observed_path": ViewDef(
        name="v_journey_observed_path", base_table="journey",
        extension_table="journey_observed_path", join_keys=["DVJId", "OperatingDate"],
    ),
    "v_journey_observed_path_extended": ViewDef(
        name="v_journey_observed_path_extended", base_table="journey",
        extension_table="journey_observed_path_extended",
        join_keys=["DVJId", "OperatingDate"],
    ),
    "v_journey_planned_path": ViewDef(
        name="v_journey_planned_path", base_table="journey",
        extension_table="journey_planned_path", join_keys=["DVJId", "OperatingDate"],
    ),
}


def generate_view_ddl(catalog: str, schema: str, view_def: ViewDef) -> str:
    """Generate CREATE OR REPLACE VIEW SQL for a view definition."""
    fq_view = f"{catalog}.{schema}.{view_def.name}"
    fq_base = f"{catalog}.{schema}.{view_def.base_table}"
    fq_ext = f"{catalog}.{schema}.{view_def.extension_table}"

    join_cond = " AND ".join(
        f"b.{k} = e.{k}" for k in view_def.join_keys
    )

    # Get extension columns (those not in join keys)
    ext_table_def = TABLE_REGISTRY[view_def.extension_table]
    ext_only_cols = [c for c in ext_table_def.columns if c not in view_def.join_keys]
    ext_select = ", ".join(f"e.{c}" for c in ext_only_cols)

    return f"""CREATE OR REPLACE VIEW {fq_view} AS
SELECT b.*, {ext_select}
FROM {fq_base} b
LEFT JOIN {fq_ext} e ON {join_cond}"""