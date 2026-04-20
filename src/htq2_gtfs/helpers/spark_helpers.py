"""PySpark column-expression helpers for HTQ2 GTFS calculations.

All functions return Spark Column objects — they do NOT collect data
to the driver. Use them inside .withColumn() / .select() chains.

Port of the pandas-based helper functions from the Synapse wheel,
converted to pure PySpark for distributed execution.
"""

from __future__ import annotations

import math

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ── Haversine Distance ───────────────────────────────────────────────────

EARTH_RADIUS_METERS = 6_371_000.0


def haversine_col(
    lat1: Column | str,
    lon1: Column | str,
    lat2: Column | str,
    lon2: Column | str,
) -> Column:
    """Compute haversine distance in meters between two GPS points.

    Args:
        lat1, lon1: Latitude/longitude of point A (degrees).
        lat2, lon2: Latitude/longitude of point B (degrees).

    Returns:
        Spark Column with distance in meters (DoubleType).
    """
    lat1_r = F.radians(F.col(lat1) if isinstance(lat1, str) else lat1)
    lon1_r = F.radians(F.col(lon1) if isinstance(lon1, str) else lon1)
    lat2_r = F.radians(F.col(lat2) if isinstance(lat2, str) else lat2)
    lon2_r = F.radians(F.col(lon2) if isinstance(lon2, str) else lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(lat1_r) * F.cos(lat2_r) * F.pow(F.sin(dlon / 2), 2)
    )
    c = F.lit(2.0) * F.asin(F.sqrt(a))

    return (c * F.lit(EARTH_RADIUS_METERS)).cast(T.DoubleType())


# ── Delay Calculations ───────────────────────────────────────────────────

def delay_seconds_col(
    observed_time: Column | str,
    scheduled_time: Column | str,
) -> Column:
    """Compute delay in seconds (observed - scheduled).

    Positive = late, negative = early, null if either input is null.
    """
    obs = F.col(observed_time) if isinstance(observed_time, str) else observed_time
    sch = F.col(scheduled_time) if isinstance(scheduled_time, str) else scheduled_time
    return (F.unix_timestamp(obs) - F.unix_timestamp(sch)).cast(T.LongType())


def delay_minutes_col(delay_seconds: Column | str) -> Column:
    """Convert delay seconds to delay minutes (rounded to 1 decimal)."""
    sec = F.col(delay_seconds) if isinstance(delay_seconds, str) else delay_seconds
    return F.round(sec / F.lit(60.0), 1).cast(T.DoubleType())


# ── Punctuality ───────────────────────────────────────────────────────────
# HTQ2 punctuality classification (Swedish transit standard):
#   Early     : delay < -60s   (more than 1 min early)
#   OnTime    : -60s ≤ delay ≤ +180s
#   SlightLate: +180s < delay ≤ +360s  (3–6 min late)
#   Late      : delay > +360s  (more than 6 min late)

PUNCTUALITY_EARLY_THRESHOLD = -60
PUNCTUALITY_ONTIME_UPPER = 180
PUNCTUALITY_SLIGHT_LATE_UPPER = 360


def punctuality_code_col(delay_seconds: Column | str) -> Column:
    """Map delay seconds to a numeric punctuality code.

    Returns:
        1 = Early, 2 = OnTime, 3 = SlightLate, 4 = Late, null if delay is null.
    """
    sec = F.col(delay_seconds) if isinstance(delay_seconds, str) else delay_seconds
    return (
        F.when(sec.isNull(), F.lit(None).cast(T.IntegerType()))
        .when(sec < F.lit(PUNCTUALITY_EARLY_THRESHOLD), F.lit(1))
        .when(sec <= F.lit(PUNCTUALITY_ONTIME_UPPER), F.lit(2))
        .when(sec <= F.lit(PUNCTUALITY_SLIGHT_LATE_UPPER), F.lit(3))
        .otherwise(F.lit(4))
    ).cast(T.IntegerType())


def punctuality_col(delay_seconds: Column | str) -> Column:
    """Map delay seconds to a human-readable punctuality text.

    Returns:
        'Early', 'OnTime', 'SlightlyLate', 'Late', or null.
    """
    sec = F.col(delay_seconds) if isinstance(delay_seconds, str) else delay_seconds
    return (
        F.when(sec.isNull(), F.lit(None).cast(T.StringType()))
        .when(sec < F.lit(PUNCTUALITY_EARLY_THRESHOLD), F.lit("Early"))
        .when(sec <= F.lit(PUNCTUALITY_ONTIME_UPPER), F.lit("OnTime"))
        .when(sec <= F.lit(PUNCTUALITY_SLIGHT_LATE_UPPER), F.lit("SlightlyLate"))
        .otherwise(F.lit("Late"))
    )


# ── Speed Calculations ────────────────────────────────────────────────────

def speed_kph_col(
    distance_meters: Column | str,
    duration_seconds: Column | str,
) -> Column:
    """Compute speed in km/h from distance (meters) and duration (seconds).

    Returns null if duration is 0 or null.
    """
    dist = F.col(distance_meters) if isinstance(distance_meters, str) else distance_meters
    dur = F.col(duration_seconds) if isinstance(duration_seconds, str) else duration_seconds
    return (
        F.when((dur.isNull()) | (dur == 0), F.lit(None).cast(T.DoubleType()))
        .otherwise(F.round(dist / dur * F.lit(3.6), 1))
    ).cast(T.DoubleType())


# ── Duration Calculations ─────────────────────────────────────────────────

def duration_seconds_col(
    start_time: Column | str,
    end_time: Column | str,
) -> Column:
    """Compute duration in seconds between two timestamps."""
    st = F.col(start_time) if isinstance(start_time, str) else start_time
    et = F.col(end_time) if isinstance(end_time, str) else end_time
    return (F.unix_timestamp(et) - F.unix_timestamp(st)).cast(T.LongType())


# ── Detection Completeness ───────────────────────────────────────────────

def detection_completeness_col(
    arrival_state: Column | str,
    departure_state: Column | str,
) -> Column:
    """Compute detection completeness fraction (0.0–1.0).

    Both arrival and departure observed = 1.0
    Only one observed = 0.5
    Neither observed = 0.0
    """
    arr = F.col(arrival_state) if isinstance(arrival_state, str) else arrival_state
    dep = F.col(departure_state) if isinstance(departure_state, str) else departure_state

    arr_detected = F.when(arr.isNotNull() & (arr != F.lit("")), F.lit(0.5)).otherwise(F.lit(0.0))
    dep_detected = F.when(dep.isNotNull() & (dep != F.lit("")), F.lit(0.5)).otherwise(F.lit(0.0))

    return (arr_detected + dep_detected).cast(T.DoubleType())


def detection_completeness_state_col(completeness: Column | str) -> Column:
    """Map completeness fraction to a state label.

    Returns: 'Full', 'Partial', or 'None'.
    """
    comp = F.col(completeness) if isinstance(completeness, str) else completeness
    return (
        F.when(comp >= F.lit(1.0), F.lit("Full"))
        .when(comp > F.lit(0.0), F.lit("Partial"))
        .otherwise(F.lit("None"))
    )


# ── Sequence helpers ──────────────────────────────────────────────────────

def is_first_stop_col(sequence_number: Column | str) -> Column:
    """Return 'Yes' if SequenceNumber == 1, else 'No'."""
    seq = F.col(sequence_number) if isinstance(sequence_number, str) else sequence_number
    return F.when(seq == F.lit(1), F.lit("Yes")).otherwise(F.lit("No"))


def is_last_stop_col(
    sequence_number: Column | str,
    last_sequence_number: Column | str,
) -> Column:
    """Return 'Yes' if SequenceNumber equals JourneyLastStopSequenceNumber."""
    seq = F.col(sequence_number) if isinstance(sequence_number, str) else sequence_number
    last = F.col(last_sequence_number) if isinstance(last_sequence_number, str) else last_sequence_number
    return F.when(seq == last, F.lit("Yes")).otherwise(F.lit("No"))
