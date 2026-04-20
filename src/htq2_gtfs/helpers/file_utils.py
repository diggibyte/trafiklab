"""File utilities for the HTQ2 GTFS pipeline.

Handles static GTFS file discovery, completeness checks for realtime
feeds, and volume path operations. Works with UC Volumes via FUSE.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from htq2_gtfs.helpers.logging_config import get_logger

logger = get_logger(__name__)

# Realtime feed filenames (3 per complete set)
REALTIME_FEEDS = {"ServiceAlerts.pb", "TripUpdates.pb", "VehiclePositions.pb"}

# Note: Synapse production has a typo "SeviceAlerts" — we handle both
REALTIME_FEEDS_WITH_TYPO = REALTIME_FEEDS | {"SeviceAlerts.pb"}

# Timestamp pattern in filenames: 2025-04-16T08-30-00Z-skane-TripUpdates.pb
FILENAME_TS_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)-(.+?)-(\w+\.pb)$"
)


@dataclass
class RealtimeFileSet:
    """A group of 3 protobuf files from a single ingestion timestamp."""
    timestamp: str          # e.g. "2025-04-16T08-30-00Z"
    region: str             # e.g. "skane"
    files: dict[str, str]   # feed_name -> full path
    is_complete: bool       # True if all 3 feeds present


def find_best_static_file(
    static_path: str,
    target_date: date,
    file_prefix: str = "skane_static_",
    file_ext: str = ".zip",
) -> Optional[str]:
    """Find the best-matching static GTFS file for a given operating date.

    Strategy:
    1. Look for exact date match first (e.g. skane_static_2025-04-16.zip)
    2. Fall back to the most recent file before target_date
    3. Return None if no static file exists

    Args:
        static_path: Directory path (FUSE-mounted UC Volume).
        target_date: The operating date to find static data for.
        file_prefix: Filename prefix to match.
        file_ext: File extension to match.

    Returns:
        Full path to the best static file, or None.
    """
    if not os.path.isdir(static_path):
        logger.warning(f"Static path does not exist: {static_path}")
        return None

    candidates: list[tuple[date, str]] = []

    for fname in os.listdir(static_path):
        if not (fname.startswith(file_prefix) and fname.endswith(file_ext)):
            continue
        # Extract date from filename: skane_static_2025-04-16.zip
        date_str = fname[len(file_prefix):-len(file_ext)]
        try:
            file_date = date.fromisoformat(date_str)
            candidates.append((file_date, os.path.join(static_path, fname)))
        except ValueError:
            logger.debug(f"Skipping file with unparseable date: {fname}")
            continue

    if not candidates:
        logger.warning(f"No static files found in {static_path}")
        return None

    # Sort by date descending
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Exact match
    for file_date, file_path in candidates:
        if file_date == target_date:
            logger.info(f"Exact static file match: {file_path}")
            return file_path

    # Most recent before target date
    for file_date, file_path in candidates:
        if file_date <= target_date:
            logger.info(f"Using static file from {file_date} for target {target_date}: {file_path}")
            return file_path

    # No file before target date — use the earliest available
    earliest = candidates[-1]
    logger.warning(
        f"No static file on or before {target_date}. Using earliest: {earliest[1]}"
    )
    return earliest[1]


def list_realtime_files(
    realtime_path: str,
    processed_files: set[str] | None = None,
) -> list[RealtimeFileSet]:
    """List and group realtime protobuf files in the Volume.

    Groups files by timestamp, checks completeness (all 3 feeds present),
    and excludes already-processed files.

    Args:
        realtime_path: Path to the realtime directory in the Volume.
        processed_files: Set of already-processed filenames to skip.

    Returns:
        List of RealtimeFileSet, sorted by timestamp ascending.
    """
    if not os.path.isdir(realtime_path):
        logger.warning(f"Realtime path does not exist: {realtime_path}")
        return []

    processed = processed_files or set()
    groups: dict[str, dict[str, str]] = {}  # timestamp -> {feed: path}
    region_by_ts: dict[str, str] = {}

    for fname in os.listdir(realtime_path):
        if fname in processed:
            continue

        match = FILENAME_TS_PATTERN.match(fname)
        if not match:
            # Handle files without timestamp (legacy or typo format)
            for feed in REALTIME_FEEDS_WITH_TYPO:
                if fname.endswith(feed):
                    logger.debug(f"Non-standard filename format: {fname}")
            continue

        ts, region, feed = match.groups()
        # Normalize typo: SeviceAlerts -> ServiceAlerts
        if feed == "SeviceAlerts.pb":
            feed = "ServiceAlerts.pb"

        if ts not in groups:
            groups[ts] = {}
            region_by_ts[ts] = region
        groups[ts][feed] = os.path.join(realtime_path, fname)

    # Build RealtimeFileSet objects
    result = []
    for ts in sorted(groups.keys()):
        feeds = groups[ts]
        result.append(RealtimeFileSet(
            timestamp=ts,
            region=region_by_ts[ts],
            files=feeds,
            is_complete=REALTIME_FEEDS.issubset(feeds.keys()),
        ))

    complete_count = sum(1 for r in result if r.is_complete)
    logger.info(
        f"Found {len(result)} realtime file sets ({complete_count} complete)",
        extra={"file_count": len(result)},
    )
    return result


def check_realtime_completeness(
    file_sets: list[RealtimeFileSet],
    min_complete_sets: int = 1,
) -> bool:
    """Check if enough complete realtime file sets exist for processing.

    The Synapse system requires at least one complete set (3 files)
    to trigger a processing run.
    """
    complete = sum(1 for fs in file_sets if fs.is_complete)
    return complete >= min_complete_sets
