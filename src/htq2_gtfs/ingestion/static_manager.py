"""Static GTFS file management for HTQ2 pipeline.

Handles daily static ZIP downloads (schedule + extra DVJId mapping),
staleness checks to avoid redundant downloads, and file-level
validation.
"""

from __future__ import annotations

import os
import zipfile
from datetime import date
from typing import Optional

from htq2_gtfs.config import IngestConfig
from htq2_gtfs.helpers.logging_config import get_logger
from htq2_gtfs.ingestion.trafiklab_client import TrafiklabClient

logger = get_logger(__name__)

# Expected file inside the extra ZIP for DVJId mapping
DVJ_MAPPING_FILE = "trips_dated_vehicle_journey.txt"


def is_static_stale(
    static_path: str,
    target_date: date,
    file_prefix: str = "skane_static_",
) -> bool:
    """Check if static GTFS data needs to be re-downloaded.

    Static data is considered stale if:
    - No static file exists for today's date
    - The directory is empty

    Static files are published once per day. We download them once
    and reuse until a new date's file is available.
    """
    if not os.path.isdir(static_path):
        logger.info(f"Static path does not exist — needs download: {static_path}")
        return True

    expected_name = f"{file_prefix}{target_date.isoformat()}.zip"
    expected_path = os.path.join(static_path, expected_name)

    if os.path.isfile(expected_path):
        size = os.path.getsize(expected_path)
        if size > 0:
            logger.info(f"Static file exists and is valid: {expected_name} ({size:,} bytes)")
            return False
        else:
            logger.warning(f"Static file exists but is empty: {expected_name}")
            return True

    logger.info(f"Static file missing for {target_date}: {expected_name}")
    return True


def download_static_if_needed(
    client: TrafiklabClient,
    config: IngestConfig,
    target_date: date,
) -> dict[str, str]:
    """Download static GTFS files if they are stale.

    Downloads both the main static ZIP (schedule data) and the
    extra ZIP (trips_dated_vehicle_journey.txt for DVJId mapping).

    Args:
        client: Authenticated Trafiklab HTTP client.
        config: Ingestion configuration.
        target_date: Date to check/download for.

    Returns:
        Dict mapping filename to local file path for downloaded files.
        Empty dict if no download was needed.
    """
    static_path = config.static_path
    os.makedirs(static_path, exist_ok=True)

    if not is_static_stale(static_path, target_date):
        return {}

    logger.info(f"Downloading static files for {target_date}...")

    downloaded: dict[str, str] = {}
    static_data = client.fetch_all_static()

    for filename, content in static_data.items():
        # Rename with date: skane.zip -> skane_static_2025-04-16.zip
        if filename.endswith("_extra.zip"):
            target_name = f"{config.region}_extra_{target_date.isoformat()}.zip"
        else:
            target_name = f"{config.region}_static_{target_date.isoformat()}.zip"

        target_path = os.path.join(static_path, target_name)

        with open(target_path, "wb") as f:
            f.write(content)

        downloaded[target_name] = target_path
        logger.info(
            f"Saved static file: {target_name} ({len(content):,} bytes)",
            extra={"status": "success"},
        )

    # Validate the extra ZIP contains DVJId mapping
    _validate_extra_zip(downloaded, config.region, target_date)

    return downloaded


def _validate_extra_zip(
    downloaded: dict[str, str],
    region: str,
    target_date: date,
) -> None:
    """Verify the extra ZIP contains trips_dated_vehicle_journey.txt."""
    extra_name = f"{region}_extra_{target_date.isoformat()}.zip"
    extra_path = downloaded.get(extra_name)

    if not extra_path or not os.path.isfile(extra_path):
        logger.warning(f"Extra ZIP not found: {extra_name}")
        return

    try:
        with zipfile.ZipFile(extra_path, "r") as zf:
            names = zf.namelist()
            if DVJ_MAPPING_FILE in names:
                info = zf.getinfo(DVJ_MAPPING_FILE)
                logger.info(
                    f"DVJId mapping file found in extra ZIP: "
                    f"{DVJ_MAPPING_FILE} ({info.file_size:,} bytes)"
                )
            else:
                logger.error(
                    f"DVJId mapping file MISSING from extra ZIP! "
                    f"Expected: {DVJ_MAPPING_FILE}. Found: {names[:5]}"
                )
    except zipfile.BadZipFile as e:
        logger.error(f"Corrupt extra ZIP: {extra_name}: {e}")
