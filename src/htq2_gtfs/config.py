"""Shared configuration for all HTQ2 GTFS pipeline entry points.

All entry points receive configuration via CLI arguments passed from
the Databricks job YAML. This module provides argument parsing and
typed configuration dataclasses.

v3.1: Added Auto Loader checkpoint_path to PrepConfig.
      6 entry points (ingest, prep, transform, views, validate, load_static)
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


# ── Constants ──────────────────────────────────────────────────────────────────────────

REGION = "skane"
TRAFIKLAB_REALTIME_BASE = "https://opendata.samtrafiken.se/gtfs-rt/{region}"
TRAFIKLAB_STATIC_BASE = "https://opendata.samtrafiken.se/gtfs/{region}"

REALTIME_FEEDS = [
    "ServiceAlerts.pb",
    "TripUpdates.pb",
    "VehiclePositions.pb",
]

STATIC_FILES = [
    "{region}.zip",          # Main static GTFS
    "{region}_extra.zip",    # Extra file with trips_dated_vehicle_journey.txt
]

# Secrets scope and keys (Databricks Secrets)
SECRET_SCOPE = "trafiklab"
SECRET_KEY_REALTIME = "realtime-api-key"
SECRET_KEY_STATIC = "static-api-key"

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = [1, 2, 4]
HTTP_TIMEOUT_SECONDS = 30

# Pipeline metadata table
METADATA_TABLE = "_pipeline_metadata"
QUALITY_RESULTS_TABLE = "_quality_results"

# Default Auto Loader checkpoint path (UC Volume, persistent across runs)
DEFAULT_CHECKPOINT_PATH = "/Volumes/htq2_dev/bronze/trafiklab_raw/checkpoints/autoloader"

# ── Table Group Constants (for --tables parameter) ───────────────────

BASE_TABLES = [
    "journey",
    "journey_call",
]

EXTENSION_TABLES = [
    "journey_call_apc",
    "journey_call_delay",
    "journey_call_statistics",
    "journey_call_stop_and_link_duration",
    "journey_call_cancelled_or_journey",
    "journey_call_deviation_on",
    "journey_call_missing_passage",
    "journey_call_on_extra_journey",
    "journey_start_delay",
    "journey_block_first_start_delay",
    "journey_missing",
    "journey_observed_path",
    "journey_observed_path_extended",
    "journey_planned_path",
]

GPS_TABLES = [
    "journey_link_halted_event",
    "journey_link_observed_path",
    "journey_link_slow_on_link_event",
    "stop_point_stop_position_offset",
    "stop_point_big_variation",
    "stop_point_large_offset",
]

# The 22 silver tables (must match HTQ2 BI_EDA schema)
SILVER_TABLES = BASE_TABLES + EXTENSION_TABLES + GPS_TABLES


# ── Configuration Dataclasses ────────────────────────────────────────────

@dataclass(frozen=True)
class IngestConfig:
    """Configuration for the ingestion entry point."""
    catalog: str
    schema: str = "bronze"
    volume: str = "trafiklab_raw"
    run_mode: str = "realtime"  # 'realtime', 'static', or 'both'
    target_date: Optional[date] = None  # Defaults to today
    region: str = REGION

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def realtime_path(self) -> str:
        return f"{self.volume_path}/realtime/{self.region}"

    @property
    def static_path(self) -> str:
        return f"{self.volume_path}/static"


@dataclass(frozen=True)
class PrepConfig:
    """Configuration for the prep entry point (parse_and_enrich).

    v3.1: Added checkpoint_path for Auto Loader exactly-once processing.
    """
    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    volume: str = "trafiklab_raw"
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH  # Auto Loader checkpoint
    region: str = REGION

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.volume}"

    @property
    def realtime_path(self) -> str:
        return f"{self.volume_path}/realtime/{self.region}"

    @property
    def static_path(self) -> str:
        return f"{self.volume_path}/static"

    @property
    def schema_checkpoint(self) -> str:
        """Checkpoint path for Auto Loader schema inference."""
        return f"{self.checkpoint_path}/schema"

    @property
    def data_checkpoint(self) -> str:
        """Checkpoint path for Auto Loader stream state."""
        return f"{self.checkpoint_path}/data"

    def silver_table(self, table_name: str) -> str:
        """Return fully qualified silver table name (for metadata)."""
        return f"{self.catalog}.{self.silver_schema}.{table_name}"


@dataclass(frozen=True)
class TransformConfig:
    """Configuration for the bronze-to-silver transform entry point."""
    catalog: str
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    volume: str = "trafiklab_raw"
    tables: str = "all"  # 'base', 'gps', or 'all'
    region: str = REGION

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.bronze_schema}/{self.volume}"

    @property
    def realtime_path(self) -> str:
        return f"{self.volume_path}/realtime/{self.region}"

    @property
    def static_path(self) -> str:
        return f"{self.volume_path}/static"

    def silver_table(self, table_name: str) -> str:
        """Return fully qualified silver table name."""
        return f"{self.catalog}.{self.silver_schema}.{table_name}"

    def get_target_tables(self) -> list[str]:
        """Return list of table names to build based on --tables param."""
        if self.tables == "base":
            return BASE_TABLES + EXTENSION_TABLES
        elif self.tables == "gps":
            return GPS_TABLES
        else:  # "all"
            return SILVER_TABLES


@dataclass(frozen=True)
class ViewsConfig:
    """Configuration for the views entry point (create_all_views)."""
    catalog: str
    schema: str = "silver"


@dataclass(frozen=True)
class QualityConfig:
    """Configuration for the quality gate entry point."""
    catalog: str
    schema: str = "silver"
    tables: str = "all"  # 'base', 'gps', or 'all'

    def table(self, table_name: str) -> str:
        """Return fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{table_name}"

    def get_target_tables(self) -> list[str]:
        """Return list of table names to check based on --tables param."""
        if self.tables == "base":
            return BASE_TABLES + EXTENSION_TABLES
        elif self.tables == "gps":
            return GPS_TABLES
        else:  # "all"
            return SILVER_TABLES


@dataclass(frozen=True)
class StaticLoaderConfig:
    """Configuration for the static GTFS loader entry point (manual job)."""
    catalog: str
    schema: str = "bronze"
    volume: str = "trafiklab_raw"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def static_path(self) -> str:
        return f"{self.volume_path}/static"


# ── Argument Parsers ───────────────────────────────────────────────────────

def parse_ingest_args(args: Optional[list[str]] = None) -> IngestConfig:
    """Parse CLI arguments for the ingestion entry point."""
    parser = argparse.ArgumentParser(description="HTQ2 GTFS Ingestion")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="bronze", help="Schema for raw volume")
    parser.add_argument("--volume", default="trafiklab_raw", help="Volume name")
    parser.add_argument("--run_mode", default="realtime",
                        choices=["realtime", "static", "both"],
                        help="What to ingest: realtime feeds, static files, or both")
    parser.add_argument("--target_date", default=None,
                        help="Target date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--region", default=REGION, help="GTFS region")

    parsed = parser.parse_args(args or sys.argv[1:])

    target_date = None
    if parsed.target_date:
        target_date = date.fromisoformat(parsed.target_date)

    return IngestConfig(
        catalog=parsed.catalog,
        schema=parsed.schema,
        volume=parsed.volume,
        run_mode=parsed.run_mode,
        target_date=target_date,
        region=parsed.region,
    )


def parse_prep_args(args: Optional[list[str]] = None) -> PrepConfig:
    """Parse CLI arguments for the prep entry point.

    v3.1: Added --checkpoint_path for Auto Loader.
    """
    parser = argparse.ArgumentParser(description="HTQ2 GTFS Parse & Enrich")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--bronze_schema", default="bronze", help="Bronze schema")
    parser.add_argument("--silver_schema", default="silver", help="Silver schema")
    parser.add_argument("--volume", default="trafiklab_raw", help="Volume name")
    parser.add_argument("--checkpoint_path", default=DEFAULT_CHECKPOINT_PATH,
                        help="Auto Loader checkpoint path (UC Volume)")
    parser.add_argument("--region", default=REGION, help="GTFS region")

    parsed = parser.parse_args(args or sys.argv[1:])

    return PrepConfig(
        catalog=parsed.catalog,
        bronze_schema=parsed.bronze_schema,
        silver_schema=parsed.silver_schema,
        volume=parsed.volume,
        checkpoint_path=parsed.checkpoint_path,
        region=parsed.region,
    )


def parse_transform_args(args: Optional[list[str]] = None) -> TransformConfig:
    """Parse CLI arguments for the transform entry point."""
    parser = argparse.ArgumentParser(description="HTQ2 GTFS Bronze to Silver")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--bronze_schema", default="bronze", help="Bronze schema")
    parser.add_argument("--silver_schema", default="silver", help="Silver schema")
    parser.add_argument("--volume", default="trafiklab_raw", help="Volume name")
    parser.add_argument("--tables", default="all",
                        choices=["base", "gps", "all"],
                        help="Which table group to build: base (16), gps (6), or all (22)")
    parser.add_argument("--region", default=REGION, help="GTFS region")

    parsed = parser.parse_args(args or sys.argv[1:])

    return TransformConfig(
        catalog=parsed.catalog,
        bronze_schema=parsed.bronze_schema,
        silver_schema=parsed.silver_schema,
        volume=parsed.volume,
        tables=parsed.tables,
        region=parsed.region,
    )


def parse_views_args(args: Optional[list[str]] = None) -> ViewsConfig:
    """Parse CLI arguments for the views entry point."""
    parser = argparse.ArgumentParser(description="HTQ2 GTFS Create SQL Views")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="silver", help="Silver schema")

    parsed = parser.parse_args(args or sys.argv[1:])

    return ViewsConfig(
        catalog=parsed.catalog,
        schema=parsed.schema,
    )


def parse_quality_args(args: Optional[list[str]] = None) -> QualityConfig:
    """Parse CLI arguments for the quality gate entry point."""
    parser = argparse.ArgumentParser(description="HTQ2 GTFS Quality Gate")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="silver", help="Silver schema")
    parser.add_argument("--tables", default="all",
                        choices=["base", "gps", "all"],
                        help="Which table group to validate: base, gps, or all")

    parsed = parser.parse_args(args or sys.argv[1:])

    return QualityConfig(
        catalog=parsed.catalog,
        schema=parsed.schema,
        tables=parsed.tables,
    )


def parse_static_loader_args(args: Optional[list[str]] = None) -> StaticLoaderConfig:
    """Parse CLI arguments for the static GTFS loader entry point."""
    parser = argparse.ArgumentParser(description="HTQ2 Static GTFS Loader")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", default="bronze", help="Bronze schema")
    parser.add_argument("--volume", default="trafiklab_raw", help="Volume name")

    parsed = parser.parse_args(args or sys.argv[1:])

    return StaticLoaderConfig(
        catalog=parsed.catalog,
        schema=parsed.schema,
        volume=parsed.volume,
    )
