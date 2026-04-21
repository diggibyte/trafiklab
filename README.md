# HTQ2 GTFS Pipeline

> **Hogia Traffic Quality (HTQ2)** GTFS Realtime processing pipeline for **Skånetrafiken** (Region Skåne, Sweden).  
> Migrated from Azure Synapse to Databricks — produces **22 analytical Delta Lake tables** + **14 SQL views** conforming to the HTQ2 BI_EDA schema.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Databricks](https://img.shields.io/badge/Databricks-DBR_17.3-orange.svg)](https://docs.databricks.com/en/release-notes/runtime/17.3lts.html)
[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](#)

---

## Overview

| Property | Value |
|---|---|
| **Source** | [Trafiklab.se](https://www.trafiklab.se/) GTFS Regional — Skåne |
| **Catalog** | `htq2_dev` / `htq2_staging` / `htq2_prod` (Unity Catalog) |
| **Schemas** | `bronze` (raw data + staging), `silver` (22 tables + 14 views) |
| **Schedule** | Every 10 min (`0 */10 * * * ?`), timezone `Europe/Stockholm` |
| **Runtime** | DBR 17.3 LTS, single-node `Standard_DS4_v2` |
| **Wheel** | `htq2_gtfs-2.0.0-py3-none-any.whl` |

---

## Architecture

### 8-Task DAG (4 Layers)

```
┌─────────────────────── LAYER 1: INGESTION (parallel) ───────────────────────┐
│                                                                              │
│   ingest_realtime                         ingest_static                      │
│   ├── ServiceAlerts.pb    (~5 KB)         ├── skane_static_*.zip   (~80 MB)  │
│   ├── TripUpdates.pb      (~250 KB)       └── skane_extra_*.zip    (~60 MB)  │
│   └── VehiclePositions.pb (~150 KB)           (if stale > 24h)               │
│                                                                              │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │   LAYER 2: PREP                      │
              │   parse_and_enrich                   │
              │   ├── Protobuf parsing               │
              │   ├── DVJId resolution (2.4M entries) │
              │   ├── Static GTFS enrichment         │
              │   │   (trips + routes + stops +       │
              │   │    stop_times + shapes)           │
              │   └── MERGE → bronze staging tables   │
              └──────────────────┬──────────────────┘
                                 │
┌────────────────────────────────▼─────────────────────────────────────────────┐
│                        LAYER 3: SILVER (parallel)                            │
│                                                                              │
│   build_base_tables          build_gps_tables        create_all_views        │
│   ├── journey                ├── journey_link_*      ├── 14 SQL views        │
│   ├── journey_call           └── stop_point_*        └── (v_* prefix, LEFT   │
│   ├── 10 extension tables        (6 tables)              JOIN base+ext)      │
│   └── 4 path tables                                                          │
│       (16 tables)                                    (depends on base)        │
│                                                                              │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │
              ┌──────────────────▼──────────────────┐
              │   LAYER 4: QUALITY                   │
              │   quality_gate                       │
              │   ├── Schema validation              │
              │   ├── Completeness checks            │
              │   └── Business rule checks           │
              │       (29 checks total)              │
              └─────────────────────────────────────┘
```

All tasks run as `python_wheel_task` entries from a single wheel, orchestrated by a Databricks Job defined in Asset Bundle YAML.

---

## Project Structure

```
htq2-gtfs-pipeline/
├── databricks.yml                      # Asset Bundle config (dev / staging / prod targets)
├── pyproject.toml                      # Wheel build config, entry points, linting
├── resources/
│   └── htq2_pipeline_job.yml           # Job definition (8 python_wheel_tasks, 4 layers)
├── dist/
│   └── htq2_gtfs-2.0.0-py3-none-any.whl
├── src/htq2_gtfs/
│   ├── config.py                       # Shared config + CLI arg parsing
│   ├── ingestion/                      # Layer 1: Trafiklab API → UC Volume
│   │   ├── main.py                     #   Entry point (realtime / static mode)
│   │   ├── trafiklab_client.py         #   HTTP client with retry + backoff
│   │   └── static_manager.py           #   Staleness check (24h threshold)
│   ├── prep/                           # Layer 2: Parse + Enrich → Bronze staging
│   │   ├── main.py                     #   Entry point
│   │   └── validator.py                #   Inline validation (fail-fast)
│   ├── processing/                     # Layer 3a: Bronze → Silver tables
│   │   ├── main.py                     #   Entry point (--tables base|gps|all)
│   │   ├── core.py                     #   Protobuf parsing + static enrichment
│   │   ├── models.py                   #   22 table schemas, 14 view defs, PKs
│   │   ├── view_builder.py             #   Builds all 22 tables (1014 lines)
│   │   ├── watermark.py                #   File tracking (_pipeline_metadata)
│   │   └── writer.py                   #   Delta write + schema alignment
│   ├── views/                          # Layer 3b: SQL view creation
│   │   └── main.py                     #   CREATE OR REPLACE VIEW (14 views)
│   ├── quality/                        # Layer 4: Quality gate
│   │   ├── main.py                     #   Entry point (--tables all)
│   │   ├── checks.py                   #   29 validation checks
│   │   └── reporter.py                 #   Results → _quality_results table
│   └── helpers/
│       ├── spark_helpers.py            #   haversine_col, punctuality, speed UDFs
│       ├── file_utils.py              #   RealtimeFileSet, find_best_static_file
│       └── logging_config.py          #   Structured logging
├── tests/                              # pytest test suite
├── setup_secrets.ipynb                 # One-time secrets provisioning
└── README.md
```

---

## Entry Points

| Entry Point | Layer | Description | Key Parameters |
|---|---|---|---|
| `ingest` | 1 | Fetch GTFS data from Trafiklab API | `--run_mode realtime\|static` |
| `prep` | 2 | Parse protobuf + static enrichment | `--bronze_schema`, `--silver_schema` |
| `transform` | 3 | Build silver Delta tables | `--tables base\|gps\|all` |
| `views` | 3 | Create 14 SQL views | `--schema silver` |
| `validate` | 4 | Run quality gate checks | `--tables all` |

All entry points accept `--catalog` (e.g., `htq2_dev`) for target environment.

---

## Silver Tables

### Base Tables (2)
| Table | PK | Description |
|---|---|---|
| `journey` | DVJId, OperatingDate | One row per dated vehicle journey |
| `journey_call` | DVJId, SequenceNumber | One row per stop call within a journey |

### Extension Tables (14)
| Table | Extends | Key Columns |
|---|---|---|
| `journey_call_delay` | journey_call | ArrivalDelaySeconds, DepartureDelaySeconds |
| `journey_call_statistics` | journey_call | Punctuality codes, completeness, ObservedStopDuration |
| `journey_call_apc` | journey_call | Passenger counts (requires APC hardware) |
| `journey_call_stop_and_link_duration` | journey_call | Duration, speed, cumulative run times |
| `journey_call_deviation_on` | journey_call | Deviation management (requires Hogia SA) |
| `journey_call_cancelled_or_journey` | journey_call | Cancellation flags |
| `journey_call_missing_passage` | journey_call | Missing passage detection |
| `journey_call_on_extra_journey` | journey_call | Extra journey flags |
| `journey_start_delay` | journey | Start delay in seconds |
| `journey_block_first_start_delay` | journey | First journey in block delay |
| `journey_missing` | journey | Missing journey detection |
| `journey_observed_path` | journey | GPS breadcrumb trail (VP extraction) |
| `journey_observed_path_extended` | journey | Cumulative position offset (haversine) |
| `journey_planned_path` | journey | Route geometry from shapes.txt |

### GPS Tables (6)
| Table | PK | Description |
|---|---|---|
| `journey_link_observed_path` | DVJId, Seq, Link | GPS points assigned to links |
| `journey_link_halted_event` | DVJId, Seq, Link | Halt detection (speed < 3 kph) |
| `journey_link_slow_on_link_event` | DVJId, Seq, Link | Slow detection (3-15 kph) |
| `stop_point_stop_position_offset` | Date, StopArea, StopPoint | GPS offset from planned stop |
| `stop_point_big_variation` | Date, StopArea, StopPoint | Large position variations |
| `stop_point_large_offset` | Date, StopArea, StopPoint | Stops with significant offsets |

### SQL Views (14)
All prefixed with `v_` — LEFT JOIN of base table (`journey` or `journey_call`) with extension tables. E.g., `v_journey_call_delay` = `journey_call` LEFT JOIN `journey_call_delay`.

### Metadata Tables (2)
| Table | Purpose |
|---|---|
| `_pipeline_metadata` | File watermark tracking (prevents reprocessing) |
| `_quality_results` | Quality check results per run |

---

## Data Sources

| Feed | File | Size | Content |
|---|---|---|---|
| Realtime | `ServiceAlerts_skane.pb` | ~5 KB | Cancellations, deviations |
| Realtime | `TripUpdates_skane.pb` | ~250 KB | Per-stop arrival/departure delays |
| Realtime | `VehiclePositions_skane.pb` | ~150 KB | GPS lat/lng/speed/bearing per vehicle |
| Static | `skane_static_YYYY-MM-DD.zip` | ~80 MB | routes, trips, stop_times, stops, shapes |
| Static | `skane_extra_YYYY-MM-DD.zip` | ~60 MB | `trips_dated_vehicle_journey.txt` (DVJId mapping) |

### Enrichment Pipeline

The `prep` task performs full static enrichment, resolving empty fields in Skånetrafiken's GTFS-RT feed:

- **DVJId resolution** — `trips_dated_vehicle_journey.txt` (2.4M entries) maps `trip_id` → `DVJId`
- **Route enrichment** — `trips.txt` + `routes.txt` → `LineNumber`, `DirectionNumber`, `ExtendedLineDesignation`
- **Stop enrichment** — `stop_times.txt` + `stops.txt` → `StopName`, `Coord_Latitude/Longitude`, `JourneyOriginName`, `JourneyEndStopName`
- **Time derivation** — Timetabled times derived from `observed_time - delay_seconds`
- **State derivation** — ARRIVED/DEPARTED based on delay presence, mapped to state numbers
- **Shapes parsing** — `shapes.txt` → planned route geometry per journey

---

## Quick Start

### Prerequisites

- Python 3.10+
- Databricks CLI configured (`databricks configure`)
- Databricks Secrets scope `trafiklab` with keys:
  - `realtime-api-key` — Trafiklab GTFS Regional realtime API key
  - `static-api-key` — Trafiklab GTFS Regional static API key

### Development Setup

```bash
# Clone and install in editable mode
cd htq2-gtfs-pipeline
pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/
mypy src/
```

### Build Wheel

```bash
python -m build --wheel
# Output: dist/htq2_gtfs-2.0.0-py3-none-any.whl
```

### Deploy to Databricks

```bash
# Deploy to dev (schedule PAUSED)
databricks bundle deploy -t dev

# Run a one-off execution
databricks bundle run htq2_gtfs_pipeline -t dev

# Deploy to staging / prod (schedule active)
databricks bundle deploy -t staging
databricks bundle deploy -t prod
```

---

## Configuration

All configuration via CLI parameters — no hardcoded values:

```yaml
# resources/htq2_pipeline_job.yml
parameters:
  - "--catalog"
  - "${var.catalog}"      # htq2_dev / htq2_staging / htq2_prod
  - "--bronze_schema"
  - "bronze"
  - "--silver_schema"
  - "silver"
```

Environment-specific variables are defined per target in `databricks.yml`.

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Append write strategy** | Matches original Synapse behavior. Watermark prevents reprocessing same files. |
| **DVJId as StringType** | Avoids Arrow conversion errors with large GID identifiers (e.g., `9015012080201077`) |
| **Single shared cluster** | Cost-optimized for POC. 8 tasks share one `Standard_DS4_v2` node. |
| **ISO 8601 timestamp parsing** | `unix_timestamp(col, "yyyy-MM-dd'T'HH:mm:ssXXX")` — Skånetrafiken uses `2026-04-19T10:30:00+00:00` format |
| **No Window dedup** | Removed to match Synapse — no ROW_NUMBER deduplication within batches |
| **`F.lit(None).cast(type)`** | Bare `F.lit(None)` creates void/NullType columns that crash Photon on DBR 17.3 |

---

## Performance

| Metric | Value |
|---|---|
| Pipeline end-to-end (warm) | ~2 min |
| Pipeline end-to-end (cold) | ~6 min |
| Ingestion | ~6 sec |
| Transform | ~20 sec |
| Quality gate | ~25 sec |
| Wheel size | 57,188 bytes |
| Static enrichment coverage | 42,927 trips, 583 routes, 10,629 stops |

---

## License

Proprietary — [Hogia Group](https://www.hogia.se/) / [DiggiByte Solutions](https://diggibyte.com/)
