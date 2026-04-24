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
| **Schemas** | `bronze` (raw volume + staging tables), `silver` (22 tables + 14 views) |
| **Main Pipeline** | Every 15 min (`0 */15 * * * ?`), timezone `Europe/Stockholm` |
| **Static Loader** | Manual trigger (separate job, run when new ZIP uploaded) |
| **Runtime** | DBR 17.3 LTS, single-node `Standard_DS4_v2` |
| **Wheel** | `htq2_gtfs-3.1.0-py3-none-any.whl` |

---

## Architecture (v3.1.0)

### Two Workflows

**Workflow 1 — Main Pipeline** (scheduled every 15 min, 6-task DAG):

```
ingest_realtime -> parse_and_enrich -> build_base_tables -> create_all_views -> quality_gate
                                    -> build_gps_tables  /
```

**Workflow 2 — Static GTFS Loader** (manual trigger, 1 task):

```
load_static_gtfs  ->  6 bronze static tables (OVERWRITE)
```

### Data Flow: API -> Volume -> Bronze -> Silver -> Views

```
+--- LAYER 1: INGESTION ------------------------------------------+
|                                                                   |
|   ingest_realtime (every 15 min)                                  |
|   +-- TripUpdates.pb      (~500 KB)                               |
|   +-- VehiclePositions.pb (~85 KB)  -> /Volumes/.../realtime/*.pb |
|   +-- ServiceAlerts.pb    (~90 KB)                                |
|                                                                   |
+-----------------------------+-------------------------------------+
                              |
+-----------------------------v-------------------------------------+
|   LAYER 2: PREP (Auto Loader + Protobuf Parsing)                  |
|                                                                    |
|   parse_and_enrich                                                 |
|   +-- Auto Loader (cloudFiles + binaryFile)                        |
|   |   +-- Checkpoint: dbfs:/htq2/checkpoints/autoloader            |
|   |   +-- Writes raw binary -> bronze._autoloader_raw_pb           |
|   +-- Protobuf parsing (main process, NOT foreachBatch)            |
|   |   +-- Static enrichment (DVJId, routes, stops, trips)          |
|   +-- APPEND -> bronze.parsed_journey                              |
|              -> bronze.parsed_call                                  |
|              -> bronze.parsed_vehicle_positions                     |
|                                                                    |
+-----------------------------+--------------------------------------+
                              |
+-----------------------------v--------------------------------------+
|   LAYER 3: SILVER BUILD (parallel)                                 |
|                                                                    |
|   build_base_tables           build_gps_tables                     |
|   +-- journey (base)          +-- journey_link_halted_event        |
|   +-- journey_call (base)     +-- journey_link_observed_path       |
|   +-- 14 extension tables     +-- journey_link_slow_on_link_event  |
|       (16 tables total)       +-- stop_point_stop_position_offset  |
|                               +-- stop_point_big_variation         |
|   create_all_views            +-- stop_point_large_offset          |
|   +-- 14 SQL views                (6 tables total)                 |
|       (v_* LEFT JOIN base+ext)                                     |
|                                                                    |
+-----------------------------+--------------------------------------+
                              |
+-----------------------------v--------------------------------------+
|   LAYER 4: QUALITY                                                 |
|                                                                    |
|   quality_gate                                                     |
|   +-- 29 validation checks (schema, completeness, business rules)  |
|   +-- Results -> silver._quality_results                           |
|       Exit code 1 on critical failure                              |
|                                                                    |
+--------------------------------------------------------------------+
```

All tasks run as `python_wheel_task` entries from a single wheel, orchestrated by Databricks Asset Bundle jobs.

---

## Key Architecture Decisions (v3.1.0)

### 1. APPEND-Only Writes (No MERGE)
GTFS-RT is event data — each .pb snapshot is a unique observation, not a row to update. All writes use APPEND mode, matching the original Synapse pattern.

### 2. Auto Loader (No Manual Watermark)
Eliminates ~80 lines of custom watermark code. Built-in exactly-once semantics via checkpoint at `dbfs:/htq2/checkpoints/autoloader`. Verified: Re-run processed only new files, zero duplicates.

### 3. No foreachBatch (Job Cluster Compatibility)
On job clusters, `foreachBatch` runs callbacks in a **separate Python process** (Spark Connect). Main-process globals are `None` inside the callback. Solution: Auto Loader writes raw binary to an intermediate staging table, then protobuf parsing happens in the main process.

### 4. Run-ID Filtering
Staging tables accumulate data across runs (APPEND). Each run has a UUID `_run_id`. Downstream tasks filter by latest run_id from `_pipeline_metadata` to process only the current run's data.

### 5. Separate Static Loader
Static GTFS changes infrequently (monthly). Runs as a separate manual job. Extracts to Volume `_tmp_extract/` (not local `/tmp` — fixes 'cannot access shared' on job clusters). Cleans up both CSVs and ZIPs after load.

---

## Project Structure

```
htq2-gtfs-pipeline/
+-- databricks.yml                      # Asset Bundle config (dev / staging / prod)
+-- pyproject.toml                      # Wheel build, 6 entry points, linting
+-- resources/
|   +-- htq2_pipeline_job.yml           # Main pipeline (6 tasks, 15-min schedule)
|   +-- htq2_static_loader_job.yml      # Static loader (1 task, manual trigger)
+-- dist/
|   +-- htq2_gtfs-3.1.0-py3-none-any.whl
+-- src/htq2_gtfs/
|   +-- config.py                       # Shared config + CLI arg parsing (6 configs)
|   +-- ingestion/                      # Layer 1: Trafiklab API -> UC Volume
|   |   +-- main.py                     #   Entry point (realtime mode)
|   |   +-- trafiklab_client.py         #   HTTP client with retry + backoff
|   |   +-- static_loader.py            #   Static ZIP -> 6 bronze tables (OVERWRITE)
|   |   +-- static_manager.py           #   Staleness check utility
|   +-- prep/                           # Layer 2: Auto Loader + Parse -> Bronze staging
|   |   +-- main.py                     #   Auto Loader -> raw binary -> protobuf parse
|   |   +-- validator.py                #   Inline validation (fail-fast)
|   +-- processing/                     # Layer 3: Bronze -> Silver tables
|   |   +-- main.py                     #   Entry point (--tables base|gps|all)
|   |   +-- core.py                     #   GTFSProcessor (protobuf + static enrichment)
|   |   +-- models.py                   #   22 table schemas, 14 view defs, PKs
|   |   +-- view_builder.py             #   ViewBuilder — builds all 22 tables
|   |   +-- writer.py                   #   Delta APPEND write + schema alignment
|   +-- views/                          # Layer 3: SQL view creation
|   |   +-- main.py                     #   CREATE OR REPLACE VIEW (14 views)
|   +-- quality/                        # Layer 4: Quality gate
|   |   +-- main.py                     #   Entry point
|   |   +-- checks.py                   #   29 validation checks
|   |   +-- reporter.py                 #   Results -> _quality_results table
|   +-- helpers/
|       +-- spark_helpers.py            #   haversine, punctuality, speed UDFs
|       +-- file_utils.py              #   RealtimeFileSet, find_best_static_file
|       +-- logging_config.py          #   Structured logging
+-- tests/                              # pytest test suite
+-- setup_secrets.ipynb                 # One-time secrets provisioning
+-- HTQ2_PIPELINE_CONTEXT.md            # Detailed implementation log
+-- README.md
```

---

## Entry Points (6)

| Entry Point | Module | Layer | Description | Key Parameters |
|---|---|---|---|---|
| `ingest` | `ingestion.main` | 1 | Fetch GTFS-RT from Trafiklab API -> Volume | `--run_mode realtime` |
| `prep` | `prep.main` | 2 | Auto Loader + protobuf parse -> staging | `--checkpoint_path` |
| `transform` | `processing.main` | 3 | Staging -> silver Delta tables (APPEND) | `--tables base/gps/all` |
| `views` | `views.main` | 3 | Create 14 SQL views | `--schema silver` |
| `validate` | `quality.main` | 4 | Run 29 quality checks | `--tables all` |
| `load_static` | `ingestion.static_loader` | - | ZIP -> 6 bronze static tables (OVERWRITE) | `--volume trafiklab_raw` |

All entry points accept `--catalog` (e.g., `htq2_dev`) for target environment.

---

## File-to-Table Mapping

### Realtime API -> Bronze Staging

| API Feed | Volume File Pattern | Bronze Staging Table | Content |
|---|---|---|---|
| TripUpdates.pb | `*-TripUpdates.pb` | `parsed_journey` | Journey metadata (1 per trip per snapshot) |
| TripUpdates.pb | `*-TripUpdates.pb` | `parsed_call` | Stop-level delays (N per journey) |
| VehiclePositions.pb | `*-VehiclePositions.pb` | `parsed_vehicle_positions` | GPS breadcrumbs (1 per vehicle per snapshot) |
| ServiceAlerts.pb | `*-ServiceAlerts.pb` | *(logged, not persisted)* | Service disruptions |

### Bronze Staging -> Silver Tables

| Bronze Source | Silver Tables |
|---|---|
| `parsed_journey` | `journey`, `journey_start_delay`, `journey_block_first_start_delay`, `journey_missing`, `journey_observed_path`, `journey_observed_path_extended`, `journey_planned_path` |
| `parsed_call` | `journey_call`, `journey_call_apc`, `journey_call_delay`, `journey_call_statistics`, `journey_call_stop_and_link_duration`, `journey_call_cancelled_or_journey`, `journey_call_deviation_on`, `journey_call_missing_passage`, `journey_call_on_extra_journey` |
| `parsed_vehicle_positions` | `journey_link_halted_event`, `journey_link_observed_path`, `journey_link_slow_on_link_event`, `stop_point_stop_position_offset`, `stop_point_big_variation`, `stop_point_large_offset` |

### Static GTFS ZIPs -> Bronze Static Tables

| ZIP File | CSV Inside | Bronze Static Table | Rows |
|---|---|---|---|
| `skane_static_*.zip` | `routes.txt` | `static_routes` | 584 |
| `skane_static_*.zip` | `trips.txt` | `static_trips` | 67,138 |
| `skane_static_*.zip` | `stops.txt` | `static_stops` | 10,635 |
| `skane_static_*.zip` | `stop_times.txt` | `static_stop_times` | 1,419,569 |
| `skane_static_*.zip` | `shapes.txt` | `static_shapes` | 2,163,301 |
| `skane_extra_*.zip` | `trips_dated_vehicle_journey.txt` | `static_dvj_mapping` | 1,852,501 |

---

## Silver Tables (22)

### Base Tables (2)

| Table | PK | Description |
|---|---|---|
| `journey` | DVJId, OperatingDate | One row per dated vehicle journey (36 cols) |
| `journey_call` | DVJId, SequenceNumber | One row per stop call within a journey (67 cols) |

### Extension Tables (14)

| Table | Extends | Key Columns |
|---|---|---|
| `journey_call_delay` | journey_call | ArrivalDelaySeconds, DepartureDelaySeconds, punctuality |
| `journey_call_statistics` | journey_call | Punctuality codes, detection completeness |
| `journey_call_apc` | journey_call | Passenger counts (boarding, alighting, onboard) |
| `journey_call_stop_and_link_duration` | journey_call | Run/stop durations, cumulative times, speeds |
| `journey_call_deviation_on` | journey_call | Deviation management cases |
| `journey_call_cancelled_or_journey` | journey_call | Cancellation flags |
| `journey_call_missing_passage` | journey_call | Missing passage detection |
| `journey_call_on_extra_journey` | journey_call | Extra journey reinforcement type |
| `journey_start_delay` | journey | First-stop departure delay |
| `journey_block_first_start_delay` | journey | Block first journey departure delay |
| `journey_missing` | journey | Missing journey reason |
| `journey_observed_path` | journey | GPS breadcrumb trail (VP extraction) |
| `journey_observed_path_extended` | journey | + cumulative position offset (haversine) |
| `journey_planned_path` | journey | Route geometry from static shapes.txt |

### GPS/Independent Tables (6)

| Table | PK | Description |
|---|---|---|
| `journey_link_observed_path` | DVJId, Seq, Link | GPS points assigned to links |
| `journey_link_halted_event` | DVJId, Seq, Link | Halt detection (speed < 3 kph) |
| `journey_link_slow_on_link_event` | DVJId, Seq, Link | Slow detection (3-15 kph) |
| `stop_point_stop_position_offset` | Date, StopArea, StopPoint | GPS offset from planned stop |
| `stop_point_big_variation` | Date, StopArea, StopPoint | Large position variations |
| `stop_point_large_offset` | Date, StopArea, StopPoint | Stops with significant offsets |

### SQL Views (14)

All prefixed with `v_` — LEFT JOIN of base table with extension table on PKs.
Example: `v_journey_call_delay` = `journey_call LEFT JOIN journey_call_delay ON (DVJId, SequenceNumber)`.

### Metadata Tables (2)

| Table | Purpose |
|---|---|
| `_pipeline_metadata` | Run tracking (run_id, status, timestamp) |
| `_quality_results` | Quality check results per run |

---

## Deployment

### Asset Bundle Targets

| Target | Catalog | Main Pipeline Schedule | Notes |
|---|---|---|---|
| `dev` | `htq2_dev` | PAUSED (manual) | Default target |
| `staging` | `htq2_staging` | Every 15 min | Pre-production |
| `prod` | `htq2_prod` | Every 10 min | Production |

### Bundle Variables

| Variable | Default | Description |
|---|---|---|
| `catalog` | `htq2_dev` | Unity Catalog catalog name |
| `volume_path` | `trafiklab_raw` | Volume name for raw data |
| `checkpoint_path` | `dbfs:/htq2/checkpoints/autoloader` | Auto Loader checkpoint (exactly-once) |

### Quick Commands

```bash
# Deploy to dev
databricks bundle deploy -t dev

# Run main pipeline
databricks bundle run htq2_gtfs_pipeline -t dev

# Run static loader (manual)
databricks bundle run htq2_static_loader -t dev
```

---

## Production Validation (v3.1.0 — April 2026)

| Check | Result |
|---|---|
| Run 1 (cold start) | SUCCESS — 681s, 57 files, 22 tables populated |
| Run 2 (exactly-once) | SUCCESS — 469s, 3 new files only, 0 duplicates |
| Quality gate | 29/29 checks PASS |
| Auto Loader checkpoint | Persistent on DBFS, survives cluster restarts |
| Job cluster compatibility | No foreachBatch globals issue |

---

## Data Source

| Feed | File | Size | Content |
|---|---|---|---|
| Realtime | `TripUpdates.pb` | ~500 KB | Per-stop arrival/departure delays |
| Realtime | `VehiclePositions.pb` | ~85 KB | GPS lat/lng per vehicle |
| Realtime | `ServiceAlerts.pb` | ~90 KB | Service disruptions (logged only) |
| Static | `skane_static_YYYY-MM-DD.zip` | ~80 MB | Routes, trips, stops, stop_times, shapes |
| Static | `skane_extra_YYYY-MM-DD.zip` | ~60 MB | DVJId mapping (trip to vehicle journey) |

### Skanetrafiken GTFS-RT Feed Characteristics

Skanetrafiken GTFS-RT feed does NOT populate several standard fields:

| Protobuf Field | Value in Feed | Enrichment Source |
|---|---|---|
| `trip.route_id` | `''` (empty) | Static trips.txt + routes.txt -> `LineNumber` |
| `trip.direction_id` | `0` (default) | Static trips.txt -> `DirectionNumber` |
| `trip.start_time` | `''` (empty) | Static stop_times.txt -> `JourneyStartTime` |
| `vehicle.label` | `''` (empty) | Falls back to `vehicle.id` -> `VehicleNumber` |

---

## Secrets

| Scope | Key | Purpose |
|---|---|---|
| `trafiklab` | `realtime-api-key` | GTFS Realtime API access |
| `trafiklab` | `static-api-key` | GTFS Static ZIP downloads |

Provisioned via `setup_secrets.ipynb`.
