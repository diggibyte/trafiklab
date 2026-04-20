# HTQ2 GTFS Pipeline

**Hogia Traffic Quality (HTQ2) GTFS Realtime processing pipeline for Skånetrafiken.**

Migrated from Azure Synapse to Databricks. Processes real-time public transit data from Trafiklab.se into 22 analytical Delta Lake tables conforming to the HTQ2 BI_EDA schema.

## Architecture

```
Trafiklab.se API → [Ingest] → UC Volume (Bronze)
                       ↓
                  [Transform] → 22 Delta Tables (Silver)
                       ↓
                  [Validate] → Quality Results
```

All three tasks run as `python_wheel_task` entries from a single wheel, orchestrated by a Databricks Job defined in Asset Bundle YAML.

## Project Structure

```
htq2-gtfs-pipeline/
├── databricks.yml           ← Asset Bundle (3 targets: dev/staging/prod)
├── pyproject.toml           ← Wheel build + entry points
├── resources/
│   └── htq2_pipeline_job.yml ← Job definition (3 python_wheel_tasks)
├── src/htq2_gtfs/           ← THE WHEEL
│   ├── config.py             ← Shared config + CLI arg parsing
│   ├── ingestion/            ← Task 1: API → Volume
│   ├── processing/           ← Task 2: Bronze → Silver
│   ├── quality/              ← Task 3: Quality Gate
│   └── helpers/              ← Shared utilities
├── tests/                   ← pytest test suite
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.10+
- Databricks CLI configured
- Databricks Secrets scope `trafiklab` with keys `realtime-api-key` and `static-api-key`

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

### Deploy to Databricks

```bash
# Deploy to dev (no schedule)
databricks bundle deploy -t dev

# Run a one-off test
databricks bundle run htq2_gtfs_pipeline -t dev

# Deploy to prod (schedule activated)
databricks bundle deploy -t prod
```

## Entry Points

| Entry Point | Task | Command |
|---|---|---|
| `ingest` | Fetch from Trafiklab API | `htq2_gtfs ingest --catalog htq2_prod --run_mode realtime` |
| `transform` | Bronze → Silver (22 tables) | `htq2_gtfs transform --catalog htq2_prod` |
| `validate` | Quality checks | `htq2_gtfs validate --catalog htq2_prod` |

## Output Tables (Silver)

22 Delta tables + 14 SQL views matching the HTQ2 BI_EDA schema:

- **Base:** `journey` (36 cols), `journey_call` (67 cols)
- **Extensions:** `journey_call_delay`, `journey_call_statistics`, etc.
- **Independent:** `journey_link_halted_event`, `stop_point_big_variation`, etc.

## Configuration

All configuration via CLI arguments (no hardcoded values):

```yaml
# In resources/htq2_pipeline_job.yml
parameters:
  - "--catalog"
  - "${var.catalog}"  # Resolved per target: htq2_dev / htq2_staging / htq2_prod
```

## License

Proprietary — Hogia / DiggiByte Solutions
