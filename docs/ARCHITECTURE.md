# HCMC AQI Monitoring and Forecasting
## Architecture

> Describes the target architecture and system boundaries.
> Actual implementation status must be checked in `AGENT_HANDOFF.md`.

## 1. System Purpose

The system ingests public air-quality measurements, prepares hourly spatial time series, trains Spark MLlib forecasting models, generates H+1 through H+24 PM2.5/PM10 forecasts, derives AQI, serves data through FastAPI, and visualizes current/forecast conditions on a Leaflet map.

Primary pollutants:

- PM2.5
- PM10

Primary use cases:

- inspect latest available conditions;
- visualize AQI spatially;
- identify hotspots;
- compare forecast horizons;
- compare Random Forest and GBT outputs;
- inspect real model metrics;
- communicate freshness honestly.

## 2. High-Level Architecture

```text
                         ┌──────────────────────┐
                         │      OpenAQ API      │
                         │  Public observations │
                         └──────────┬───────────┘
                                    │
                                    │ HTTP polling / historical fetch
                                    ▼
                         ┌──────────────────────┐
                         │ Python Ingestion     │
                         │ - pagination         │
                         │ - date windows       │
                         │ - normalization      │
                         │ - truncation warning │
                         └──────────┬───────────┘
                                    │
                                    ▼
                 ┌─────────────────────────────────────┐
                 │ Storage Layer                       │
                 │ Local dev: Parquet                  │
                 │ Distributed: HDFS-compatible write │
                 │ - append                            │
                 │ - dedup                             │
                 │ - partition                         │
                 └────────────────┬────────────────────┘
                                  │
                                  ▼
                       ┌───────────────────────┐
                       │ Apache Spark          │
                       │ Hourly preparation    │
                       │ Spatial aggregation   │
                       │ Dense hourly timeline │
                       └───────────┬───────────┘
                                   │
                                   ▼
                       ┌───────────────────────┐
                       │ Feature Engineering   │
                       │ - local time features │
                       │ - hourly lags         │
                       │ - rolling features    │
                       │ - horizon_hour        │
                       │ - origin/target ts    │
                       └───────────┬───────────┘
                                   │
                                   ▼
                     ┌──────────────────────────┐
                     │ Spark MLlib              │
                     │ RF PM2.5                 │
                     │ RF PM10                  │
                     │ GBT PM2.5                │
                     │ GBT PM10                 │
                     └────────────┬─────────────┘
                                  │
                 ┌────────────────┴─────────────────┐
                 │                                  │
                 ▼                                  ▼
      ┌──────────────────────┐          ┌──────────────────────┐
      │ Forecast Artifacts   │          │ Metrics Artifacts    │
      │ H+1 ... H+24         │          │ MAE / RMSE / R²      │
      │ PM2.5 / PM10 / AQI   │          │ by model/horizon     │
      └──────────┬───────────┘          └──────────┬───────────┘
                 │                                  │
                 └────────────────┬─────────────────┘
                                  ▼
                       ┌───────────────────────┐
                       │ FastAPI              │
                       │ /api/current         │
                       │ /api/forecast        │
                       │ /api/hotspots        │
                       │ /api/metrics         │
                       │ /api/health          │
                       └───────────┬───────────┘
                                   │
                                   ▼
                       ┌───────────────────────┐
                       │ Leaflet Frontend      │
                       │ Map-first dashboard   │
                       │ Current / Forecast    │
                       │ Horizon controls      │
                       │ Hotspots / freshness  │
                       │ Metrics               │
                       └───────────────────────┘
```

## 3. Data Ingestion Architecture

### 3.1 Source

Primary source: OpenAQ API v3.

Expected data, where available:

- station/location identifiers
- sensor identifiers
- pollutant parameter
- measurement value
- timestamp
- coordinates
- unit/source metadata

Do not assume every station measures both pollutants, every station updates at the same interval, or every HCMC area has dense coverage.

### 3.2 Ingestion Responsibilities

The ingestion layer should:

- use explicit date windows;
- preserve pagination;
- warn when safety/page limits may truncate data;
- normalize into a consistent internal schema;
- preserve source timestamps;
- preserve coordinates;
- preserve sensor identity where available;
- avoid silent data loss.

### 3.3 Historical vs Current

Historical ingestion is optimized for training, evaluation, and reproducibility.

Current/latest ingestion is optimized for latest available conditions, freshness calculation, and map display.

These should not be conflated.

## 4. Storage Architecture

### 4.1 Local Development

Use Parquet.

Conceptual layout:

```text
data/
├── raw/
│   └── measurements/
├── processed/
│   └── hourly/
└── predictions/
```

Actual layout must follow implemented code.

### 4.2 HDFS-Compatible Storage

For `hdfs://...` paths, use Spark/Hadoop-compatible writes.

Do not rely on plain Pandas `to_parquet()` as the HDFS implementation.

### 4.3 Append and Dedup

Historical ingestion must use a non-destructive strategy:

```text
existing affected partition
        +
new ingestion batch
        ↓
union
        ↓
deduplicate by stable identity
        ↓
rewrite only affected partition(s)
```

A candidate stable identity is `sensor_id + parameter + datetime_utc`, but the actual implementation must confirm whether this is sufficient.

### 4.4 Partitioning

Prefer low-cardinality, time-aware partitions such as:

- `parameter + date`
- `year + month + day + parameter`

Avoid excessive tiny partitions.

## 5. Spark Processing Architecture

### 5.1 Spatial Aggregation

Use a stable spatial identity such as:

- `grid_lat` + `grid_lon`
- or dedicated `grid_id`

Do not use averaged floating-point display coordinates as the primary join identity.

### 5.2 Hourly Regularization

Before lag/lead generation:

1. aggregate to hourly resolution;
2. compute min/max timestamps per series;
3. generate a dense hourly calendar;
4. left-join observations onto the calendar;
5. preserve missing hours as explicit nulls.

This makes `lag(value, 1)` meaningful on a truly hourly timeline.

### 5.3 Timezone

Use UTC for storage, ordering, and deterministic temporal joins.

Use `Asia/Ho_Chi_Minh` for hour-of-day, day-of-week, and local UI labels.

## 6. Forecasting Architecture

### 6.1 Multi-Horizon Design

For each origin `t` create:

```text
horizon_hour=1  -> target_ts=t+1h
horizon_hour=2  -> target_ts=t+2h
...
horizon_hour=24 -> target_ts=t+24h
```

### 6.2 Required Temporal Fields

Each training row should preserve:

- `forecast_origin_ts`
- `target_ts`
- `horizon_hour`

### 6.3 Model Features

Typical features may include, only if implemented:

- `grid_lat`
- `grid_lon`
- local `hour`
- local `day_of_week`
- hourly lag features
- rolling means
- `sensor_count`
- `horizon_hour`

### 6.4 Models

Four model pipelines:

- `RandomForestRegressor` — PM2.5
- `RandomForestRegressor` — PM10
- `GBTRegressor` — PM2.5
- `GBTRegressor` — PM10

### 6.5 Train / Validation / Test

Use chronological splitting. Do not use random split.

Leak-safe requirement:

```text
train target_ts < validation boundary
validation target_ts < test boundary
```

A sample whose future label crosses a boundary must not remain in an earlier split.

## 7. Model Evaluation Architecture

Evaluate:

- MAE
- RMSE
- R²

By:

- pollutant
- model
- horizon

At minimum:

- H+1
- H+3
- H+6
- H+12
- H+24

Prefer all 24 horizons when practical.

Metrics artifacts should identify:

- pollutant
- model
- horizon_hour
- split
- MAE
- RMSE
- R²
- sample_count
- evaluation timestamp

Do not fabricate metrics.

## 8. AQI Architecture

Use the documented EPA AQI implementation selected by the project.

Required behavior:

- concentration preprocessing follows the standard;
- PM2.5 precision handling is explicit;
- PM10 precision handling is explicit;
- unsupported pollutants do not default to PM10;
- breakpoint gaps do not fabricate AQI 500;
- frontend legend bands match backend category thresholds.

When both pollutant AQIs are available:

```text
combined AQI = max(PM2.5 AQI, PM10 AQI)
```

Missing-pollutant behavior must be explicit and documented.

## 9. Backend Architecture

### `GET /api/current`

Latest available observations, with fields such as:

- grid identity
- latitude
- longitude
- PM2.5
- PM10
- AQI
- observation timestamp
- observation age
- freshness status

### `GET /api/forecast`

Forecast points, with fields such as:

- grid identity
- model
- horizon_hour
- forecast_origin_ts
- target_ts
- PM2.5
- PM10
- AQI
- freshness metadata

### `GET /api/hotspots`

Ranked hotspots with explicit ranking logic.

### `GET /api/metrics`

Real model metrics only.

### `GET /api/health`

Application/artifact availability. Do not claim external dependencies are healthy unless actually checked.

## 10. Freshness Architecture

Prefer per-grid fields:

- `observation_ts`
- `observation_age_hours`
- `freshness_status`

Suggested states:

- `fresh`
- `delayed`
- `stale`
- `missing`

Global fields such as `generated_at` and `data_as_of` may exist in addition to per-grid freshness. A single global max timestamp must not hide stale grids.

## 11. Frontend Architecture

### 11.1 Design Principle

Map-first. The map is the primary visual anchor.

### 11.2 Main Modes

```text
Current
Forecast
```

Current mode:

- current observations
- freshness
- current hotspots

Forecast mode:

- model selector
- H+1 through H+24
- actual target timestamp
- forecast hotspots
- model metrics

### 11.3 Suggested Lightweight Modules

```text
app/static/
├── index.html
├── css/
│   └── app.css
└── js/
    ├── api.js
    ├── map.js
    ├── current.js
    ├── forecast.js
    ├── hotspots.js
    ├── metrics.js
    └── app.js
```

Only introduce modules when useful. Do not refactor solely for cosmetic folder structure.

### 11.4 Taste Skill

Phase 4 must explicitly use `/taste` to improve hierarchy, spacing, typography, responsive behavior, accessibility, and map-control competition.

## 12. Testing Architecture

Recommended test areas:

```text
tests/
├── unit/
├── spark/
├── storage/
└── api/
```

Required coverage includes:

### AQI

- exact boundaries
- values around boundaries
- values such as `9.05`
- unsupported pollutant
- negative/null behavior

### Spark Features

- dense hourly timeline
- missing-hour behavior
- hourly lags
- H+1 target
- H+24 target
- `forecast_origin_ts`
- `target_ts`
- timezone conversion

### Storage

- first write
- overlapping second write
- history retention
- dedup
- local read-back
- storage scheme routing

### Split/Evaluation

- chronological boundaries
- no target crossing into later split
- metrics schema

### API

- current
- forecast
- hotspots
- metrics
- health
- empty data
- stale data
- missing artifacts

## 13. Cross-Agent Architecture

Stable plan:

```text
docs/IMPLEMENTATION_PLAN.md
```

Mutable handoff:

```text
docs/AGENT_HANDOFF.md
```

Recommended agent instruction files:

```text
AGENTS.md
CLAUDE.md
```

Continuity flow:

```text
Claude Code
    ↓
read plan + handoff
    ↓
verify repo state
    ↓
implement + test
    ↓
update handoff
    ↓
session ends

OpenAI Codex
    ↓
read AGENTS.md
    ↓
read plan + handoff
    ↓
verify git/source/tests
    ↓
continue exact next step
```

## 14. Implemented vs Optional Future Architecture

Core target architecture:

- OpenAQ ingestion
- Parquet/HDFS-compatible storage
- Spark batch processing
- Spark MLlib
- FastAPI
- Leaflet

Optional future architecture, only if actually implemented later:

```text
OpenAQ poller
    ↓
Kafka
    ↓
Spark Structured Streaming
    ↓
Realtime serving store
    ↓
FastAPI/WebSocket
    ↓
Leaflet
```

Do not present Kafka or Structured Streaming as implemented unless source code and deployment actually prove it.

## 15. Architecture Truthfulness Rules

1. Source code is stronger evidence than README text.
2. Tests are stronger evidence than handoff claims.
3. Git state must be checked before continuing work.
4. Do not claim true realtime unless implemented.
5. Do not claim real HDFS success unless tested.
6. Do not claim OpenAQ success without an actual successful run.
7. Do not expose fake metrics.
8. Do not mark a phase complete without verification.
