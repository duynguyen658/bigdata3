# HCMC AQI Monitoring and Forecasting
## Master Implementation Plan

> Stable five-phase implementation plan. Current execution state belongs in `AGENT_HANDOFF.md`.

## 1. Project Goal

Build a technically correct and presentation-ready urban air-quality monitoring and 24-hour forecasting system using:

- OpenAQ
- PM2.5 and PM10
- Apache Spark
- Spark MLlib
- hourly time-series preparation
- H+1 through H+24 forecasting
- AQI calculation
- Parquet
- HDFS-compatible storage
- FastAPI
- Leaflet

Do not call the system true realtime unless true realtime streaming is actually implemented and verified.

## 2. Agreed Technical Decisions

### 2.1 Forecast Architecture

Use stacked multi-horizon supervised-learning rows:

```text
features(t) + horizon_hour=1  -> target(t+1h)
features(t) + horizon_hour=2  -> target(t+2h)
...
features(t) + horizon_hour=24 -> target(t+24h)
```

Requirements:

- `horizon_hour` ranges from 1 through 24.
- `horizon_hour` is an explicit model feature.
- H+1 means true `t+1h`.
- H+24 means true `t+24h`.
- Preserve `forecast_origin_ts` and `target_ts`.

Train four model pipelines total:

1. Random Forest — PM2.5
2. Random Forest — PM10
3. GBT — PM2.5
4. GBT — PM10

Do not revert to one fake T+24 model relabeled as H+1 through H+24. Do not create 96 independent models unless a future documented technical decision explicitly justifies it.

### 2.2 Time-Series Semantics

Hourly regularization must happen before lag and lead generation.

Missing hours must remain explicit. Row offsets must never be described as hourly offsets unless the timeline is truly hourly.

Candidate features, only when actually implemented:

- `lag_1h`
- `lag_3h`
- `lag_6h`
- `lag_12h`
- `lag_24h`
- `rolling_mean_3h`
- `rolling_mean_6h`
- `rolling_mean_12h`
- `rolling_mean_24h`

### 2.3 Temporal Leakage

The dataset must preserve:

- `forecast_origin_ts`
- `target_ts`

Train/validation/test splitting must be chronological and leak-safe. A sample must not remain in an earlier split if its `target_ts` crosses into a later split.

### 2.4 AQI Standard

Use the documented EPA AQI standard selected by the implementation.

Requirements:

- concentration preprocessing follows the standard exactly;
- do not blindly use Python `round()` if it violates the standard;
- PM2.5 and PM10 precision handling is explicit;
- unsupported pollutants must not default to PM10;
- breakpoint gaps must not fabricate AQI 500.

### 2.5 Timezone

Use UTC for stable storage and ordering where appropriate.

Use `Asia/Ho_Chi_Minh` for local temporal features and UI labels, including:

- hour of day
- day of week
- urban activity patterns
- local forecast labels

### 2.6 Storage

Historical ingestion must not erase previous measurements by default.

Local Parquet and HDFS-compatible storage must use an explicit strategy for:

- append
- deduplication
- overlapping ingestion windows
- partitioning

The stable measurement identity must be derived from the actual schema. Prefer a key equivalent to `sensor_id + parameter + datetime_utc` only if those fields are truly sufficient.

### 2.7 Frontend

Phase 4 must explicitly use `/taste`.

Preserve Leaflet unless a rewrite is technically justified. The map must remain the primary visual anchor.

## 3. Phase 1 — Correctness Foundation

### Goals

- hourly regularization
- true hourly lag semantics
- H+1 through H+24 target construction
- `horizon_hour` feature
- `forecast_origin_ts`
- `target_ts`
- preparation for leak-safe splitting
- EPA AQI normalization
- unsupported-pollutant handling
- `Asia/Ho_Chi_Minh` local temporal features
- deterministic tests

### Acceptance Criteria

- H+1 maps to true `t+1h`.
- H+24 maps to true `t+24h`.
- all intended horizons are generated correctly.
- missing hours cannot silently change lag semantics.
- train-time and inference-time feature semantics are compatible.
- AQI breakpoint gaps cannot fabricate AQI 500.
- unsupported pollutants are handled explicitly.
- local temporal features use `Asia/Ho_Chi_Minh`.
- tests verify target semantics, not arbitrary prediction inequality.

## 4. Phase 2 — Ingestion, Storage, and ML Evaluation

### Goals

- improve OpenAQ historical ingestion
- preserve pagination
- report possible truncation
- use explicit date windows
- prefer hourly historical data where technically appropriate
- append historical data
- deduplicate overlapping ingestion windows
- define stable deduplication keys
- define partition strategy
- support local Parquet
- support real HDFS-compatible Spark writes
- chronological train split
- chronological validation split
- chronological test split
- prevent `target_ts` leakage
- MAE evaluation
- RMSE evaluation
- R² evaluation
- metrics artifacts
- metrics by pollutant/model/horizon

At minimum report H+1, H+3, H+6, H+12, and H+24. Prefer all 24 horizons when practical.

### Acceptance Criteria

- old non-overlapping rows survive future ingestion runs;
- overlapping rows do not create uncontrolled duplicates;
- local Parquet behavior is deterministic;
- `hdfs://` paths use Spark/Hadoop-compatible writes;
- no false claim of real-HDFS success when no cluster was tested;
- train/validation/test are chronological;
- split boundaries account for `target_ts`;
- metrics come from held-out data;
- placeholder metrics are never presented as real.

## 5. Phase 3 — Backend APIs and Freshness

Implement or refactor:

- `GET /api/current`
- `GET /api/forecast`
- `GET /api/hotspots`
- `GET /api/metrics`
- `GET /api/health`

Clearly separate current, forecast, and historical data.

Expose where applicable:

- `generated_at`
- `data_as_of`
- `observation_ts`
- `observation_age_hours`
- `freshness_status`
- `forecast_origin_ts`
- `target_ts`

Freshness should be available per grid where practical. A single global maximum timestamp is not sufficient to describe stale grids.

Suggested states:

- `fresh`
- `delayed`
- `stale`
- `missing`

## 6. Phase 4 — Frontend Redesign Using /taste

This phase must explicitly invoke and use `/taste`.

### Goals

- map-first dashboard
- Current / Forecast mode switch
- H+1 through H+24 controls
- actual target timestamps
- Random Forest / GBT selector
- real model metrics only
- compact KPI strip
- ranked hotspot panel
- per-grid freshness states
- stale and missing-data states
- responsive desktop/tablet/mobile layout
- accessible controls

Avoid:

- generic AI dashboard appearance
- excessive cards
- unnecessary gradients
- glassmorphism clutter
- decorative UI without function

Preserve Leaflet unless a rewrite is truly necessary.

## 7. Phase 5 — Verification and Documentation

### Goals

- complete unit tests
- Spark feature tests
- horizon semantics tests
- AQI tests
- storage tests
- API tests
- integration verification
- sample-data verification
- OpenAQ verification when credentials are available
- Spark training verification
- FastAPI verification
- frontend verification
- README update
- architecture documentation
- exact run commands
- honest limitations

### Full Audit Requirements

Verify:

1. H+1 is truly `t+1h`.
2. H+24 is truly `t+24h`.
3. all H+1 through H+24 semantics are correct.
4. missing hours cannot corrupt hourly lag semantics.
5. `forecast_origin_ts` is correct.
6. `target_ts` is correct.
7. temporal splitting prevents label leakage.
8. train/validation/test are chronological.
9. AQI preprocessing matches the documented standard.
10. unsupported pollutants do not default to PM10.
11. local temporal features use `Asia/Ho_Chi_Minh`.
12. historical ingestion does not erase prior data.
13. overlapping ingestion is deduplicated correctly.
14. local Parquet works.
15. HDFS routing is technically real.
16. untested real HDFS behavior is labeled honestly.
17. MAE/RMSE/R² come from held-out data.
18. metrics are not fabricated.
19. current and forecast APIs are separate.
20. freshness behavior is explicit.
21. frontend mode switching works.
22. `/taste` redesign requirements are reflected in the actual UI.

## 8. Global Completion Rules

A phase may only be marked `COMPLETED` when:

- implementation exists;
- relevant tests were run;
- failures were resolved or explicitly documented;
- repository state was inspected;
- results were verified.

If a technical decision changes, document the old decision, new decision, reason, and affected phases.

## 9. Cross-Agent Continuity

All coding agents should:

1. read `docs/IMPLEMENTATION_PLAN.md`;
2. read `docs/AGENT_HANDOFF.md`;
3. inspect actual source code;
4. inspect `git status`;
5. inspect `git diff`;
6. inspect recent commits;
7. treat source code and tests as stronger evidence than handoff text;
8. update `docs/AGENT_HANDOFF.md` after meaningful progress;
9. never mark unverified work complete;
10. preserve the five-phase plan unless a technically necessary change is documented.
