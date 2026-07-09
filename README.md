# HCMC AQI Monitoring and Forecasting

A city-scale air-quality monitoring and 24-hour forecasting project for Ho Chi Minh City, Vietnam. The system ingests PM2.5 and PM10 measurements, stores them as Parquet, prepares hourly spatial time series with Apache Spark, trains Spark MLlib forecasting models, computes U.S. EPA AQI values, exposes FastAPI endpoints, and renders a Leaflet map-first dashboard.

This repository is intentionally honest about what has and has not been verified. It is suitable for local development, coursework, demos, and further engineering work. It should not be described as a production realtime system unless true streaming ingestion, production serving, and operational monitoring are added and verified.

## Current Project Status

| Area | Status | Notes |
|---|---|---|
| AQI calculation | VERIFIED | Deterministic unit tests cover PM2.5/PM10 boundaries, truncation behavior, unsupported pollutants, and combined AQI. |
| Spark feature engineering | VERIFIED | Tests cover dense hourly regularization, true hourly lag semantics, H+1 through H+24 targets, timestamps, local timezone features, and chronological split behavior. |
| Local Parquet storage | VERIFIED | Tests cover append, overwrite, partitioning, and deduplication by stable measurement identity. |
| Backend APIs | VERIFIED | API tests cover current observations, forecast filtering, hotspots, metrics, and health behavior. |
| Frontend dashboard | VERIFIED_LOCAL | Browser QA passed for desktop and mobile using local static assets; no console errors or failed requests were observed. |
| Standalone Spark training script | VERIFIED_LOCAL_FAST | A local Spark run completed with runtime-size overrides and regenerated forecast/metrics artifacts. Default larger model settings may still run slower on Windows. |
| OpenAQ API | VERIFIED_AUTH_SMOKE | Authenticated OpenAQ v3 smoke passed for pollutant parameters and TP.HCM locations. Full measurement ingestion was not run to avoid mutating the HDFS dataset. |
| Real HDFS cluster writes | VERIFIED | Spark read/write/append-dedup/delete verification passed against `hdfs://localhost:9000/aqi-hcmc`; training also wrote forecast parquet and model artifacts to HDFS. |

## What The System Does

The project implements a batch AQI monitoring and forecasting pipeline:

1. Ingest PM2.5 and PM10 measurements from OpenAQ API v3, or generate synthetic Ho Chi Minh City sensor data for demos.
2. Store measurements in a Parquet data lake with append and deduplication behavior.
3. Aggregate observations into spatial grid cells and hourly time steps.
4. Create a dense hourly timeline before lag and lead generation so lag offsets mean true hour offsets.
5. Build stacked multi-horizon supervised rows from H+1 through H+24.
6. Train four intended model pipelines:
   - Random Forest for PM2.5
   - Random Forest for PM10
   - GBTRegressor for PM2.5
   - GBTRegressor for PM10
7. Evaluate models with MAE, RMSE, and R2 by pollutant, model, horizon, and split.
8. Generate forecast artifacts and AQI categories.
9. Serve current, forecast, hotspot, metrics, and health data through FastAPI.
10. Display current and forecast AQI on a Leaflet dashboard.

## Architecture

```text
OpenAQ API v3 or synthetic generator
        |
        v
Measurement normalization
        |
        v
Parquet storage
  - local filesystem for development
  - Spark/Hadoop-compatible path for hdfs:// targets
  - append + dedup
  - partitioned by parameter/date
        |
        v
Spark hourly preparation
  - spatial grid aggregation
  - dense hourly regularization
  - explicit missing hours
        |
        v
Feature engineering
  - local time features in Asia/Ho_Chi_Minh
  - lag_1h, lag_3h, lag_24h
  - horizon_hour
  - forecast_origin_ts
  - target_ts
        |
        v
Spark MLlib models
  - random_forest
  - gbt
  - pm25 and pm10 where data is available
        |
        v
Artifacts
  - forecast JSON
  - forecast Parquet
  - metrics JSON
  - Spark model directories
        |
        v
FastAPI + Leaflet frontend
```

See also:

- `docs/IMPLEMENTATION_PLAN.md` for the five-phase target plan.
- `docs/ARCHITECTURE.md` for the target architecture and design rules.
- `docs/AGENT_HANDOFF.md` for the mutable, current execution state.

## Repository Layout

```text
app/
  main.py                 FastAPI app and API endpoints
  static/
    index.html            Dashboard shell
    styles.css            Dashboard styles
    app.js                Frontend data loading and Leaflet behavior

src/
  aqi.py                  U.S. EPA AQI calculation for PM2.5 and PM10
  config.py               Environment-driven settings
  io.py                   Measurement Parquet read/write helpers
  openaq_client.py        OpenAQ API v3 client

scripts/
  generate_sample_data.py Synthetic HCMC PM2.5/PM10 generator
  ingest_openaq.py        OpenAQ ingestion CLI
  train_forecast_spark.py Spark feature engineering, training, metrics, forecasts

tests/
  test_aqi.py                       AQI correctness tests
  test_spark_features.py            Spark feature and horizon tests
  test_storage_phase2.py            Storage and metrics serialization tests
  test_api_phase3.py                FastAPI endpoint tests
  test_train_forecast_integration.py Spark train/forecast integration smoke test

docs/
  IMPLEMENTATION_PLAN.md  Stable five-phase plan
  ARCHITECTURE.md         Target system architecture
  AGENT_HANDOFF.md        Mutable implementation state and verification log

data/                     Local generated data and prediction artifacts, ignored by git
models/                   Local Spark model artifacts, ignored by git
```

## Requirements

The project is Python-based and currently pins:

- Python 3.11 or newer is recommended.
- FastAPI `0.116.0`
- Uvicorn `0.35.0`
- pandas `2.3.1`
- pyarrow `20.0.0`
- PySpark `4.0.0`
- pytest `9.0.2`
- requests `2.32.4`
- python-dotenv `1.1.1`

Install Java before running Spark locally. PySpark requires a working Java runtime. Java 17 is a safe default for modern Spark development environments.

## Setup

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

### macOS or Linux

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Then edit `.env` as needed.

## Configuration

The application reads `.env` through `src/config.py`.

| Variable | Default | Purpose |
|---|---|---|
| `OPENAQ_API_KEY` | empty | Required for real OpenAQ ingestion. Synthetic data does not need it. |
| `OPENAQ_BASE_URL` | `https://api.openaq.org/v3` | OpenAQ API base URL. |
| `HCMC_BBOX` | `106.45,10.35,107.05,11.15` | Rough Ho Chi Minh City bounding box as `min_lon,min_lat,max_lon,max_lat`. |
| `HDFS_BASE_PATH` | empty | Optional base path such as `hdfs://namenode:9000/aqi-hcmc`. Leave empty for local development. |
| `HADOOP_USER_NAME` | empty | Optional HDFS client user for Windows-to-WSL local clusters, for example `minhduy`. |
| `MEASUREMENTS_PATH` | `data/parquet/measurements` | Measurement Parquet dataset path. |
| `PREDICTIONS_PATH` | `data/predictions/forecast_24h.json` | Forecast JSON artifact served by the API. |
| `PREDICTIONS_PARQUET_PATH` | `data/predictions/forecast_24h_parquet` | Forecast Parquet artifact path. |
| `METRICS_PATH` | `data/predictions/metrics.json` | Model metrics JSON artifact path. |
| `MODELS_PATH` | `models/aqi_forecast` | Spark model output directory. |
| `AQI_RF_NUM_TREES` | empty | Optional Random Forest tree count override for local smoke runs. |
| `AQI_RF_MAX_DEPTH` | empty | Optional Random Forest max-depth override for local smoke runs. |
| `AQI_GBT_MAX_ITER` | empty | Optional GBT iteration-count override for local smoke runs. |
| `AQI_GBT_MAX_DEPTH` | empty | Optional GBT max-depth override for local smoke runs. |
| `AQI_GBT_STEP_SIZE` | empty | Optional GBT step-size override for local smoke runs. |

When `HDFS_BASE_PATH` is set, `settings.storage_path(...)` prefixes configured relative paths with that base path. For example:

```env
HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc
MEASUREMENTS_PATH=data/parquet/measurements
```

resolves measurement storage to:

```text
hdfs://namenode:9000/aqi-hcmc/data/parquet/measurements
```

## Quick Local Demo With Synthetic Data

Synthetic data is useful when an OpenAQ API key is unavailable or when the HCMC public sensor coverage is too sparse for a visual demo.

Generate sample measurements:

```powershell
python scripts/generate_sample_data.py --sensors 120 --days 7 --overwrite
```

For a larger presentation dataset:

```powershell
python scripts/generate_sample_data.py --sensors 1200 --days 14 --overwrite
```

Train models and write forecast/metrics artifacts:

```powershell
python scripts/train_forecast_spark.py
```

For a faster local Windows smoke/regeneration run, keep all feature semantics and horizons intact but reduce model size:

```powershell
$env:AQI_RF_NUM_TREES='6'
$env:AQI_RF_MAX_DEPTH='5'
$env:AQI_GBT_MAX_ITER='8'
$env:AQI_GBT_MAX_DEPTH='3'
$env:AQI_GBT_STEP_SIZE='0.1'
python scripts/train_forecast_spark.py
```

Start the API and dashboard:

```powershell
uvicorn app.main:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

Important: default model sizes can still run slowly on Windows local Spark. Use the runtime-size overrides above for local smoke verification when needed.

## OpenAQ Ingestion

OpenAQ ingestion requires `OPENAQ_API_KEY`.

Add the key to `.env`:

```env
OPENAQ_API_KEY=your-api-key
```

Ingest recent data:

```powershell
python scripts/ingest_openaq.py --days 14 --limit-locations 80
```

Ingest an explicit UTC window:

```powershell
python scripts/ingest_openaq.py --datetime-from 2026-07-01T00:00:00Z --datetime-to 2026-07-08T00:00:00Z
```

Useful options:

| Option | Purpose |
|---|---|
| `--days N` | Use the last `N` days when explicit datetime bounds are not provided. |
| `--datetime-from` | UTC start timestamp, for example `2026-07-01T00:00:00Z`. |
| `--datetime-to` | UTC end timestamp, for example `2026-07-08T00:00:00Z`. |
| `--limit-locations N` | Stop after `N` matching locations. |
| `--max-pages-per-sensor N` | Limit paginated measurement requests per sensor. Warnings are printed if this may truncate data. |
| `--overwrite` | Replace the existing measurement dataset instead of append + dedup. |

If no PM2.5 or PM10 data is found inside the configured bounding box, the script exits with a message suggesting synthetic data.

## Storage Semantics

Measurement rows use this schema:

```text
sensor_id
location_id
location_name
datetime_utc
datetime_local
latitude
longitude
parameter
unit
value
source
date
```

Normalization behavior:

- `parameter` is normalized to lowercase and periods are removed, so `PM2.5` becomes `pm25`.
- `datetime_utc` is parsed as UTC and serialized consistently.
- `date` is derived from `datetime_utc`.
- Rows missing `sensor_id`, `parameter`, `datetime_utc`, or `date` are dropped.

Deduplication identity:

```text
sensor_id + parameter + datetime_utc
```

Partition strategy:

```text
parameter/date
```

Default write behavior is append + dedup. Use `--overwrite` only when you intentionally want to replace the dataset, such as regenerating sample data.

## Spark Feature Engineering

The Spark pipeline is implemented in `scripts/train_forecast_spark.py`.

The important correctness rule is:

```text
Hourly regularization happens before lag and lead generation.
```

That means:

- Raw observations are aggregated to hourly grid-cell rows.
- Each `(grid_lat, grid_lon, parameter)` series gets a dense hourly calendar.
- Missing hours stay as explicit null rows.
- Lag features such as `lag_1h` and `lag_24h` correspond to actual hourly offsets.
- Rows with missing required lag or target values are dropped before training.

The feature frame includes:

```text
grid_lat
grid_lon
parameter
hour_ts
value
sensor_count
latitude
longitude
hour
day_of_week
lag_1h
lag_3h
lag_24h
forecast_origin_ts
target_ts
horizon_hour
label
split
```

Local temporal features use:

```text
Asia/Ho_Chi_Minh
```

Storage, ordering, split boundaries, and timestamp arithmetic use UTC.

## Forecasting Semantics

The project uses stacked multi-horizon supervised rows:

```text
features(t) + horizon_hour=1  -> target(t+1h)
features(t) + horizon_hour=2  -> target(t+2h)
...
features(t) + horizon_hour=24 -> target(t+24h)
```

This is not a single T+24 model relabeled as H+1 through H+24. The tests explicitly guard against that bug by checking target timestamps and labels for all horizons.

The model feature vector currently includes:

```text
grid_lat
grid_lon
hour
day_of_week
lag_1h
lag_3h
lag_24h
sensor_count
horizon_hour
```

Models are trained separately by pollutant when data is available:

```text
random_forest / pm25
random_forest / pm10
gbt / pm25
gbt / pm10
```

If a pollutant has no training rows, that pollutant/model combination is skipped instead of failing the entire pipeline.

## Train, Validation, and Test Splits

The split is chronological and based on `target_ts`, not random sampling.

Default fractions:

```text
train      70%
validation 15%
test       15%
```

The boundary rule is intentionally leak-safe:

```text
train target_ts < validation boundary
validation target_ts < test boundary
test target_ts >= test boundary
```

This prevents a row from staying in an earlier split when its future label crosses into a later split.

## Metrics

When training completes with `METRICS_PATH` configured, the script writes:

```text
data/predictions/metrics.json
```

Metrics are reported by:

- model
- pollutant parameter
- horizon hour
- split

Metric fields:

```text
sample_count
mae
rmse
r2
```

Non-finite metric values are serialized as JSON `null`, not `NaN` or `Infinity`.

The API serves only metrics found in the artifact. It does not fabricate placeholder values.

## AQI Calculation

AQI logic is implemented in `src/aqi.py`.

Supported pollutants:

- PM2.5
- PM10

Unsupported pollutants return `None`. They do not silently fall back to PM10.

The implementation follows U.S. EPA AQI breakpoint behavior selected by the project:

- PM2.5 concentrations are truncated to 1 decimal place before bucket lookup.
- PM10 concentrations are truncated to the nearest integer before bucket lookup.
- Python's built-in `round()` is not used for concentration preprocessing.
- Final AQI index values are rounded to the nearest whole AQI value after interpolation.
- Negative concentrations are clamped to zero.
- Combined AQI is the maximum valid pollutant AQI among the available pollutants.

AQI categories returned by the backend:

| AQI range | Category key |
|---|---|
| `0-50` | `good` |
| `51-100` | `moderate` |
| `101-150` | `unhealthy_sensitive` |
| `151-200` | `unhealthy` |
| `201-300` | `very_unhealthy` |
| `301+` | `hazardous` |
| missing | `unknown` |

## Forecast Artifacts

Forecast JSON rows include:

```text
model
latitude
longitude
forecast_origin_ts
target_ts
forecast_ts
horizon_hour
sensor_count
values
aqi
category
```

`values` can contain:

```json
{
  "pm25": 30.4,
  "pm10": 52.1
}
```

`aqi` is computed from available pollutant predictions. If only one pollutant is present, the AQI uses that pollutant only.

## API

Start the server:

```powershell
uvicorn app.main:app --reload
```

Base URL:

```text
http://127.0.0.1:8000
```

### `GET /`

Serves the Leaflet dashboard.

### `GET /api/models`

Returns supported model IDs and display labels.

Example response shape:

```json
{
  "default": "random_forest",
  "models": [
    {"id": "random_forest", "label": "Random Forest"},
    {"id": "gbt", "label": "GBTRegressor"}
  ]
}
```

### `GET /api/current`

Returns latest available local measurement points by grid cell.

Important fields:

```text
city
mode
generated_at
data_as_of
artifact
count
points
```

Each point can include:

```text
grid_lat
grid_lon
latitude
longitude
values
aqi
category
observation_ts
observation_age_hours
freshness_status
sensor_count
```

Freshness states:

| State | Rule |
|---|---|
| `fresh` | observation age is at most 2 hours |
| `delayed` | observation age is more than 2 hours and at most 12 hours |
| `stale` | observation age is more than 12 hours |
| `missing` | observation timestamp cannot be calculated |

### `GET /api/forecast`

Query parameters:

| Parameter | Default | Rule |
|---|---|---|
| `horizon` | `1` | integer from `1` to `24` |
| `model` | `random_forest` | `random_forest` or `gbt`; unknown values fall back to `random_forest` |

Example:

```text
/api/forecast?horizon=6&model=gbt
```

Response includes:

```text
mode
horizon_hour
model
generated_at
data_as_of
target_as_of
artifact
count
points
```

### `GET /api/hotspots`

Returns ranked AQI hotspots.

Query parameters:

| Parameter | Default | Rule |
|---|---|---|
| `mode` | `forecast` | `current` or `forecast` |
| `horizon` | `1` | integer from `1` to `24`, used for forecast mode |
| `model` | `random_forest` | used for forecast mode |
| `limit` | `10` | integer from `1` to `50` |

Examples:

```text
/api/hotspots?mode=current&limit=10
/api/hotspots?mode=forecast&horizon=12&model=random_forest&limit=5
```

### `GET /api/metrics`

Returns model metrics from `METRICS_PATH` if the artifact exists.

Optional filters:

| Parameter | Example |
|---|---|
| `model` | `random_forest` |
| `parameter` | `PM2.5`, `pm25`, `PM10`, or `pm10` |
| `split` | `validation` or `test` |

Example:

```text
/api/metrics?model=gbt&parameter=PM2.5&split=test
```

If the metrics artifact is missing, the response has:

```json
{
  "available": false,
  "metrics": []
}
```

### `GET /api/health`

Reports local artifact availability.

For local paths, the API checks whether files/directories exist. For `hdfs://` paths, the local FastAPI process reports that direct checking is unsupported rather than pretending the HDFS path is healthy.

Health status:

- `ok` when local checked artifacts exist.
- `degraded` when one or more local checked artifacts are missing, or no checked artifact is available.

## Frontend Dashboard

The frontend is a static Leaflet dashboard served from `app/static`.

Runtime map assets are vendored under `app/static/vendor`, so the dashboard does not require CDN access for Leaflet, heatmap rendering, marker images, fonts, or base map tiles during local verification.

Implemented UI behavior:

- Map-first layout.
- Current and Forecast modes.
- H+1 through H+24 forecast horizon control.
- Random Forest and GBT model selector.
- KPI strip.
- AQI legend.
- Ranked hotspot panel.
- Metrics display when `metrics.json` is available.
- Empty and missing-artifact states.

The dashboard expects the FastAPI server to provide data. It does not fabricate missing values.

Browser QA has been run with a local FastAPI server and Playwright headless Chromium. Desktop and mobile checks passed with no console errors, no failed network requests, visible current/forecast data, visible metrics, visible hotspots, and no horizontal overflow.

## HDFS Usage

Local development does not require Hadoop.

For a Hadoop/HDFS environment, configure:

```env
HDFS_BASE_PATH=hdfs://namenode:9000/aqi-hcmc
```

Then run ingestion/training normally:

```powershell
python scripts/ingest_openaq.py --days 14
python scripts/train_forecast_spark.py
```

Important limitations:

- Measurement writes to `hdfs://` are routed through Spark and Hadoop filesystem APIs.
- Real HDFS verification has been run against `hdfs://localhost:9000/aqi-hcmc`.
- From Windows, set `HADOOP_USER_NAME` to the HDFS owner user if HDFS rejects writes from the Windows account name.

## Testing

Run the full test suite:

```powershell
python -m pytest
```

Run focused tests:

```powershell
python -m pytest tests/test_aqi.py
python -m pytest tests/test_spark_features.py
python -m pytest tests/test_storage_phase2.py
python -m pytest tests/test_api_phase3.py
python -m pytest tests/test_train_forecast_integration.py
```

Expected coverage areas:

- AQI boundary behavior and unsupported pollutants.
- Dense hourly timeline construction.
- Missing-hour lag behavior.
- H+1 through H+24 target construction.
- `forecast_origin_ts` and `target_ts` correctness.
- Asia/Ho_Chi_Minh local temporal features.
- Chronological split behavior based on `target_ts`.
- Local Parquet append, overwrite, partitioning, and deduplication.
- Metrics JSON serialization.
- FastAPI current, forecast, hotspot, metrics, and health endpoints.
- Spark training/forecast integration.

On Windows, PySpark tests may require running outside restrictive sandboxes because local Spark workers use local processes and loopback communication.

## Typical Development Workflows

### Regenerate synthetic data and run API

```powershell
python scripts/generate_sample_data.py --sensors 120 --days 7 --overwrite
uvicorn app.main:app --reload
```

This is enough to test `/api/current` and the Current dashboard mode.

### Regenerate data, train models, and view forecasts

```powershell
python scripts/generate_sample_data.py --sensors 120 --days 7 --overwrite
python scripts/train_forecast_spark.py
uvicorn app.main:app --reload
```

This should produce:

```text
data/predictions/forecast_24h.json
data/predictions/forecast_24h_parquet
data/predictions/metrics.json
models/aqi_forecast
```

Default model sizes may run slowly on Windows local Spark. Use the `AQI_RF_*` and `AQI_GBT_*` overrides for local smoke/regeneration runs when fast feedback is more important than model size.

### Ingest real OpenAQ data and train

```powershell
python scripts/ingest_openaq.py --datetime-from 2026-07-01T00:00:00Z --datetime-to 2026-07-08T00:00:00Z --limit-locations 80
python scripts/train_forecast_spark.py
uvicorn app.main:app --reload
```

Do this only after setting `OPENAQ_API_KEY`.

## Troubleshooting

### `OPENAQ_API_KEY is required for real OpenAQ ingestion.`

Set `OPENAQ_API_KEY` in `.env`, or use `scripts/generate_sample_data.py` instead.

### No OpenAQ rows found

The configured HCMC bounding box may not contain enough public PM2.5/PM10 observations for the requested time window. Try:

- increasing the time window;
- increasing `--limit-locations`;
- checking the `HCMC_BBOX`;
- using synthetic data for demos.

### Spark is slow on Windows

Local PySpark can be slow, especially with multiple model fits. Try:

- reducing synthetic sensor count and day count;
- running from a normal terminal instead of a restricted sandbox;
- closing other Java/Spark processes;
- setting `AQI_RF_NUM_TREES`, `AQI_RF_MAX_DEPTH`, `AQI_GBT_MAX_ITER`, `AQI_GBT_MAX_DEPTH`, and `AQI_GBT_STEP_SIZE` for a smaller local smoke model.

### Metrics API returns `available: false`

`data/predictions/metrics.json` does not exist yet. Run:

```powershell
python scripts/train_forecast_spark.py
```

If training does not complete, the API will correctly avoid serving fake metrics.

### Forecast API returns zero points

`data/predictions/forecast_24h.json` is missing or does not contain rows matching the requested `horizon` and `model`. Run training and check that the forecast artifact exists.

### Health is `degraded`

This usually means one or more local artifacts are missing. It is expected before generating measurements, forecasts, and metrics.

## Known Limitations

- This is a batch system, not true realtime streaming.
- OpenAQ authenticated API smoke has been verified, but a full measurement ingestion run was not performed in the latest verification pass.
- Real HDFS read/write, append/dedup, API health/current reads, training forecast parquet writes, and model artifact writes have been verified against the local WSL-backed HDFS endpoint.
- Default-size standalone Spark training may still be slow on Windows local Spark; a local-fast run with runtime-size overrides has been verified.
- Public OpenAQ coverage for Ho Chi Minh City may be sparse or inconsistent depending on sensor availability and the selected time window.
- The synthetic generator is for demos and repeatable development only. It must not be presented as real sensor data.

## Verification Rules For Future Work

After meaningful implementation work:

1. Run relevant tests.
2. Inspect `git status`.
3. Inspect `git diff`.
4. Update `docs/AGENT_HANDOFF.md`.
5. Record commands run and whether they passed, failed, or were skipped.
6. Record known bugs and unverified items honestly.
7. Keep the "Exact Next Step" section in `docs/AGENT_HANDOFF.md` current.

Do not mark full OpenAQ measurement ingestion as verified without an actual successful ingestion run.
