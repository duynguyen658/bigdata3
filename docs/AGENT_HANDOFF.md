# Agent Handoff
## HCMC AQI Monitoring and Forecasting

Mutable execution-state document for Claude Code, OpenAI Codex, or another coding agent.

## 1. Current State

- Repository root: `D:\bigdata2`
- Current branch: `main`
- Latest commit: `d30e461 update 2`
- Working tree: `scripts/train_forecast_spark.py`, `tests/test_forecast_outputs.py`, and `docs/AGENT_HANDOFF.md` modified; untracked `.claude/`; generated `data/`, `models/`, caches ignored
- Active phase: Phase 5 verification/documentation after Phase 2-4 implementation work
- Active task: corrective P1 forecast pollutant merge identity fix completed; next is artifact regeneration/training runtime verification
- Last updated: 2026-07-09
- Last agent: OpenAI Codex

## 2. Phase Status

| Phase | Status | Verified | Notes |
|---|---|---|---|
| Phase 1 - Correctness Foundation | COMPLETED | YES | Full pytest passes; Spark feature/AQI tests cover hourly regularization, H+1..H+24, timezone, AQI edge cases |
| Phase 2 - Ingestion, Storage, ML Evaluation | IMPLEMENTED_NOT_VERIFIED | PARTIAL | Unit/integration tests pass; full standalone train smoke timed out at 5 minutes on Windows local Spark |
| Phase 3 - Backend APIs and Freshness | COMPLETED | YES | API tests and temporary uvicorn HTTP verification pass |
| Phase 4 - Frontend via /taste | IMPLEMENTED_NOT_VERIFIED | PARTIAL | `/taste` skill was unavailable; used `impeccable` design context instead. Static implementation done; HTTP `/` returns 200; no browser screenshot QA yet |
| Phase 5 - Verification and Documentation | IN_PROGRESS | PARTIAL | README and handoff updated; OpenAQ real run and HDFS cluster verification not run |

Do not mark Phase 2 or Phase 4 fully complete until the verification gaps below are resolved or deliberately accepted.

## 3. Work Completed

### Phase 1 Verification

Files:

- `src/aqi.py`
- `scripts/train_forecast_spark.py`
- `tests/test_aqi.py`
- `tests/test_spark_features.py`
- `tests/test_train_forecast_integration.py`

Implementation:

- EPA AQI concentration truncation for PM2.5/PM10.
- Unsupported pollutants return `None`, not PM10 fallback.
- Dense hourly regularization before lag/lead.
- Stacked H+1 through H+24 rows with `horizon_hour`, `forecast_origin_ts`, `target_ts`.
- Local temporal features use `Asia/Ho_Chi_Minh`.
- Random Forest and GBT pipelines both trained in integration test.

Verification:

- `python -m pytest` passed with Spark outside sandbox.

### Phase 2 Implementation

Files:

- `src/io.py`
- `src/config.py`
- `.env.example`
- `src/openaq_client.py`
- `scripts/ingest_openaq.py`
- `scripts/generate_sample_data.py`
- `scripts/train_forecast_spark.py`
- `tests/test_storage_phase2.py`
- `tests/test_spark_features.py`

Implementation:

- Measurement writes now append by default, deduplicate by `sensor_id + parameter + datetime_utc`, and partition by `parameter/date`.
- `--overwrite` exists for sample generation and OpenAQ ingestion.
- OpenAQ ingestion supports `--datetime-from`, `--datetime-to`, max-page truncation warnings, and append/dedup storage.
- `hdfs://` measurement paths route through Spark/Hadoop-compatible write logic instead of pandas.
- Chronological split uses `target_ts` quantile boundaries and assigns `train`, `validation`, `test`.
- Metrics artifact writer emits MAE/RMSE/R2 by model, pollutant, horizon, and split. Non-finite metric values become JSON `null`.

Verification:

- `python -m pytest tests/test_storage_phase2.py` passed.
- `python -m pytest` passed, 40 tests total after Phase 3 additions.
- Generated sample smoke data successfully:
  `python scripts\generate_sample_data.py --sensors 4 --days 3 --overwrite` with temp env paths wrote 576 rows.

Known verification gap:

- `python scripts\train_forecast_spark.py` with temp env paths timed out twice at 5 minutes on Windows local Spark before writing `metrics.json`/forecast JSON. It had started writing model artifacts. This may be runtime/hyperparameter cost, but it is not resolved.
- Real HDFS cluster was not tested.
- Real OpenAQ API ingestion was not tested.

### Phase 3 Implementation

Files:

- `app/main.py`
- `tests/test_api_phase3.py`

Implementation:

- Added `/api/current` from latest local measurement Parquet with per-grid `observation_ts`, `observation_age_hours`, and `freshness_status`.
- Refactored `/api/forecast` to include artifact status, `generated_at`, `data_as_of`, and `target_as_of`.
- Extended `/api/hotspots` for current or forecast mode.
- Added `/api/metrics` with model/parameter/split filters.
- Added `/api/health` with artifact availability and honest HDFS-local-check behavior.

Verification:

- `python -m pytest tests/test_api_phase3.py` passed.
- Temporary uvicorn job verified:
  `/` status 200, `/api/health` status `degraded`, `/api/current` count 122, `/api/forecast?horizon=1&model=random_forest` count 122.

### Phase 4 Implementation

Files:

- `.impeccable.md`
- `app/static/index.html`
- `app/static/styles.css`
- `app/static/app.js`

Implementation:

- Created design context for a calm civic operational dashboard.
- Rebuilt frontend as map-first Current/Forecast dashboard.
- Added mode switch, model selector, horizon control, KPI strip, AQI legend, hotspots, metrics strip, and explicit empty states.
- Frontend consumes `/api/current`, `/api/forecast`, and `/api/metrics`.

Verification:

- `python -m pytest tests/test_api_phase3.py` passed after frontend changes.
- CSS grep found no `border-left`, `border-right`, or gradient-text banned patterns.
- Temporary uvicorn job returned `/` status 200.

Known verification gap:

- Browser screenshot/visual QA was not run.
- `/taste` itself is not available in this environment; `impeccable` was used as the nearest available frontend design skill.

### Git Ignore

Files:

- `.gitignore`

Implementation:

- Ignores Python caches, env files, logs, Spark/Hadoop artifacts, `data/`, `models/`, and large local analytical outputs.

### Phase 5 Documentation Update

Files:

- `README.md`
- `docs/AGENT_HANDOFF.md`

Implementation:

- Rewrote README fully in English.
- Expanded README with status, architecture, setup, configuration, synthetic data workflow, OpenAQ workflow, storage semantics, Spark feature semantics, forecasting semantics, metrics, AQI behavior, API contract, frontend notes, HDFS notes, tests, troubleshooting, known limitations, and future verification rules.
- Kept limitations explicit: OpenAQ live ingestion NOT_VERIFIED, real HDFS NOT_VERIFIED, full standalone Spark train PARTIAL, frontend browser/visual QA PARTIAL.

Verification:

- Checked README diff.
- Checked README for non-ASCII characters; no matches.
- No code tests were run for this docs-only change.

### Phase 5 Corrective P0 Lag Fix

Files:

- `scripts/train_forecast_spark.py`
- `tests/test_spark_features.py`
- `docs/AGENT_HANDOFF.md`

Verified defect:

- `build_forecast()` created inference lag features by assigning `lag_1h`, `lag_3h`, and `lag_24h` to the latest/current `value`.
- Training uses true historical lags from the regularized hourly time series, so inference feature semantics did not match training semantics.

Implementation:

- Added `forecast_candidate_frame(hourly)` to construct inference candidates.
- Inference now computes `lag_1h`, `lag_3h`, and `lag_24h` with the same ascending per-grid/per-parameter window semantics used by `feature_frame()`.
- Inference now drops latest-origin candidates with missing required current value or lag history instead of filling missing lag history with current value.
- `build_forecast()` now scores the corrected candidate frame.

Regression tests:

- Added `test_forecast_candidate_lags_use_history_not_latest_value`.
- Added `test_forecast_candidate_drops_latest_origin_when_required_lag_is_missing`.

Verification:

- `python -m pytest tests\test_spark_features.py` -> 17 passed.
- `python -m pytest tests\test_train_forecast_integration.py` first timed out at 120 seconds, then passed with longer timeout: 1 passed in 343.17 seconds.
- `python -m pytest` -> 42 passed in 336.13 seconds.

Artifact note:

- Existing generated `data/predictions/forecast_24h.json` has 5,856 rows but is stale and lacks `forecast_origin_ts` and `target_ts`.
- `data/predictions/metrics.json` is absent.
- Forecast artifacts were not regenerated in this pass because standalone training remains a known long-running verification gap.

### Phase 5 Corrective P1 Forecast Merge Identity Fix

Files:

- `scripts/train_forecast_spark.py`
- `tests/test_forecast_outputs.py`
- `docs/AGENT_HANDOFF.md`

Verified defect:

- `write_outputs()` merged PM2.5 and PM10 forecast rows using rounded display `latitude` and `longitude`.
- Those display coordinates are pollutant-specific averages and can differ even when rows belong to the same forecast grid cell.
- This could split PM2.5 and PM10 into separate JSON points for the same model/grid/target/horizon instead of combining them into one AQI point.

Implementation:

- Changed forecast JSON merge identity to use stable `grid_lat` and `grid_lon`.
- Added `grid_lat` and `grid_lon` to forecast JSON payload rows.
- Kept display `latitude` and `longitude` for map rendering, averaging them across newly merged pollutant rows.
- Kept `sensor_count` as the maximum pollutant-level sensor count for the merged point.

Regression tests:

- Added `tests/test_forecast_outputs.py`.
- Added `test_write_outputs_merges_pollutants_by_grid_identity_not_display_coordinates`.

Verification:

- Initial regression test command failed due a Windows Spark Python worker crash when the test used a local Python-row DataFrame; test setup was rewritten to construct the frame with Spark expressions.
- `python -m pytest tests\test_forecast_outputs.py` -> 1 passed.
- `python -m pytest tests\test_spark_features.py tests\test_forecast_outputs.py` -> 18 passed.
- `python -m pytest` -> 43 passed in 175.22 seconds.

Artifact note:

- Existing generated `data/predictions/forecast_24h.json` is still stale: 5,856 rows, no `grid_lat`, no `forecast_origin_ts`, no `target_ts`.
- `data/predictions/metrics.json` is still absent.
- Forecast artifacts were not regenerated in this pass.

## 4. Current In-Progress Work

### Task: Phase 5 verification and final polish

- Current state: P0 train/inference lag mismatch and P1 forecast pollutant merge identity defects fixed; full pytest passes with 43 tests; artifact regeneration remains partial
- Files involved: all source, tests, README, handoff
- Blocker: standalone Spark train script runtime exceeded 5-minute smoke timeout on Windows local Spark in prior runs; current generated forecast artifact is stale and lacks `grid_lat`, `forecast_origin_ts`, and `target_ts`
- Next technical action: optimize or parameterize training hyperparameters so a small standalone train smoke completes and regenerates forecast JSON/metrics.json with the corrected lag and merge semantics, then run browser visual QA for the frontend

## 5. Data and Schema State

### 5.1 Measurement Record

Actual fields:

- `sensor_id`
- `location_id`
- `location_name`
- `datetime_utc`
- `datetime_local`
- `latitude`
- `longitude`
- `parameter`
- `unit`
- `value`
- `source`
- derived storage partition `date`

Stable dedup identity:

- `sensor_id + parameter + datetime_utc`

Partition strategy:

- `parameter/date`

### 5.2 Training Feature Row

Actual fields include:

- `grid_lat`
- `grid_lon`
- `parameter`
- `hour_ts`
- `value`
- `sensor_count`
- `latitude`
- `longitude`
- `hour`
- `day_of_week`
- `lag_1h`
- `lag_3h`
- `lag_24h`
- `forecast_origin_ts`
- `target_ts`
- `horizon_hour`
- `label`
- `split` after Phase 2 split function

### 5.3 Forecast Output

Forecast JSON rows include:

- `model`
- `grid_lat`
- `grid_lon`
- `latitude`
- `longitude`
- `forecast_origin_ts`
- `target_ts`
- `forecast_ts`
- `horizon_hour`
- `sensor_count`
- `values`
- `aqi`
- `category`

### 5.4 Current API Output

Current API points include:

- `grid_lat`
- `grid_lon`
- `latitude`
- `longitude`
- `values`
- `aqi`
- `category`
- `observation_ts`
- `observation_age_hours`
- `freshness_status`
- `sensor_count`

## 6. Commands and Verification

### Successfully Executed

- `Get-Content -Raw docs/IMPLEMENTATION_PLAN.md`
- `Get-Content -Raw docs/AGENT_HANDOFF.md`
- `Get-Content -Raw docs/ARCHITECTURE.md`
- `git status --short`
- `git diff --stat`
- `git diff -- README.md`
- `git log --oneline -n 10`
- `git branch --show-current`
- `Select-String -Path README.md -Pattern '[^\x00-\x7F]'` -> no matches
- `Select-String -Path README.md,docs\AGENT_HANDOFF.md -Pattern '[^\x00-\x7F]'` -> no matches
- `python -m py_compile src\io.py src\openaq_client.py src\config.py scripts\ingest_openaq.py scripts\generate_sample_data.py scripts\train_forecast_spark.py`
- `python -m pytest tests\test_storage_phase2.py`
- `python -m pytest tests\test_api_phase3.py tests\test_storage_phase2.py`
- `python -m pytest` -> 38 passed before Phase 3, then 40 passed after Phase 3
- Temporary uvicorn job with HTTP checks -> `/` 200, current/forecast API reachable
- `python -m pytest tests\test_spark_features.py` -> 17 passed after corrective P0 lag fix
- `python -m pytest tests\test_train_forecast_integration.py` -> 1 passed in 343.17 seconds after increasing command timeout
- `python -m pytest` -> 42 passed after corrective P0 lag fix
- Forecast artifact inspection: `data\predictions\forecast_24h.json` has 5,856 rows but no `forecast_origin_ts`/`target_ts`
- Metrics artifact inspection: `data\predictions\metrics.json` is absent
- `python -m pytest tests\test_forecast_outputs.py` -> 1 passed after P1 merge identity test rewrite
- `python -m pytest tests\test_spark_features.py tests\test_forecast_outputs.py` -> 18 passed
- `python -m pytest` -> 43 passed after P1 merge identity fix
- Forecast artifact inspection after P1 fix: existing `data\predictions\forecast_24h.json` still has 5,856 rows and lacks `grid_lat`, `forecast_origin_ts`, and `target_ts`

### Failed or Partial Commands

- `python scripts\train_forecast_spark.py` with temp env paths and 12 sensors/4 days timed out after 304 seconds.
- `python scripts\train_forecast_spark.py` with temp env paths and 4 sensors/3 days timed out after 304 seconds.
- Attempts to leave uvicorn running with `Start-Process` failed due Windows `Path/PATH` duplicate environment issue; temporary `Start-Job` verification worked inside one command.
- `python -m pytest tests\test_train_forecast_integration.py` with 120-second command timeout timed out before completion; rerun with longer timeout passed.
- Initial `python -m pytest tests\test_forecast_outputs.py` failed because the test used a local Python-row Spark DataFrame that crashed the Windows Spark Python worker during Parquet write; test setup was rewritten and rerun successfully.

### Not Yet Run

- OpenAQ ingestion with a real API key.
- Real HDFS cluster write/read verification.
- Browser screenshot/visual QA.
- Long-running standalone Spark train completion after corrective lag and merge-identity fixes.
- Regeneration of forecast JSON and metrics artifacts after corrective lag and merge-identity fixes.

## 7. Test Status

| Test Area | Status | Evidence |
|---|---|---|
| AQI unit tests | PASS | `tests/test_aqi.py` in full pytest |
| Spark feature tests | PASS | `tests/test_spark_features.py` passed standalone with 17 tests and in full pytest |
| Forecast output tests | PASS | `tests/test_forecast_outputs.py` covers PM2.5/PM10 merge identity |
| Horizon semantics tests | PASS | H+1..H+24 tests pass |
| Storage tests | PASS | `tests/test_storage_phase2.py` pass |
| API tests | PASS | `tests/test_api_phase3.py` pass |
| Integration tests | PASS | `tests/test_train_forecast_integration.py` passed with longer timeout |
| Frontend verification | PARTIAL | `/` HTTP 200; no visual/browser QA |

## 8. Exact Next Step

Primary next action:

```text
Optimize or parameterize scripts/train_forecast_spark.py so a small standalone Spark train run completes with the corrected inference lag and forecast merge semantics, regenerates forecast JSON with `grid_lat`/`grid_lon`/`forecast_origin_ts`/`target_ts`, writes metrics.json, then run browser visual QA for the Phase 4 frontend.
```

## 9. Instructions for the Next Agent

1. Read `docs/IMPLEMENTATION_PLAN.md` completely.
2. Read this handoff completely.
3. Inspect `git status`, `git diff`, and recent commits.
4. Treat source code and tests as stronger evidence than this document.
5. Do not claim real HDFS or OpenAQ success until actually run.
6. Do not mark Phase 2 complete until the standalone train smoke gap is resolved or explicitly accepted.
7. Do not mark Phase 4 complete until visual/browser QA is run.
8. Update this handoff after meaningful progress.

## 10. Update Log

### 2026-07-09 - OpenAI Codex

Phase:

- Phase 5 corrective implementation

Work:

- Verified the suspected P1 PM2.5/PM10 forecast merge identity defect in `scripts/train_forecast_spark.py`.
- Changed `write_outputs()` to merge forecast pollutants by stable `grid_lat`/`grid_lon` instead of rounded display latitude/longitude.
- Added `grid_lat` and `grid_lon` to generated forecast JSON rows.
- Added regression coverage in `tests/test_forecast_outputs.py`.
- Re-inspected existing generated artifacts; forecast JSON is still stale and metrics JSON is absent.

Tests:

- `python -m pytest tests\test_forecast_outputs.py` -> 1 passed after rewriting the test setup away from a local Python-row Spark DataFrame.
- `python -m pytest tests\test_spark_features.py tests\test_forecast_outputs.py` -> 18 passed.
- `python -m pytest` -> 43 passed.

Result:

- P1 merge identity defect is fixed and covered by regression tests.
- Generated forecast/metrics artifacts still need regeneration after the standalone training runtime gap is addressed.

### 2026-07-08 - OpenAI Codex

Phase:

- Phase 5 corrective implementation

Work:

- Verified the suspected P0 train/inference lag mismatch in `scripts/train_forecast_spark.py`.
- Fixed inference candidate construction so `lag_1h`, `lag_3h`, and `lag_24h` come from historical hourly rows instead of the current value.
- Added regression tests for inference lag values and missing required lag history.
- Inspected generated artifacts and confirmed existing forecast JSON is stale/missing origin and target timestamps; metrics JSON is absent.

Tests:

- `python -m pytest tests\test_spark_features.py` -> 17 passed.
- `python -m pytest tests\test_train_forecast_integration.py` -> first command timed out at 120 seconds; rerun with longer timeout passed.
- `python -m pytest` -> 42 passed.

Result:

- P0 code defect is fixed and covered by regression tests.
- Artifact regeneration and standalone train runtime optimization remain the next verification gap.

### 2026-07-08 - OpenAI Codex

Phase:

- Phase 5 documentation

Work:

- Rewrote `README.md` fully in English with detailed setup, architecture, workflow, API, storage, Spark, AQI, testing, troubleshooting, and limitation sections.
- Updated this handoff to reflect the docs-only change and current git state.

Tests:

- Not run; docs-only change.
- README checked for non-ASCII characters; no matches.

Result:

- README is now detailed English documentation and preserves honesty about unverified OpenAQ, HDFS, standalone training runtime, and frontend visual QA gaps.

### 2026-07-08 - OpenAI Codex

Phase:

- Phase 2, Phase 3, Phase 4, Phase 5 partial

Work:

- Verified Phase 1.
- Implemented Phase 2 ingestion/storage/split/metrics changes.
- Implemented Phase 3 backend APIs and API tests.
- Implemented Phase 4 map-first frontend using `impeccable` because `/taste` was unavailable.
- Updated README and `.gitignore`.

Tests:

- Full pytest passed: 40 tests.
- Temporary uvicorn HTTP verification passed for `/`, `/api/current`, `/api/forecast`, `/api/health`.

Result:

- Repo is much closer to the five-phase target.
- Remaining risks are explicit: standalone Spark train runtime, real OpenAQ/HDFS verification, and visual QA.
