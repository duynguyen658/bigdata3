# AGENTS.md
## Cross-Agent Instructions for HCMC AQI Monitoring and Forecasting
## 1. Required Reading Before Major Work

Before making meaningful implementation changes, read completely:

1. `docs/IMPLEMENTATION_PLAN.md`
2. `docs/AGENT_HANDOFF.md`
3. `docs/ARCHITECTURE.md` when the task affects architecture, data flow, storage, ML, APIs, or frontend structure

Then inspect the actual repository state.

At minimum, inspect:

```bash
git status
git diff
git log --oneline -n 10
```

Treat the following as stronger evidence than handoff summaries:

- source code
- tests
- generated artifacts
- git state
- actual command results

Do not assume the handoff document is perfectly current.

---

## 2. Cross-Agent Continuity Rules

Every coding agent must:

1. Read `docs/IMPLEMENTATION_PLAN.md` before major work.
2. Read `docs/AGENT_HANDOFF.md` before major work.
3. Verify handoff claims against the actual repository.
4. Inspect `git status`.
5. Inspect `git diff`.
6. Inspect recent commits.
7. Avoid redoing completed work unless a verified defect exists.
8. Update `docs/AGENT_HANDOFF.md` after meaningful progress.
9. Never mark unverified work as complete.
10. Preserve the agreed five-phase plan unless a technically necessary change is documented.
11. Keep the `Exact Next Step` section current.
12. Record blockers honestly.
13. Preserve compatibility with the existing repository unless a change is technically justified.

---

## 3. Five-Phase Execution Plan

### Phase 1 — Correctness Foundation

- hourly regularization
- true hourly lag semantics
- H+1 through H+24 target construction
- `horizon_hour`
- `forecast_origin_ts`
- `target_ts`
- AQI correctness
- unsupported pollutant handling
- `Asia/Ho_Chi_Minh` local temporal features
- deterministic tests

### Phase 2 — Ingestion, Storage, and ML Evaluation

- OpenAQ historical ingestion improvements
- append/dedup storage
- partition strategy
- local Parquet correctness
- HDFS-compatible Spark writes
- chronological train/validation/test
- target leakage prevention
- MAE
- RMSE
- R²
- metrics artifacts by pollutant/model/horizon

### Phase 3 — Backend APIs and Freshness

- `/api/current`
- `/api/forecast`
- `/api/hotspots`
- `/api/metrics`
- `/api/health`
- current vs forecast separation
- per-grid freshness
- stale/missing behavior

### Phase 4 — Frontend Redesign

- map-first dashboard
- Current / Forecast switch
- H+1 through H+24 controls
- target timestamps
- RF / GBT selector
- real metrics only
- KPI strip
- hotspot panel
- freshness states
- responsive behavior
- accessibility

When Claude Code performs this phase, it must explicitly use `/taste`.

When another agent performs this phase, it should preserve the same design requirements and consult the project skill if available.

### Phase 5 — Verification and Documentation

- full test suite
- end-to-end verification
- sample-data verification
- OpenAQ verification when credentials are available
- Spark training verification
- FastAPI verification
- frontend verification
- README update
- architecture documentation
- exact run commands
- honest limitations

Do not skip directly to a later phase unless the user explicitly requests it or a dependency requires it.

---

## 4. Forecasting Decisions That Must Be Preserved

Use stacked multi-horizon supervised rows.

Required semantics:

```text
features(t) + horizon_hour=1  -> target(t+1h)
features(t) + horizon_hour=2  -> target(t+2h)
...
features(t) + horizon_hour=24 -> target(t+24h)
```

Preserve:

- `forecast_origin_ts`
- `target_ts`
- `horizon_hour`

Target architecture:

- Random Forest PM2.5
- Random Forest PM10
- GBT PM2.5
- GBT PM10

Do not revert to:

- one T+24 model relabeled as H+1 through H+24;
- 96 separate models;

unless a new documented technical decision explicitly justifies it.

---

## 5. Time-Series Rules

Hourly regularization must happen before lag/lead generation.

Missing hours must remain explicit.

Do not describe row offsets as hourly offsets unless the timeline is truly hourly.

For leakage-safe evaluation:

- train/validation/test must be chronological;
- `target_ts` must not cross into a later split.

Do not use random splitting for time-series model evaluation.

---

## 6. AQI Rules

Use the documented EPA AQI implementation selected by the repository.

Do not blindly use Python `round()` when it violates the required concentration preprocessing rule.

Unsupported pollutants must never silently default to PM10.

Boundary behavior must be deterministic and tested.

---

## 7. Timezone Rules

Use UTC where appropriate for:

- storage
- ordering
- deterministic temporal processing

Use:

```text
Asia/Ho_Chi_Minh
```

for local temporal features and local UI timestamps where appropriate.

---

## 8. Storage Rules

Historical ingestion must not erase all previous data by default.

Storage changes must define:

- append behavior
- deduplication behavior
- overlap handling
- partition strategy

For `hdfs://` paths, use a real Spark/Hadoop-compatible write path.

Do not claim successful real-HDFS verification unless an actual cluster test was run.

---

## 9. API Rules

Keep current and forecast data semantically separate.

Expected endpoints:

```text
GET /api/current
GET /api/forecast
GET /api/hotspots
GET /api/metrics
GET /api/health
```

Do not serve fake metrics.

Do not hide stale per-grid data behind one fresh global timestamp.

---

## 10. Frontend Rules

The map is the primary visual anchor.

Avoid:

- generic AI-dashboard styling
- excessive cards
- unnecessary gradients
- glassmorphism clutter
- decorative complexity without function

Preserve Leaflet unless a rewrite is technically justified.

Do not fabricate values that are missing from backend data.

---

## 11. Verification Rules

After meaningful implementation work:

1. Run relevant tests.
2. Inspect `git diff`.
3. Inspect `git status`.
4. Fix failures where appropriate.
5. Update `docs/AGENT_HANDOFF.md`.
6. Record actual commands run.
7. Record pass/fail/skip results.
8. Record unverified items.
9. Update `Exact Next Step`.

Never mark work complete without evidence.

---

## 12. Handoff Update Requirements

After a meaningful checkpoint, update:

```text
docs/AGENT_HANDOFF.md
```

At minimum record:

- current branch
- latest commit
- working-tree state
- active phase
- active task
- completed work
- files changed
- commands run
- tests
- known bugs/risks
- exact next step
- append-only update log entry

The next agent must be able to continue without reading prior chat history.

---

## 13. Truthfulness Rules

Do not claim:

- true realtime if only polling/batch exists;
- successful OpenAQ verification without a successful authenticated run;
- successful HDFS verification without a real cluster test;
- model quality without held-out evaluation;
- completed work without verification.

When uncertain, label the item:

```text
NOT_VERIFIED
```

or:

```text
PARTIAL
```

instead of guessing.
