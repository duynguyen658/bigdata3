# CLAUDE.md
## Claude Code Instructions for HCMC AQI Monitoring and Forecasting

These instructions apply specifically to Claude Code when working in this repository.

---

## 1. Required Startup Workflow

Before major implementation work, read completely:

1. `docs/IMPLEMENTATION_PLAN.md`
2. `docs/AGENT_HANDOFF.md`
3. `docs/ARCHITECTURE.md` when architecture or data flow is involved

Then inspect the actual repository.

At minimum:

```bash
git status
git diff
git log --oneline -n 10
```

Treat source code, tests, git state, and generated artifacts as stronger evidence than the handoff document.

Do not trust README claims without checking source.

---

## 2. Work Only Within the Active Phase

Use the current phase from:

```text
docs/AGENT_HANDOFF.md
```

Do not automatically start the next phase.

If the current task is Phase 2, do not begin Phase 3 until:

- requested work is implemented;
- relevant tests are run;
- the diff is reviewed;
- handoff state is updated;
- the user approves or explicitly requests continuation.

---

## 3. Five-Phase Plan

### Phase 1 — Correctness Foundation

- hourly regularization
- true hourly lag semantics
- H+1 through H+24 targets
- `horizon_hour`
- `forecast_origin_ts`
- `target_ts`
- AQI correctness
- unsupported pollutant handling
- `Asia/Ho_Chi_Minh` local features
- deterministic tests

### Phase 2 — Ingestion, Storage, and ML Evaluation

- OpenAQ ingestion improvements
- append/dedup storage
- partition strategy
- local Parquet
- HDFS-compatible writes
- chronological train/validation/test
- target leakage prevention
- MAE/RMSE/R²
- metrics artifacts

### Phase 3 — Backend APIs and Freshness

- `/api/current`
- `/api/forecast`
- `/api/hotspots`
- `/api/metrics`
- `/api/health`
- current/forecast separation
- per-grid freshness
- stale/missing behavior

### Phase 4 — Frontend Redesign

Claude Code must explicitly invoke and use:

```text
/taste
```

before or during frontend redesign work.

Required frontend goals:

- map-first dashboard
- Current / Forecast switch
- H+1 through H+24 controls
- actual target timestamp
- RF / GBT selector
- real metrics only
- compact KPI strip
- ranked hotspots
- freshness states
- stale/missing states
- responsive desktop/tablet/mobile
- accessibility
- preserve Leaflet unless rewrite is justified

### Phase 5 — Verification and Documentation

- full tests
- end-to-end verification
- sample mode
- OpenAQ mode when credentials exist
- Spark training verification
- FastAPI verification
- frontend verification
- README update
- architecture documentation
- limitations
- exact commands

---

## 4. Forecasting Rules

Preserve stacked multi-horizon training.

Required semantics:

```text
features(t) + horizon_hour=1  -> target(t+1h)
features(t) + horizon_hour=24 -> target(t+24h)
```

The project should use four model pipelines:

- RF PM2.5
- RF PM10
- GBT PM2.5
- GBT PM10

Do not revert to:

- one fake T+24 model shown as H+1 through H+24;
- 96 independent models;

unless a documented technical change explicitly justifies it.

---

## 5. Hourly and Leakage Rules

Hourly regularization must happen before lag/lead generation.

Missing hours must remain explicit.

Preserve:

- `forecast_origin_ts`
- `target_ts`

For train/validation/test:

- use chronological splits;
- prevent a row from staying in an earlier split when its `target_ts` crosses a boundary;
- do not use random split for time-series evaluation.

---

## 6. AQI Rules

Use the documented EPA AQI standard implementation.

Do not blindly rely on Python `round()` if that violates the required preprocessing rule.

Unsupported pollutants must not default to PM10.

AQI boundary behavior must be tested.

---

## 7. Timezone Rules

Use UTC for stable storage/order where appropriate.

Use:

```text
Asia/Ho_Chi_Minh
```

for local temporal features and user-facing local timestamps where appropriate.

---

## 8. Storage Rules

Do not erase all historical measurements on repeated ingestion.

Storage implementation must define:

- append
- deduplication
- overlap handling
- partitioning

For `hdfs://` paths, use Spark/Hadoop-compatible writes.

If a real HDFS cluster is unavailable:

- implement correctly;
- test routing/logic where possible;
- mark real end-to-end verification as `NOT_RUN`;
- do not claim success.

---

## 9. Frontend / Taste Workflow

When Phase 4 begins:

1. read actual backend routes and schemas;
2. inspect current frontend files;
3. invoke `/taste`;
4. redesign the real implementation;
5. preserve backend truth;
6. do not fabricate missing values;
7. verify responsive behavior.

The design must avoid:

- generic AI dashboard appearance
- excessive cards
- unnecessary gradients
- glassmorphism clutter
- decorative UI without function

The map must remain the primary visual anchor.

---

## 10. Testing and Self-Review

After implementation work:

1. run relevant tests;
2. inspect actual diff;
3. verify no unrelated changes;
4. fix failures;
5. run tests again if code changed;
6. update `docs/AGENT_HANDOFF.md`.

For Phase 1 self-review, explicitly verify:

- H+1 means true t+1h;
- H+24 means true t+24h;
- missing hours cannot corrupt lag semantics;
- origin and target timestamps are correct;
- train/inference feature semantics are compatible;
- AQI preprocessing matches the documented standard;
- unsupported pollutants do not default to PM10;
- local temporal features use `Asia/Ho_Chi_Minh`;
- tests verify semantics rather than arbitrary prediction inequality.

For later phases, apply equivalent strict self-review before proceeding.

---

## 11. Handoff Update Workflow

After meaningful progress, update:

```text
docs/AGENT_HANDOFF.md
```

Record:

- actual branch
- actual latest commit
- actual working-tree state
- active phase
- active task
- files changed
- commands executed
- tests
- pass/fail/skip counts
- unverified items
- exact next step
- append-only update log entry

Do not fabricate commands or test results.

Do not mark a phase complete unless implementation and relevant verification exist.

---

## 12. Cross-Agent Continuity

This repository may be handed from Claude Code to OpenAI Codex.

Before ending a major checkpoint:

1. update `docs/AGENT_HANDOFF.md`;
2. ensure `Exact Next Step` contains one concrete next action;
3. record current blockers;
4. record test state;
5. record uncommitted changes;
6. avoid leaving ambiguous partial work.

Another agent should be able to continue without prior chat context.

---

## 13. Truthfulness Rules

Do not claim:

- true realtime if the system is batch/polling only;
- successful OpenAQ verification without a successful authenticated run;
- successful HDFS verification without a real cluster test;
- good model quality without held-out evaluation;
- successful frontend verification without actually checking it;
- completed work without verification.

Use:

```text
NOT_VERIFIED
PARTIAL
NOT_RUN
```

when appropriate.

---

## 14. Phase 4 Mandatory Skill Rule

Phase 4 frontend work must explicitly use:

```text
/taste
```

Do not silently skip this requirement.

After `/taste` work, perform a final visual quality review for:

- hierarchy
- spacing
- typography
- map/control competition
- responsiveness
- accessibility
- stale/missing-state clarity
- generic AI styling regressions
