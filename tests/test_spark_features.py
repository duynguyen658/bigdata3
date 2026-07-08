from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.train_forecast_spark import LOCAL_TZ, feature_frame, forecast_candidate_frame, prepare_hourly, split_feature_frame
from src.io import write_measurements_parquet


GRID_LAT = 10.7769
GRID_LON = 106.7009
BASE_UTC = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

HOURLY_SCHEMA = StructType(
    [
        StructField("grid_lat", DoubleType(), False),
        StructField("grid_lon", DoubleType(), False),
        StructField("parameter", StringType(), False),
        StructField("hour_ts", TimestampType(), False),
        StructField("value", DoubleType(), True),
        StructField("sensor_count", LongType(), False),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)

# NOTE on timestamps in this file: pyspark's `collect()` converts TimestampType
# values to Python via `datetime.fromtimestamp()`, which uses the *local OS
# timezone* of the machine running the tests -- not `spark.sql.session.timeZone`
# and not the value's true UTC instant. That conversion is a display-layer
# artifact of the Python driver only; every Spark-side computation (hour(),
# dayofweek(), lag/lead, interval arithmetic, unix_timestamp()) operates on the
# correct internal instant regardless of the host's local timezone. To keep
# these tests correct on any machine, timestamps are therefore never compared
# as collected Python datetime objects -- they are compared as
# `F.unix_timestamp(...)` (epoch seconds), which is an unambiguous absolute
# instant computed entirely inside Spark before collection.


def _epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _hourly_row(hour_index: int, value: float | None, sensor_count: int = 1):
    return (
        GRID_LAT,
        GRID_LON,
        "pm25",
        BASE_UTC + timedelta(hours=hour_index),
        value,
        sensor_count,
        GRID_LAT,
        GRID_LON,
    )


def _build_hourly_df(spark_session, hour_indices, gap_hours=frozenset()):
    start = min(hour_indices)
    end = max(hour_indices)
    gap_values = [int(h) for h in gap_hours]
    is_gap = F.col("hour_index").isin(gap_values) if gap_values else F.lit(False)
    return (
        spark_session.range(start, end + 1)
        .withColumnRenamed("id", "hour_index")
        .select(
            F.lit(GRID_LAT).cast("double").alias("grid_lat"),
            F.lit(GRID_LON).cast("double").alias("grid_lon"),
            F.lit("pm25").alias("parameter"),
            F.from_unixtime(F.lit(_epoch(BASE_UTC)) + F.col("hour_index") * F.lit(3600)).cast("timestamp").alias("hour_ts"),
            F.when(is_gap, F.lit(None).cast("double")).otherwise(F.col("hour_index").cast("double")).alias("value"),
            F.when(is_gap, F.lit(0)).otherwise(F.lit(1)).cast("long").alias("sensor_count"),
            F.lit(GRID_LAT).cast("double").alias("latitude"),
            F.lit(GRID_LON).cast("double").alias("longitude"),
        )
    )


def _row_for(features_df, origin_hour_index: int, horizon_hour: int):
    """Find the stacked feature row for a given origin hour + horizon.

    Adds `origin_epoch`/`target_epoch` (plain longs, safe to collect) so
    callers can assert absolute timestamp correctness without touching a
    raw collected TimestampType value.
    """
    origin_epoch = _epoch(BASE_UTC + timedelta(hours=origin_hour_index))
    matches = (
        features_df.where(
            (F.unix_timestamp(F.col("forecast_origin_ts")) == origin_epoch)
            & (F.col("horizon_hour") == horizon_hour)
        )
        .withColumn("origin_epoch", F.unix_timestamp(F.col("forecast_origin_ts")))
        .withColumn("target_epoch", F.unix_timestamp(F.col("target_ts")))
        .collect()
    )
    return matches[0] if matches else None


# --- 1. prepare_hourly: dense hourly regularization (missing hourly observations) ---


def test_prepare_hourly_fills_missing_hour_as_explicit_null_row(spark_session, tmp_path):
    rows = []
    for h in range(0, 51):
        if h == 20:
            continue  # deliberately missing observation
        rows.append(
            {
                "sensor_id": 1,
                "location_id": 1,
                "location_name": "test-sensor",
                "datetime_utc": (BASE_UTC + timedelta(hours=h)).isoformat(),
                "datetime_local": (BASE_UTC + timedelta(hours=h + 7)).isoformat(),
                "latitude": GRID_LAT,
                "longitude": GRID_LON,
                "parameter": "pm25",
                "unit": "ug/m3",
                "value": float(h),
                "source": "test",
            }
        )
    df = pd.DataFrame(rows)
    measurements_path = str(tmp_path / "measurements")
    write_measurements_parquet(df, measurements_path)

    dense = prepare_hourly(measurements_path).withColumn(
        "epoch", F.unix_timestamp(F.col("hour_ts"))
    )
    collected = {row["epoch"]: row for row in dense.select("epoch", "value", "sensor_count").collect()}

    # 51 distinct hours (0..50) must all be present, including the gap.
    assert len(collected) == 51

    gap_row = collected[_epoch(BASE_UTC + timedelta(hours=20))]
    assert gap_row["value"] is None
    assert gap_row["sensor_count"] == 0

    real_row = collected[_epoch(BASE_UTC + timedelta(hours=19))]
    assert real_row["value"] == 19.0
    assert real_row["sensor_count"] == 1


def test_latest_hour_per_cell_is_always_a_real_observation_never_a_gap(spark_session, tmp_path):
    """build_forecast anchors every forecast on the row with max(hour_ts) per
    grid cell, freezing lag_1h/3h/24h at its `value`. That is only safe
    because the dense calendar's bounds are derived from the real (pre-fill)
    data's own min/max hour -- so the last hour in the calendar can never be
    a filled gap, even if the second-to-last hour is. This locks in that
    invariant directly, since a regression here would silently anchor
    forecasts on a null observation.
    """
    rows = []
    for h in range(0, 51):
        if h == 49:
            continue  # gap immediately before the last real observation
        rows.append(
            {
                "sensor_id": 1,
                "location_id": 1,
                "location_name": "test-sensor",
                "datetime_utc": (BASE_UTC + timedelta(hours=h)).isoformat(),
                "datetime_local": (BASE_UTC + timedelta(hours=h + 7)).isoformat(),
                "latitude": GRID_LAT,
                "longitude": GRID_LON,
                "parameter": "pm25",
                "unit": "ug/m3",
                "value": float(h),
                "source": "test",
            }
        )
    df = pd.DataFrame(rows)
    measurements_path = str(tmp_path / "measurements_latest")
    write_measurements_parquet(df, measurements_path)

    dense = prepare_hourly(measurements_path)
    max_row = dense.orderBy(F.col("hour_ts").desc()).limit(1).collect()[0]
    assert max_row["value"] == 50.0
    assert max_row["sensor_count"] == 1


# --- 2. feature_frame: lag propagation through a gap (row-offset vs hour-offset) ---
# All gap tests use a single shared layout: hours 0..100, one missing
# observation at hour 40. Origins are always >= 24 so `lag_24h` has enough
# history to be non-null *except* when the 24h lookback specifically lands on
# the gap -- this cleanly isolates "dropped because of the gap" from "dropped
# because there isn't 24h of history yet" (a real, separate constraint of the
# stacked feature frame that earlier drafts of these tests conflated).

GAP_HOUR = 40
GAP_RANGE = range(0, 101)


def test_lag_1h_is_null_and_row_dropped_when_previous_hour_is_a_gap(spark_session):
    hourly = _build_hourly_df(spark_session, GAP_RANGE, gap_hours={GAP_HOUR})
    features = feature_frame(hourly)

    # Origin 41: lag_1h looks back to hour 40 (the gap) -> null -> dropped.
    assert _row_for(features, GAP_HOUR + 1, horizon_hour=1) is None


def test_lag_3h_is_null_and_row_dropped_when_three_hours_back_is_a_gap(spark_session):
    hourly = _build_hourly_df(spark_session, GAP_RANGE, gap_hours={GAP_HOUR})
    features = feature_frame(hourly)

    # Origin 43: lag_3h looks back to hour 40 (the gap) -> null -> dropped.
    assert _row_for(features, GAP_HOUR + 3, horizon_hour=1) is None


def test_lag_24h_is_null_and_row_dropped_when_24h_back_is_a_gap(spark_session):
    hourly = _build_hourly_df(spark_session, GAP_RANGE, gap_hours={GAP_HOUR})
    features = feature_frame(hourly)

    origin = GAP_HOUR + 24  # lag_24h looks back exactly to the gap hour.
    for h in (1, 3, 24):
        assert _row_for(features, origin, horizon_hour=h) is None


def test_lags_are_true_hour_offsets_not_row_offsets_away_from_the_gap(spark_session):
    hourly = _build_hourly_df(spark_session, GAP_RANGE, gap_hours={GAP_HOUR})
    features = feature_frame(hourly)

    origin = 50  # far enough from hour 40 that none of its lags touch the gap
    row = _row_for(features, origin, horizon_hour=1)
    assert row is not None
    assert row["lag_1h"] == 49.0
    assert row["lag_3h"] == 47.0
    assert row["lag_24h"] == 26.0


def test_label_for_horizon_that_skips_over_the_gap_still_resolves(spark_session):
    # horizon_hour=2 at origin 39 targets hour 41 (present) even though hour
    # 40 (the intervening hour) is a gap -- lead() reads the target row
    # directly, it doesn't traverse the hours in between.
    hourly = _build_hourly_df(spark_session, GAP_RANGE, gap_hours={GAP_HOUR})
    features = feature_frame(hourly)

    origin = GAP_HOUR - 1
    row = _row_for(features, origin, horizon_hour=2)
    assert row is not None
    assert row["label"] == float(GAP_HOUR + 1)

    # But horizon_hour=1 at the same origin targets hour 40 itself (the gap).
    assert _row_for(features, origin, horizon_hour=1) is None


# --- 3. Correct multi-horizon target construction: H+1 through H+24 -----------------
# Origins here are always >= 24 (so lag_24h has full history) and leave at
# least 24 hours of headroom after the origin (so every horizon's label is
# in range) -- both are real preconditions of the stacked feature frame that
# must be respected for a row to survive `na.drop`.

PLAIN_RANGE = range(0, 60)
PLAIN_ORIGIN = 30


def test_horizon_1_maps_to_value_at_t_plus_1_hour(spark_session):
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)

    row = _row_for(features, PLAIN_ORIGIN, horizon_hour=1)
    assert row is not None
    assert row["label"] == float(PLAIN_ORIGIN + 1)
    assert row["target_epoch"] == _epoch(BASE_UTC + timedelta(hours=PLAIN_ORIGIN + 1))


def test_horizon_24_maps_to_value_at_t_plus_24_hours(spark_session):
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)

    row = _row_for(features, PLAIN_ORIGIN, horizon_hour=24)
    assert row is not None
    assert row["label"] == float(PLAIN_ORIGIN + 24)
    assert row["target_epoch"] == _epoch(BASE_UTC + timedelta(hours=PLAIN_ORIGIN + 24))


def test_all_24_horizons_are_present_for_an_interior_origin(spark_session):
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)

    for h in range(1, 25):
        row = _row_for(features, PLAIN_ORIGIN, horizon_hour=h)
        assert row is not None, f"missing horizon {h}"
        assert row["label"] == float(PLAIN_ORIGIN + h)
        assert row["target_epoch"] == _epoch(BASE_UTC + timedelta(hours=PLAIN_ORIGIN + h))


def test_horizon_1_and_horizon_24_are_distinct_targets_not_one_relabeled_value(spark_session):
    # Regression guard against the original bug: a single lead(value, 24)
    # label must not be reused verbatim as the H+1 target. This checks target
    # *construction* (distinct target times/values by design), not prediction
    # inequality -- two different horizons could legitimately predict the same
    # number, so that would not be a valid test.
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)

    row_h1 = _row_for(features, PLAIN_ORIGIN, horizon_hour=1)
    row_h24 = _row_for(features, PLAIN_ORIGIN, horizon_hour=24)
    assert row_h1["label"] == float(PLAIN_ORIGIN + 1)
    assert row_h24["label"] == float(PLAIN_ORIGIN + 24)
    assert row_h1["target_epoch"] != row_h24["target_epoch"]
    assert row_h1["target_epoch"] == row_h1["origin_epoch"] + 1 * 3600
    assert row_h24["target_epoch"] == row_h24["origin_epoch"] + 24 * 3600


# --- 4. forecast_origin_ts / target_ts explicit columns (leakage-prevention groundwork) ---


def test_forecast_origin_ts_equals_hour_ts_for_every_row(spark_session):
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)
    mismatches = features.where(
        F.unix_timestamp(F.col("forecast_origin_ts")) != F.unix_timestamp(F.col("hour_ts"))
    ).count()
    assert mismatches == 0
    assert features.where(F.col("horizon_hour") == 1).count() > 0  # sanity: rows exist


def test_target_ts_equals_origin_plus_horizon_for_every_row(spark_session):
    hourly = _build_hourly_df(spark_session, range(0, 40))
    features = feature_frame(hourly)
    mismatches = features.where(
        F.unix_timestamp(F.col("target_ts"))
        != F.unix_timestamp(F.col("forecast_origin_ts")) + F.col("horizon_hour") * 3600
    ).count()
    assert mismatches == 0
    assert features.count() > 0  # sanity: rows exist


# --- 5. Timezone correctness: Asia/Ho_Chi_Minh, not UTC -----------------------------


def test_hour_and_day_of_week_use_local_time_not_utc(spark_session):
    assert LOCAL_TZ == "Asia/Ho_Chi_Minh"
    hourly = _build_hourly_df(spark_session, PLAIN_RANGE)
    features = feature_frame(hourly)

    # Origin hour 42 = BASE_UTC + 42h = 2024-01-02T18:00:00Z (UTC: Tuesday, hour 18).
    # In Asia/Ho_Chi_Minh (UTC+7) this is 2024-01-03T01:00:00 (Wednesday, hour 1)
    # -- a day-boundary crossing, so the test also distinguishes local weekday
    # from UTC weekday, not just the hour.
    origin = 42
    row = _row_for(features, origin, horizon_hour=1)
    assert row is not None

    utc_ts = BASE_UTC + timedelta(hours=origin)
    assert utc_ts.hour == 18
    assert utc_ts.isoweekday() == 2  # Tuesday (ISO: Mon=1..Sun=7)

    assert row["hour"] == 1
    assert row["day_of_week"] == 4  # Spark dayofweek: Sun=1..Sat=7 -> Wednesday=4
    # Explicitly not the (incorrect) UTC-derived values.
    assert row["hour"] != 18
    assert row["day_of_week"] != 3  # would be Tuesday if UTC were used


# --- 6. Chronological split: boundaries use target_ts to prevent leakage ------------


def test_chronological_split_uses_target_ts_boundaries(spark_session):
    hourly = _build_hourly_df(spark_session, range(0, 220))
    features = feature_frame(hourly)
    split = split_feature_frame(features).withColumn(
        "target_epoch", F.unix_timestamp(F.col("target_ts"))
    )

    stats = {
        row["split"]: row
        for row in split.groupBy("split")
        .agg(
            F.count("*").alias("count"),
            F.min("target_epoch").alias("min_target_epoch"),
            F.max("target_epoch").alias("max_target_epoch"),
        )
        .collect()
    }

    assert set(stats) == {"train", "validation", "test"}
    assert all(row["count"] > 0 for row in stats.values())
    assert stats["train"]["max_target_epoch"] < stats["validation"]["min_target_epoch"]
    assert stats["validation"]["max_target_epoch"] < stats["test"]["min_target_epoch"]


# --- 7. Inference feature compatibility: forecast candidates use real lags ---------


def test_forecast_candidate_lags_use_history_not_latest_value(spark_session):
    hourly = _build_hourly_df(spark_session, range(0, 51))
    candidate = forecast_candidate_frame(hourly)

    row = candidate.where(F.col("horizon_hour") == 1).collect()[0]
    assert row["value"] == 50.0
    assert row["lag_1h"] == 49.0
    assert row["lag_3h"] == 47.0
    assert row["lag_24h"] == 26.0

    assert row["lag_1h"] != row["value"]
    assert row["lag_3h"] != row["value"]
    assert row["lag_24h"] != row["value"]
    assert candidate.count() == 24


def test_forecast_candidate_drops_latest_origin_when_required_lag_is_missing(spark_session):
    hourly = _build_hourly_df(spark_session, range(0, 51), gap_hours={49})
    candidate = forecast_candidate_frame(hourly)

    # Latest real observation is hour 50, but lag_1h points to explicit gap
    # hour 49. Inference must not fill that missing history with current value.
    assert candidate.count() == 0
