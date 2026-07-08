from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.train_forecast_spark import build_forecast, feature_frame, train_models


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


def _synthetic_hourly(spark_session, hours: int = 200):
    return (
        spark_session.range(0, hours)
        .withColumnRenamed("id", "hour_index")
        .select(
            F.lit(GRID_LAT).cast("double").alias("grid_lat"),
            F.lit(GRID_LON).cast("double").alias("grid_lon"),
            F.lit("pm25").alias("parameter"),
            F.from_unixtime(F.lit(int(BASE_UTC.timestamp())) + F.col("hour_index") * F.lit(3600)).cast("timestamp").alias("hour_ts"),
            (
                F.lit(20.0)
                + F.lit(10.0) * F.sin(F.lit(2 * math.pi) * F.col("hour_index").cast("double") / F.lit(24.0))
            ).alias("value"),
            F.lit(1).cast("long").alias("sensor_count"),
            F.lit(GRID_LAT).cast("double").alias("latitude"),
            F.lit(GRID_LON).cast("double").alias("longitude"),
        )
    )


def test_train_and_forecast_round_trip_uses_horizon_conditioned_model(spark_session, tmp_path):
    """End-to-end smoke test: feature_frame's stacked schema must actually be
    fittable and scorable through the real Pipeline/VectorAssembler, and
    build_forecast's candidate frame must carry the same feature columns
    (including horizon_hour) that training produced -- this is what point 6
    (train/inference feature compatibility) means in practice, beyond just
    the two functions agreeing on column *names*.
    """
    hourly = _synthetic_hourly(spark_session)
    features = feature_frame(hourly)
    assert features.count() > 0

    models = train_models(features, str(tmp_path / "models"))
    # Only pm25 has data; pm10 has none and must be skipped, not error out.
    assert set(models.keys()) == {("random_forest", "pm25"), ("gbt", "pm25")}

    # forecast_ts is collected as a plain epoch-seconds long, not a raw
    # TimestampType -- pyspark's collect() converts TimestampType via the
    # local OS timezone (see test_spark_features.py's module note), which
    # would make a direct datetime equality check environment-dependent.
    forecast = build_forecast(hourly, models).withColumn(
        "forecast_epoch", F.unix_timestamp(F.col("forecast_ts"))
    )
    rows = forecast.collect()
    assert rows

    horizons_seen = {int(r.horizon_hour) for r in rows}
    assert horizons_seen == set(range(1, 25))

    models_seen = {r.model for r in rows}
    assert models_seen == {"random_forest", "gbt"}

    # The scored prediction must never be negative (build_forecast clamps via
    # F.greatest(prediction, 0.0)) and must be a finite real number.
    for r in rows:
        assert r.predicted_value >= 0.0
        assert math.isfinite(r.predicted_value)

    # forecast_ts must equal the origin (latest observed hour) plus horizon_hour.
    latest_hour_epoch = int((BASE_UTC + timedelta(hours=199)).timestamp())
    for r in rows:
        expected_epoch = latest_hour_epoch + int(r.horizon_hour) * 3600
        assert r.forecast_epoch == expected_epoch
