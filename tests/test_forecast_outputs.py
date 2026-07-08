from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import functions as F

from scripts.train_forecast_spark import write_outputs


def test_write_outputs_merges_pollutants_by_grid_identity_not_display_coordinates(spark_session, tmp_path):
    origin = datetime(2026, 7, 8, 0, 0, 0)
    target = datetime(2026, 7, 8, 1, 0, 0)
    forecast = (
        spark_session.range(0, 2)
        .withColumn("model", F.lit("random_forest"))
        .withColumn("grid_lat", F.lit(10.78).cast("double"))
        .withColumn("grid_lon", F.lit(106.70).cast("double"))
        .withColumn("latitude", F.when(F.col("id") == 0, F.lit(10.77691)).otherwise(F.lit(10.78349)).cast("double"))
        .withColumn("longitude", F.when(F.col("id") == 0, F.lit(106.70091)).otherwise(F.lit(106.70749)).cast("double"))
        .withColumn("parameter", F.when(F.col("id") == 0, F.lit("pm25")).otherwise(F.lit("pm10")))
        .withColumn("sensor_count", F.when(F.col("id") == 0, F.lit(2)).otherwise(F.lit(3)).cast("int"))
        .withColumn("forecast_origin_ts", F.from_unixtime(F.lit(int(origin.timestamp()))).cast("timestamp"))
        .withColumn("target_ts", F.from_unixtime(F.lit(int(target.timestamp()))).cast("timestamp"))
        .withColumn("forecast_ts", F.col("target_ts"))
        .withColumn("horizon_hour", F.lit(1).cast("int"))
        .withColumn("predicted_value", F.when(F.col("id") == 0, F.lit(30.4)).otherwise(F.lit(52.1)).cast("double"))
        .drop("id")
    )

    json_path = tmp_path / "forecast.json"
    parquet_path = tmp_path / "forecast_parquet"
    write_outputs(forecast, str(json_path), str(parquet_path))

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(payload) == 1

    point = payload[0]
    assert point["grid_lat"] == 10.78
    assert point["grid_lon"] == 106.7
    assert point["values"] == {"pm25": 30.4, "pm10": 52.1}
    assert point["sensor_count"] == 3
    assert round(point["latitude"], 5) == 10.7802
    assert round(point["longitude"], 5) == 106.7042
    assert point["forecast_origin_ts"] == "2026-07-08T00:00:00"
    assert point["target_ts"] == "2026-07-08T01:00:00"
