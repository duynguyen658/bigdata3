from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.aqi import aqi_category, pollutant_aqi
from src.config import settings


MODEL_BUILDERS = {
    "random_forest": lambda: RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        numTrees=24,
        maxDepth=7,
        seed=42,
    ),
    "gbt": lambda: GBTRegressor(
        featuresCol="features",
        labelCol="label",
        maxIter=40,
        maxDepth=5,
        stepSize=0.08,
        seed=42,
    ),
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("hcmc-aqi-forecast")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate()
    )


def prepare_hourly(measurements_path: str):
    spark = SparkSession.getActiveSession()
    assert spark is not None
    raw = spark.read.parquet(measurements_path)
    clean = (
        raw.select(
            F.col("sensor_id").cast("long"),
            F.col("location_id").cast("long"),
            "location_name",
            F.to_timestamp("datetime_utc").alias("ts"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.lower("parameter").alias("parameter"),
            F.col("value").cast("double").alias("value"),
        )
        .where("value is not null and latitude is not null and longitude is not null")
        .where(F.col("parameter").isin("pm25", "pm10"))
    )
    with_grid = clean.withColumn("hour_ts", F.date_trunc("hour", F.col("ts"))).withColumn(
        "grid_lat", F.round(F.col("latitude") / F.lit(0.02)) * F.lit(0.02)
    ).withColumn("grid_lon", F.round(F.col("longitude") / F.lit(0.02)) * F.lit(0.02))
    return (
        with_grid.groupBy("grid_lat", "grid_lon", "parameter", "hour_ts")
        .agg(
            F.avg("value").alias("value"),
            F.countDistinct("sensor_id").alias("sensor_count"),
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
        )
        .where("sensor_count > 0")
    )


def feature_frame(hourly):
    w = Window.partitionBy("grid_lat", "grid_lon", "parameter").orderBy("hour_ts")
    return (
        hourly.withColumn("hour", F.hour("hour_ts"))
        .withColumn("day_of_week", F.dayofweek("hour_ts"))
        .withColumn("lag_1h", F.lag("value", 1).over(w))
        .withColumn("lag_3h", F.lag("value", 3).over(w))
        .withColumn("lag_24h", F.lag("value", 24).over(w))
        .withColumn("label", F.lead("value", 24).over(w))
        .na.drop(subset=["lag_1h", "lag_3h", "lag_24h", "label"])
    )


def train_models(features, model_base_path: str):
    feature_cols = [
        "grid_lat",
        "grid_lon",
        "hour",
        "day_of_week",
        "lag_1h",
        "lag_3h",
        "lag_24h",
        "sensor_count",
    ]
    models = {}
    for parameter in ["pm25", "pm10"]:
        train = features.where(F.col("parameter") == parameter)
        if train.limit(1).count() == 0:
            continue
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        for model_name, estimator_builder in MODEL_BUILDERS.items():
            pipeline = Pipeline(stages=[assembler, estimator_builder()])
            model = pipeline.fit(train)
            save_path = f"{model_base_path.rstrip('/')}/{model_name}/{parameter}"
            model.write().overwrite().save(save_path)
            models[(model_name, parameter)] = model
    return models


def build_forecast(hourly, models):
    if not models:
        raise RuntimeError("No models were trained. Need PM2.5 or PM10 history.")

    latest_hour = hourly.agg(F.max("hour_ts").alias("latest")).collect()[0]["latest"]
    if latest_hour is None:
        raise RuntimeError("No hourly measurements available.")

    base_w = Window.partitionBy("grid_lat", "grid_lon", "parameter").orderBy(F.col("hour_ts").desc())
    latest = (
        hourly.withColumn("rank", F.row_number().over(base_w))
        .where("rank = 1")
        .drop("rank")
        .withColumn("lag_1h", F.col("value"))
        .withColumn("lag_3h", F.col("value"))
        .withColumn("lag_24h", F.col("value"))
    )

    spark = SparkSession.getActiveSession()
    assert spark is not None
    horizons = spark.range(1, 25).withColumnRenamed("id", "horizon_hour")
    candidate = (
        latest.crossJoin(horizons)
        .withColumn("forecast_ts", F.from_unixtime(F.unix_timestamp("hour_ts") + F.col("horizon_hour") * 3600).cast("timestamp"))
        .withColumn("hour", F.hour("forecast_ts"))
        .withColumn("day_of_week", F.dayofweek("forecast_ts"))
    )
    forecasts = []
    for (model_name, parameter), model in models.items():
        scored = model.transform(candidate.where(F.col("parameter") == parameter))
        forecasts.append(
            scored.select(
                F.lit(model_name).alias("model"),
                "grid_lat",
                "grid_lon",
                "latitude",
                "longitude",
                "parameter",
                "sensor_count",
                "forecast_ts",
                "horizon_hour",
                F.greatest(F.col("prediction"), F.lit(0.0)).alias("predicted_value"),
            )
        )
    result = forecasts[0]
    for frame in forecasts[1:]:
        result = result.unionByName(frame)
    return result


def write_outputs(forecast, json_path: str, parquet_path: str) -> None:
    forecast.write.mode("overwrite").parquet(parquet_path)

    rows = forecast.collect()
    by_cell_hour: dict[tuple[str, float, float, str, int], dict] = {}
    for row in rows:
        key = (
            row.model,
            round(row.latitude, 5),
            round(row.longitude, 5),
            row.forecast_ts.isoformat(),
            int(row.horizon_hour),
        )
        item = by_cell_hour.setdefault(
            key,
            {
                "model": row.model,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "forecast_ts": row.forecast_ts.isoformat(),
                "horizon_hour": int(row.horizon_hour),
                "sensor_count": int(row.sensor_count),
                "values": {},
            },
        )
        item["values"][row.parameter] = round(float(row.predicted_value), 2)

    payload = []
    for item in by_cell_hour.values():
        scores = {param: pollutant_aqi(param, value) for param, value in item["values"].items()}
        aqi = max(score for score in scores.values() if score is not None) if scores else None
        item["aqi"] = aqi
        item["category"] = aqi_category(aqi)
        payload.append(item)

    payload.sort(key=lambda x: (x["model"], x["horizon_hour"], -1 if x["aqi"] is None else -x["aqi"]))
    if not json_path.startswith("hdfs://"):
        Path(json_path).parent.mkdir(parents=True, exist_ok=True)
        Path(json_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    settings.ensure_local_dirs()
    spark = build_spark()
    measurements_path = settings.storage_path(settings.measurements_path)
    model_path = settings.storage_path(settings.models_path)
    predictions_parquet = settings.storage_path(settings.predictions_parquet_path)

    hourly = prepare_hourly(measurements_path).cache()
    features = feature_frame(hourly).cache()
    models = train_models(features, model_path)
    forecast = build_forecast(hourly, models)
    write_outputs(forecast, settings.predictions_path, predictions_parquet)
    print(f"Wrote 24h forecast to {settings.predictions_path} and {predictions_parquet}")
    spark.stop()


if __name__ == "__main__":
    main()
