from __future__ import annotations

import json
import math
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


LOCAL_TZ = "Asia/Ho_Chi_Minh"
FORECAST_HORIZONS = range(1, 25)
SPLIT_TRAIN_FRACTION = 0.70
SPLIT_VALIDATION_FRACTION = 0.15


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
    hourly = (
        with_grid.groupBy("grid_lat", "grid_lon", "parameter", "hour_ts")
        .agg(
            F.avg("value").alias("value"),
            F.countDistinct("sensor_id").alias("sensor_count"),
            F.avg("latitude").alias("latitude"),
            F.avg("longitude").alias("longitude"),
        )
        .where("sensor_count > 0")
    )

    # Regularize to a dense hourly calendar per (grid_lat, grid_lon, parameter) so
    # that a row-offset lag/lead in feature_frame corresponds to a true hour
    # offset. Coverage gaps become explicit null rows rather than being silently
    # skipped -- they are never forward-filled, only dropped downstream once
    # they would otherwise poison a lag/label calculation.
    bounds = hourly.groupBy("grid_lat", "grid_lon", "parameter").agg(
        F.min("hour_ts").alias("min_ts"), F.max("hour_ts").alias("max_ts")
    )
    calendar = bounds.withColumn(
        "hour_ts", F.explode(F.sequence(F.col("min_ts"), F.col("max_ts"), F.expr("INTERVAL 1 HOUR")))
    ).select("grid_lat", "grid_lon", "parameter", "hour_ts")

    dense = calendar.join(
        hourly, on=["grid_lat", "grid_lon", "parameter", "hour_ts"], how="left"
    ).withColumn("sensor_count", F.coalesce(F.col("sensor_count"), F.lit(0)))
    return dense


def feature_frame(hourly):
    """Build one stacked training row per (grid cell, parameter, origin hour, horizon).

    Each row's `label` is the true value at `horizon_hour` hours after
    `forecast_origin_ts` (via `lead(value, horizon_hour)`), so a model trained
    on this frame genuinely learns horizon-conditioned dynamics for H+1..H+24,
    instead of a single fixed lead(value, 24) label being reused for every
    displayed horizon. `hour`/`day_of_week` are computed from the *origin*
    time (in Asia/Ho_Chi_Minh local time) both here and at inference in
    `build_forecast`, so train/inference feature semantics stay identical.
    `forecast_origin_ts`/`target_ts` are kept explicit so a later chronological
    split can require `target_ts` (not just `forecast_origin_ts`) to respect
    the split boundary and avoid leaking future labels into training.
    """
    w = Window.partitionBy("grid_lat", "grid_lon", "parameter").orderBy("hour_ts")
    local_ts = F.from_utc_timestamp(F.col("hour_ts"), LOCAL_TZ)
    base = (
        hourly.withColumn("hour", F.hour(local_ts))
        .withColumn("day_of_week", F.dayofweek(local_ts))
        .withColumn("lag_1h", F.lag("value", 1).over(w))
        .withColumn("lag_3h", F.lag("value", 3).over(w))
        .withColumn("lag_24h", F.lag("value", 24).over(w))
        .withColumn("forecast_origin_ts", F.col("hour_ts"))
    )
    stacked = None
    for h in FORECAST_HORIZONS:
        frame = (
            base.withColumn("horizon_hour", F.lit(h))
            .withColumn("target_ts", F.col("forecast_origin_ts") + F.expr(f"INTERVAL {h} HOURS"))
            .withColumn("label", F.lead("value", h).over(w))
        )
        stacked = frame if stacked is None else stacked.unionByName(frame)
    return stacked.na.drop(subset=["lag_1h", "lag_3h", "lag_24h", "label"])


def split_feature_frame(features):
    """Add chronological train/validation/test split labels.

    Boundaries are computed from `target_ts`, not just origin time. That means
    a row whose future label crosses a later split boundary cannot remain in an
    earlier split, which is the leakage rule required for multi-horizon rows.
    """
    with_epoch = features.withColumn("_target_epoch", F.unix_timestamp("target_ts"))
    quantiles = with_epoch.approxQuantile(
        "_target_epoch",
        [SPLIT_TRAIN_FRACTION, SPLIT_TRAIN_FRACTION + SPLIT_VALIDATION_FRACTION],
        0.0,
    )
    if len(quantiles) != 2:
        raise RuntimeError("Could not compute chronological split boundaries.")
    validation_boundary, test_boundary = quantiles
    return (
        with_epoch.withColumn(
            "split",
            F.when(F.col("_target_epoch") < F.lit(validation_boundary), F.lit("train"))
            .when(F.col("_target_epoch") < F.lit(test_boundary), F.lit("validation"))
            .otherwise(F.lit("test")),
        )
        .drop("_target_epoch")
    )


def _has_column(frame, column: str) -> bool:
    return column in frame.columns


def _json_metric(value: float) -> float | None:
    return float(value) if math.isfinite(value) else None


def evaluate_model(model, frame, model_name: str, parameter: str, split_name: str) -> list[dict]:
    metrics: list[dict] = []
    scored = model.transform(frame)
    error = F.col("label") - F.col("prediction")
    aggregated = (
        scored.groupBy("horizon_hour")
        .agg(
            F.count("*").alias("sample_count"),
            F.avg(F.abs(error)).alias("mae"),
            F.sqrt(F.avg(F.pow(error, F.lit(2.0)))).alias("rmse"),
            F.sum(F.pow(error, F.lit(2.0))).alias("sse"),
            F.sum("label").alias("label_sum"),
            F.sum(F.pow(F.col("label"), F.lit(2.0))).alias("label_sq_sum"),
        )
        .collect()
    )
    for row in aggregated:
        sample_count = int(row["sample_count"])
        denominator = float(row["label_sq_sum"]) - (float(row["label_sum"]) ** 2 / sample_count)
        r2 = None if denominator <= 0 else 1.0 - (float(row["sse"]) / denominator)
        record = {
            "model": model_name,
            "parameter": parameter,
            "horizon_hour": int(row["horizon_hour"]),
            "split": split_name,
            "sample_count": sample_count,
            "mae": _json_metric(float(row["mae"])),
            "rmse": _json_metric(float(row["rmse"])),
            "r2": _json_metric(r2) if r2 is not None else None,
        }
        metrics.append(record)
    return sorted(metrics, key=lambda item: item["horizon_hour"])


def write_metrics(metrics: list[dict], metrics_path: str) -> None:
    from datetime import datetime, timezone

    safe_metrics = [
        {key: _json_metric(value) if isinstance(value, float) else value for key, value in record.items()}
        for record in metrics
    ]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": safe_metrics,
    }
    if not metrics_path.startswith("hdfs://"):
        Path(metrics_path).parent.mkdir(parents=True, exist_ok=True)
        Path(metrics_path).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
            encoding="utf-8",
        )


def train_models(features, model_base_path: str, metrics_path: str | None = None):
    split_features = features if _has_column(features, "split") else split_feature_frame(features)
    feature_cols = [
        "grid_lat",
        "grid_lon",
        "hour",
        "day_of_week",
        "lag_1h",
        "lag_3h",
        "lag_24h",
        "sensor_count",
        "horizon_hour",
    ]
    models = {}
    metrics: list[dict] = []
    for parameter in ["pm25", "pm10"]:
        parameter_rows = split_features.where(F.col("parameter") == parameter).cache()
        train = parameter_rows.where(F.col("split") == "train")
        if train.limit(1).count() == 0:
            parameter_rows.unpersist()
            continue
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        for model_name, estimator_builder in MODEL_BUILDERS.items():
            pipeline = Pipeline(stages=[assembler, estimator_builder()])
            model = pipeline.fit(train)
            save_path = f"{model_base_path.rstrip('/')}/{model_name}/{parameter}"
            model.write().overwrite().save(save_path)
            models[(model_name, parameter)] = model
            if metrics_path:
                for split_name in ["validation", "test"]:
                    eval_frame = parameter_rows.where(F.col("split") == split_name)
                    if eval_frame.limit(1).count() > 0:
                        metrics.extend(evaluate_model(model, eval_frame, model_name, parameter, split_name))
        parameter_rows.unpersist()
    if metrics_path:
        write_metrics(metrics, metrics_path)
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
        .withColumn("forecast_origin_ts", F.col("hour_ts"))
    )

    spark = SparkSession.getActiveSession()
    assert spark is not None
    horizons = spark.range(1, 25).withColumnRenamed("id", "horizon_hour")
    # hour/day_of_week must be derived from hour_ts (the forecast origin, in
    # local time), matching feature_frame's training semantics exactly -- NOT
    # from forecast_ts (the target time), which would mismatch what the model
    # was trained on and reintroduce the H+1..H+24 mislabeling bug.
    local_origin_ts = F.from_utc_timestamp(F.col("hour_ts"), LOCAL_TZ)
    candidate = (
        latest.crossJoin(horizons)
        .withColumn("target_ts", F.from_unixtime(F.unix_timestamp("hour_ts") + F.col("horizon_hour") * 3600).cast("timestamp"))
        .withColumn("forecast_ts", F.col("target_ts"))
        .withColumn("hour", F.hour(local_origin_ts))
        .withColumn("day_of_week", F.dayofweek(local_origin_ts))
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
                "forecast_origin_ts",
                "target_ts",
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
            row.target_ts.isoformat(),
            int(row.horizon_hour),
        )
        item = by_cell_hour.setdefault(
            key,
            {
                "model": row.model,
                "latitude": float(row.latitude),
                "longitude": float(row.longitude),
                "forecast_origin_ts": row.forecast_origin_ts.isoformat(),
                "target_ts": row.target_ts.isoformat(),
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
    split_features = split_feature_frame(features).cache()
    models = train_models(split_features, model_path, metrics_path=settings.metrics_path)
    forecast = build_forecast(hourly, models)
    write_outputs(forecast, settings.predictions_path, predictions_parquet)
    print(f"Wrote 24h forecast to {settings.predictions_path} and {predictions_parquet}")
    spark.stop()


if __name__ == "__main__":
    main()
