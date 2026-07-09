from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.aqi import aqi_category, pollutant_aqi
from src.config import settings
from src.io import read_predictions_json


app = FastAPI(title="HCMC AQI Monitoring")
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

AVAILABLE_MODELS = {"random_forest", "gbt"}
GRID_SIZE_DEGREES = 0.02
FRESH_HOURS = 2.0
DELAYED_HOURS = 12.0


@app.get("/")
def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")


def _filter_points(data: list[dict], horizon: int, model: str) -> list[dict]:
    return [
        item
        for item in data
        if int(item.get("horizon_hour", 1)) == horizon and item.get("model", "random_forest") == model
    ]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.getActiveSession()
        or SparkSession.builder.appName("hcmc-aqi-api")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _freshness(age_hours: float | None) -> str:
    if age_hours is None:
        return "missing"
    if age_hours <= FRESH_HOURS:
        return "fresh"
    if age_hours <= DELAYED_HOURS:
        return "delayed"
    return "stale"


def _hdfs_artifact_status(path: str) -> dict:
    try:
        spark = _spark_session()
        hadoop_conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI.create(path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        hdfs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        exists = bool(fs.exists(hdfs_path))
        updated_at = None
        if exists:
            modified_ms = fs.getFileStatus(hdfs_path).getModificationTime()
            updated_at = _iso(datetime.fromtimestamp(modified_ms / 1000, tz=timezone.utc))
        return {
            "path": path,
            "exists": exists,
            "check_supported": True,
            "updated_at": updated_at,
        }
    except Exception as exc:
        return {
            "path": path,
            "exists": False,
            "check_supported": True,
            "error": str(exc),
        }


def _artifact_status(path: str) -> dict:
    if path.startswith("hdfs://"):
        return _hdfs_artifact_status(path)
    target = Path(path)
    return {
        "path": path,
        "exists": target.exists(),
        "check_supported": True,
        "updated_at": _iso(datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc)) if target.exists() else None,
    }


def _read_measurements_frame(path: str) -> pd.DataFrame:
    if path.startswith("hdfs://"):
        return _spark_session().read.parquet(path).toPandas()
    if not Path(path).exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _current_points_from_measurements(path: str) -> list[dict]:
    frame = _read_measurements_frame(path)
    if frame.empty:
        return []


    frame = frame.copy()
    frame["parameter"] = frame["parameter"].astype(str).str.lower().str.replace(".", "", regex=False)
    frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    frame["latitude"] = pd.to_numeric(frame["latitude"], errors="coerce")
    frame["longitude"] = pd.to_numeric(frame["longitude"], errors="coerce")
    frame["observation_ts"] = pd.to_datetime(frame["datetime_utc"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["value", "latitude", "longitude", "observation_ts"])
    frame = frame[frame["parameter"].isin(["pm25", "pm10"])]
    if frame.empty:
        return []

    frame["grid_lat"] = (frame["latitude"] / GRID_SIZE_DEGREES).round() * GRID_SIZE_DEGREES
    frame["grid_lon"] = (frame["longitude"] / GRID_SIZE_DEGREES).round() * GRID_SIZE_DEGREES
    latest_idx = frame.groupby(["grid_lat", "grid_lon", "parameter"])["observation_ts"].idxmax()
    latest = frame.loc[latest_idx].sort_values("observation_ts")
    now = _utc_now()
    points: list[dict] = []

    for (grid_lat, grid_lon), group in latest.groupby(["grid_lat", "grid_lon"]):
        values = {row.parameter: round(float(row.value), 2) for row in group.itertuples()}
        observation_ts = group["observation_ts"].max().to_pydatetime()
        age_hours = (now - observation_ts).total_seconds() / 3600
        scores = {param: pollutant_aqi(param, value) for param, value in values.items()}
        aqi = max((score for score in scores.values() if score is not None), default=None)
        points.append(
            {
                "grid_lat": round(float(grid_lat), 5),
                "grid_lon": round(float(grid_lon), 5),
                "latitude": round(float(group["latitude"].mean()), 6),
                "longitude": round(float(group["longitude"].mean()), 6),
                "values": values,
                "aqi": aqi,
                "category": aqi_category(aqi),
                "observation_ts": _iso(observation_ts),
                "observation_age_hours": round(age_hours, 2),
                "freshness_status": _freshness(age_hours),
                "sensor_count": int(group["sensor_id"].nunique()) if "sensor_id" in group else None,
            }
        )
    return sorted(points, key=lambda item: item.get("aqi") or 0, reverse=True)


def _forecast_payload(horizon: int, model: str) -> dict:
    model = model if model in AVAILABLE_MODELS else "random_forest"
    data = read_predictions_json(settings.predictions_path)
    points = _filter_points(data, horizon, model)
    points.sort(key=lambda item: item.get("aqi") or 0, reverse=True)
    origins = [_parse_utc(item.get("forecast_origin_ts")) for item in points]
    targets = [_parse_utc(item.get("target_ts") or item.get("forecast_ts")) for item in points]
    data_as_of = max((value for value in origins if value is not None), default=None)
    target_as_of = max((value for value in targets if value is not None), default=None)
    return {
        "city": "Ho Chi Minh City",
        "mode": "forecast",
        "horizon_hour": horizon,
        "model": model,
        "generated_at": _iso(_utc_now()),
        "data_as_of": _iso(data_as_of),
        "target_as_of": _iso(target_as_of),
        "artifact": _artifact_status(settings.predictions_path),
        "count": len(points),
        "points": points,
    }


@app.get("/api/models")
def models() -> dict:
    return {
        "default": "random_forest",
        "models": [
            {"id": "random_forest", "label": "Random Forest"},
            {"id": "gbt", "label": "GBTRegressor"},
        ],
    }


@app.get("/api/current")
def current() -> dict:
    path = settings.storage_path(settings.measurements_path)
    points = _current_points_from_measurements(path)
    observations = [_parse_utc(item.get("observation_ts")) for item in points]
    return {
        "city": "Ho Chi Minh City",
        "mode": "current",
        "generated_at": _iso(_utc_now()),
        "data_as_of": _iso(max((value for value in observations if value is not None), default=None)),
        "artifact": _artifact_status(path),
        "count": len(points),
        "points": points,
    }


@app.get("/api/forecast")
def forecast(horizon: int = Query(1, ge=1, le=24), model: str = Query("random_forest")) -> dict:
    return _forecast_payload(horizon, model)


@app.get("/api/hotspots")
def hotspots(
    horizon: int = Query(1, ge=1, le=24),
    limit: int = Query(10, ge=1, le=50),
    model: str = Query("random_forest"),
    mode: str = Query("forecast", pattern="^(current|forecast)$"),
) -> dict:
    if mode == "current":
        points = _current_points_from_measurements(settings.storage_path(settings.measurements_path))
        points.sort(key=lambda item: item.get("aqi") or 0, reverse=True)
        return {"mode": "current", "limit": limit, "count": len(points[:limit]), "hotspots": points[:limit]}

    points = _forecast_payload(horizon, model)["points"]
    points.sort(key=lambda item: item.get("aqi") or 0, reverse=True)
    return {"mode": "forecast", "horizon_hour": horizon, "model": model, "limit": limit, "count": len(points[:limit]), "hotspots": points[:limit]}


@app.get("/api/metrics")
def metrics(
    model: str | None = Query(None),
    parameter: str | None = Query(None),
    split: str | None = Query(None),
) -> dict:
    path = settings.metrics_path
    if path.startswith("hdfs://") or not Path(path).exists():
        return {"available": False, "artifact": _artifact_status(path), "metrics": []}

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("metrics", [])
    if model:
        rows = [row for row in rows if row.get("model") == model]
    if parameter:
        normalized = parameter.lower().replace(".", "")
        rows = [row for row in rows if row.get("parameter") == normalized]
    if split:
        rows = [row for row in rows if row.get("split") == split]
    return {
        "available": True,
        "generated_at": payload.get("generated_at"),
        "artifact": _artifact_status(path),
        "count": len(rows),
        "metrics": rows,
    }


@app.get("/api/health")
def health() -> dict:
    measurement_path = settings.storage_path(settings.measurements_path)
    artifacts = {
        "measurements": _artifact_status(measurement_path),
        "forecast": _artifact_status(settings.predictions_path),
        "metrics": _artifact_status(settings.metrics_path),
    }
    local_checks = [item["exists"] for item in artifacts.values() if item.get("check_supported")]
    status = "ok" if local_checks and all(local_checks) else "degraded"
    return {
        "status": status,
        "generated_at": _iso(_utc_now()),
        "artifacts": artifacts,
    }
