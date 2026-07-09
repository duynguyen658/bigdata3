from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd
from fastapi.testclient import TestClient

from app import main as app_main
from src.io import write_measurements_parquet


def _settings(measurements_path, predictions_path, metrics_path, storage_path=None):
    return SimpleNamespace(
        measurements_path=str(measurements_path),
        predictions_path=str(predictions_path),
        metrics_path=str(metrics_path),
        storage_path=storage_path or (lambda value: value),
    )


def _measurement(parameter: str, value: float, ts: datetime, sensor_id: int = 1) -> dict:
    return {
        "sensor_id": sensor_id,
        "location_id": sensor_id,
        "location_name": f"sensor-{sensor_id}",
        "datetime_utc": ts.isoformat(),
        "datetime_local": (ts + timedelta(hours=7)).isoformat(),
        "latitude": 10.7769,
        "longitude": 106.7009,
        "parameter": parameter,
        "unit": "ug/m3",
        "value": value,
        "source": "test",
    }


def test_current_api_returns_latest_grid_observations_with_freshness(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    measurements_path = tmp_path / "measurements"
    write_measurements_parquet(
        pd.DataFrame(
            [
                _measurement("pm25", 80.0, now - timedelta(hours=4)),
                _measurement("pm25", 20.0, now - timedelta(hours=1)),
                _measurement("pm10", 40.0, now - timedelta(hours=1), sensor_id=2),
            ]
        ),
        str(measurements_path),
        mode="overwrite",
    )
    monkeypatch.setattr(
        app_main,
        "settings",
        _settings(measurements_path, tmp_path / "forecast.json", tmp_path / "metrics.json"),
    )

    response = TestClient(app_main.app).get("/api/current")
    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "current"
    assert payload["count"] == 1

    point = payload["points"][0]
    assert point["values"]["pm25"] == 20.0
    assert point["values"]["pm10"] == 40.0
    assert point["freshness_status"] == "fresh"
    assert point["observation_age_hours"] <= 2.0
    assert point["aqi"] is not None


def test_forecast_hotspots_metrics_and_health_apis(tmp_path, monkeypatch):
    forecast_path = tmp_path / "forecast.json"
    metrics_path = tmp_path / "metrics.json"
    measurements_path = tmp_path / "missing-measurements"
    forecast_path.write_text(
        json.dumps(
            [
                {
                    "model": "random_forest",
                    "latitude": 10.77,
                    "longitude": 106.70,
                    "forecast_origin_ts": "2026-07-08T00:00:00+00:00",
                    "target_ts": "2026-07-08T01:00:00+00:00",
                    "forecast_ts": "2026-07-08T01:00:00+00:00",
                    "horizon_hour": 1,
                    "sensor_count": 2,
                    "values": {"pm25": 30.0},
                    "aqi": 89,
                    "category": "Moderate",
                },
                {
                    "model": "gbt",
                    "latitude": 10.78,
                    "longitude": 106.71,
                    "forecast_origin_ts": "2026-07-08T00:00:00+00:00",
                    "target_ts": "2026-07-08T01:00:00+00:00",
                    "forecast_ts": "2026-07-08T01:00:00+00:00",
                    "horizon_hour": 1,
                    "sensor_count": 2,
                    "values": {"pm25": 42.0},
                    "aqi": 117,
                    "category": "Unhealthy for Sensitive Groups",
                },
            ]
        ),
        encoding="utf-8",
    )
    metrics_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-07-08T00:00:00+00:00",
                "metrics": [
                    {
                        "model": "gbt",
                        "parameter": "pm25",
                        "horizon_hour": 1,
                        "split": "test",
                        "sample_count": 12,
                        "mae": 1.1,
                        "rmse": 1.4,
                        "r2": 0.7,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(app_main, "settings", _settings(measurements_path, forecast_path, metrics_path))
    client = TestClient(app_main.app)

    forecast = client.get("/api/forecast?horizon=1&model=gbt").json()
    assert forecast["mode"] == "forecast"
    assert forecast["count"] == 1
    assert forecast["points"][0]["model"] == "gbt"
    assert forecast["target_as_of"] == "2026-07-08T01:00:00+00:00"

    hotspots = client.get("/api/hotspots?horizon=1&model=gbt&limit=1").json()
    assert hotspots["mode"] == "forecast"
    assert hotspots["hotspots"][0]["aqi"] == 117

    metrics = client.get("/api/metrics?model=gbt&parameter=PM2.5&split=test").json()
    assert metrics["available"] is True
    assert metrics["count"] == 1
    assert metrics["metrics"][0]["rmse"] == 1.4

    health = client.get("/api/health").json()
    assert health["status"] == "degraded"
    assert health["artifacts"]["forecast"]["exists"] is True
    assert health["artifacts"]["measurements"]["exists"] is False


def test_current_and_health_support_hdfs_measurements(monkeypatch, tmp_path):
    hdfs_base = "hdfs://localhost:9000/aqi-hcmc"
    measurement_rel = "data/parquet/measurements"
    measurement_path = f"{hdfs_base}/{measurement_rel}"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    forecast_path = tmp_path / "forecast.json"
    metrics_path = tmp_path / "metrics.json"
    forecast_path.write_text("[]", encoding="utf-8")
    metrics_path.write_text('{"metrics": []}', encoding="utf-8")

    monkeypatch.setattr(
        app_main,
        "settings",
        _settings(
            measurement_rel,
            forecast_path,
            metrics_path,
            storage_path=lambda value: f"{hdfs_base}/{value.strip('/')}",
        ),
    )
    monkeypatch.setattr(
        app_main,
        "_read_measurements_frame",
        lambda path: pd.DataFrame(
            [
                _measurement("pm25", 18.0, now - timedelta(hours=1)),
                _measurement("pm10", 38.0, now - timedelta(hours=1), sensor_id=2),
            ]
        )
        if path == measurement_path
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        app_main,
        "_hdfs_artifact_status",
        lambda path: {
            "path": path,
            "exists": path == measurement_path,
            "check_supported": True,
            "updated_at": "2026-07-09T00:00:00+00:00",
        },
    )

    client = TestClient(app_main.app)
    current = client.get("/api/current").json()
    assert current["artifact"]["path"] == measurement_path
    assert current["artifact"]["exists"] is True
    assert current["count"] == 1
    assert current["points"][0]["values"] == {"pm25": 18.0, "pm10": 38.0}

    health = client.get("/api/health").json()
    assert health["status"] == "ok"
    assert health["artifacts"]["measurements"]["check_supported"] is True
    assert health["artifacts"]["measurements"]["exists"] is True
