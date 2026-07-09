from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from scripts.train_forecast_spark import write_metrics
from src.io import _combine_spark_measurements_for_write, write_measurements_parquet


BASE_UTC = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _measurement(sensor_id: int, hour: int, parameter: str, value: float) -> dict:
    ts = BASE_UTC + timedelta(hours=hour)
    return {
        "sensor_id": sensor_id,
        "location_id": 100 + sensor_id,
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


def test_write_measurements_appends_deduplicates_and_partitions(tmp_path):
    path = tmp_path / "measurements"

    first = pd.DataFrame(
        [
            _measurement(1, 0, "pm25", 10.0),
            _measurement(1, 1, "pm25", 11.0),
            _measurement(2, 0, "pm10", 20.0),
        ]
    )
    assert write_measurements_parquet(first, str(path), mode="overwrite") == 3

    second = pd.DataFrame(
        [
            _measurement(1, 1, "PM2.5", 99.0),  # same stable identity, updated value
            _measurement(1, 2, "pm25", 12.0),
        ]
    )
    assert write_measurements_parquet(second, str(path), mode="append") == 4

    rows = pd.read_parquet(path)
    assert len(rows) == 4
    assert {"parameter", "date"}.issubset(rows.columns)
    assert (path / "parameter=pm25").exists()
    assert (path / "parameter=pm10").exists()

    pm25_hour_1 = rows[
        (rows["sensor_id"] == 1)
        & (rows["parameter"] == "pm25")
        & (rows["datetime_utc"].str.startswith("2024-01-01T01:00:00"))
    ]
    assert len(pm25_hour_1) == 1
    assert pm25_hour_1.iloc[0]["value"] == 99.0


def test_write_measurements_overwrite_replaces_existing_dataset(tmp_path):
    path = tmp_path / "measurements"
    assert write_measurements_parquet(pd.DataFrame([_measurement(1, 0, "pm25", 10.0)]), str(path)) == 1
    assert write_measurements_parquet(pd.DataFrame([_measurement(2, 0, "pm10", 20.0)]), str(path), mode="overwrite") == 1

    rows = pd.read_parquet(path)
    assert len(rows) == 1
    assert rows.iloc[0]["sensor_id"] == 2
    assert rows.iloc[0]["parameter"] == "pm10"


def test_spark_append_propagates_existing_dataset_read_errors(monkeypatch):
    class FailingReader:
        def parquet(self, path):
            raise RuntimeError(f"cannot read {path}")

    class FakeSpark:
        read = FailingReader()

    batch = object()
    monkeypatch.setattr("src.io._spark_path_exists", lambda spark, path: True)

    with pytest.raises(RuntimeError, match="cannot read hdfs://namenode:9000/aqi/measurements"):
        _combine_spark_measurements_for_write(
            FakeSpark(),
            batch,
            "hdfs://namenode:9000/aqi/measurements",
            mode="append",
        )


def test_write_metrics_serializes_non_finite_values_as_null(tmp_path):
    metrics_path = tmp_path / "metrics.json"
    write_metrics(
        [
            {
                "model": "random_forest",
                "parameter": "pm25",
                "horizon_hour": 1,
                "split": "validation",
                "sample_count": 1,
                "mae": 1.2,
                "rmse": math.nan,
                "r2": math.inf,
            }
        ],
        str(metrics_path),
    )

    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert payload["metrics"][0]["mae"] == 1.2
    assert payload["metrics"][0]["rmse"] is None
    assert payload["metrics"][0]["r2"] is None
