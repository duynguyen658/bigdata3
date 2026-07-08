from __future__ import annotations

from pathlib import Path
import shutil

import pandas as pd


MEASUREMENT_COLUMNS = [
    "sensor_id",
    "location_id",
    "location_name",
    "datetime_utc",
    "datetime_local",
    "latitude",
    "longitude",
    "parameter",
    "unit",
    "value",
    "source",
]


def write_measurements_parquet(df: pd.DataFrame, path: str) -> None:
    frame = df.copy()
    for column in MEASUREMENT_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[MEASUREMENT_COLUMNS]
    if not path.startswith("hdfs://"):
        target = Path(path)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False, partition_cols=["parameter"])


def read_predictions_json(path: str) -> list[dict]:
    import json

    target = Path(path)
    if not target.exists():
        return []
    return json.loads(target.read_text(encoding="utf-8"))
