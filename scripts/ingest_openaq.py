from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config import settings
from src.io import write_measurements_parquet
from src.openaq_client import OpenAQClient


def coordinates_from_location(location: dict[str, Any]) -> tuple[float | None, float | None]:
    coords = location.get("coordinates") or {}
    return coords.get("latitude"), coords.get("longitude")


def sensors_from_location(location: dict[str, Any], wanted_parameters: set[str]) -> list[dict[str, Any]]:
    sensors = []
    for sensor in location.get("sensors", []) or []:
        parameter = sensor.get("parameter") or {}
        name = str(parameter.get("name") or parameter.get("displayName") or "").lower().replace(".", "")
        if name in wanted_parameters:
            sensors.append(sensor)
    return sensors


def flatten_measurement(measurement: dict[str, Any], sensor: dict[str, Any], location: dict[str, Any]) -> dict[str, Any]:
    parameter = measurement.get("parameter") or sensor.get("parameter") or {}
    period = measurement.get("period") or {}
    datetime_obj = period.get("datetimeFrom") or measurement.get("datetime") or {}
    lat, lon = coordinates_from_location(location)
    return {
        "sensor_id": sensor.get("id") or measurement.get("sensorsId"),
        "location_id": location.get("id") or measurement.get("locationsId"),
        "location_name": location.get("name"),
        "datetime_utc": datetime_obj.get("utc"),
        "datetime_local": datetime_obj.get("local"),
        "latitude": lat,
        "longitude": lon,
        "parameter": str(parameter.get("name") or parameter.get("displayName") or "").lower().replace(".", ""),
        "unit": parameter.get("units"),
        "value": measurement.get("value"),
        "source": "openaq",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest OpenAQ measurements for Ho Chi Minh City.")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--limit-locations", type=int, default=80)
    parser.add_argument("--max-pages-per-sensor", type=int, default=20)
    args = parser.parse_args()

    settings.ensure_local_dirs()
    client = OpenAQClient(settings.openaq_api_key, settings.openaq_base_url)
    parameter_ids = client.parameter_ids(settings.hcmc_bbox)
    wanted = {"pm25", "pm10"}
    locations = client.locations(settings.hcmc_bbox, list(parameter_ids.values()), args.limit_locations)

    now = datetime.now(timezone.utc)
    since = now - timedelta(days=args.days)
    rows: list[dict[str, Any]] = []

    for location in locations:
        sensors = sensors_from_location(location, wanted)
        for sensor in sensors:
            measurements = client.sensor_measurements(
                int(sensor["id"]),
                datetime_from=since,
                datetime_to=now,
                max_pages=args.max_pages_per_sensor,
            )
            for measurement in measurements:
                rows.append(flatten_measurement(measurement, sensor, location))

    if not rows:
        raise SystemExit(
            "No OpenAQ PM2.5/PM10 rows found for TP.HCM bbox. "
            "Try a larger bbox or run scripts/generate_sample_data.py for demo data."
        )

    df = pd.DataFrame(rows)
    df = df.dropna(subset=["datetime_utc", "latitude", "longitude", "parameter", "value"])
    path = settings.storage_path(settings.measurements_path)
    write_measurements_parquet(df, path)
    print(f"Wrote {len(df):,} OpenAQ rows from {len(locations)} locations to {path}")


if __name__ == "__main__":
    main()
