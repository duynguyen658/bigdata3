from __future__ import annotations

import argparse
import math
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.config import settings
from src.io import write_measurements_parquet


DISTRICTS = [
    ("Quan 1", 10.7769, 106.7009),
    ("Quan 3", 10.7840, 106.6840),
    ("Binh Thanh", 10.8030, 106.7070),
    ("Thu Duc", 10.8490, 106.7710),
    ("Tan Binh", 10.8015, 106.6520),
    ("Binh Chanh", 10.6950, 106.5760),
    ("Nha Be", 10.6956, 106.7403),
    ("Cu Chi", 10.9733, 106.4933),
]


def sample_sensor(sensor_index: int) -> tuple[str, float, float]:
    district, center_lat, center_lon = random.choice(DISTRICTS)
    lat = center_lat + random.uniform(-0.035, 0.035)
    lon = center_lon + random.uniform(-0.045, 0.045)
    return f"{district} sensor {sensor_index:04d}", lat, lon


def build_rows(sensor_count: int, days: int) -> list[dict]:
    random.seed(42)
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    hours = int((end - start).total_seconds() // 3600)
    sensors = [sample_sensor(i) for i in range(sensor_count)]
    rows: list[dict] = []

    for sensor_id, (name, lat, lon) in enumerate(sensors, start=1):
        local_bias = random.uniform(-8, 20)
        traffic_bias = 8 if "Quan 1" in name or "Binh Thanh" in name or "Tan Binh" in name else 0
        for h in range(hours):
            ts = start + timedelta(hours=h)
            hour = ts.hour
            rush = 1.0 if hour in {7, 8, 17, 18, 19} else 0.0
            night_inversion = 1.0 if hour in {22, 23, 0, 1, 2, 3, 4, 5} else 0.0
            weekly = 3.5 * math.sin(2 * math.pi * ts.weekday() / 7)
            weather_noise = random.gauss(0, 4)
            pm25 = max(3, 20 + local_bias + traffic_bias + 14 * rush + 8 * night_inversion + weekly + weather_noise)
            pm10 = max(8, pm25 * random.uniform(1.35, 1.9) + random.gauss(3, 6))

            for parameter, value in [("pm25", pm25), ("pm10", pm10)]:
                rows.append(
                    {
                        "sensor_id": sensor_id,
                        "location_id": sensor_id,
                        "location_name": name,
                        "datetime_utc": ts.isoformat(),
                        "datetime_local": (ts + timedelta(hours=7)).isoformat(),
                        "latitude": round(lat, 6),
                        "longitude": round(lon, 6),
                        "parameter": parameter,
                        "unit": "ug/m3",
                        "value": round(value, 2),
                        "source": "synthetic-hcmc",
                    }
                )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic HCMC AQI sensor data.")
    parser.add_argument("--sensors", type=int, default=1200)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--overwrite", action="store_true", help="Replace existing measurement dataset instead of append+dedup.")
    args = parser.parse_args()

    settings.ensure_local_dirs()
    df = pd.DataFrame(build_rows(args.sensors, args.days))
    path = settings.storage_path(settings.measurements_path)
    total_rows = write_measurements_parquet(df, path, mode="overwrite" if args.overwrite else "append")
    print(f"Wrote {len(df):,} rows to {path}; dataset now has {total_rows:,} deduplicated rows")


if __name__ == "__main__":
    main()
