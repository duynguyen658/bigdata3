from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    openaq_api_key: str = _env("OPENAQ_API_KEY")
    openaq_base_url: str = _env("OPENAQ_BASE_URL", "https://api.openaq.org/v3").rstrip("/")
    hcmc_bbox: str = _env("HCMC_BBOX", "106.45,10.35,107.05,11.15")
    hdfs_base_path: str = _env("HDFS_BASE_PATH")
    measurements_path: str = _env("MEASUREMENTS_PATH", "data/parquet/measurements")
    predictions_path: str = _env("PREDICTIONS_PATH", "data/predictions/forecast_24h.json")
    predictions_parquet_path: str = _env("PREDICTIONS_PARQUET_PATH", "data/predictions/forecast_24h_parquet")
    models_path: str = _env("MODELS_PATH", "models/aqi_forecast")

    def storage_path(self, relative_or_local: str) -> str:
        if self.hdfs_base_path:
            return f"{self.hdfs_base_path.rstrip('/')}/{relative_or_local.strip('/')}"
        return relative_or_local

    def ensure_local_dirs(self) -> None:
        for value in [self.measurements_path, self.predictions_path, self.predictions_parquet_path, self.models_path]:
            if value.startswith("hdfs://"):
                continue
            path = Path(value)
            if path.suffix:
                path.parent.mkdir(parents=True, exist_ok=True)
            else:
                path.mkdir(parents=True, exist_ok=True)


settings = Settings()
