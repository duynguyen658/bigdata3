from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable

import requests


class OpenAQClient:
    def __init__(self, api_key: str, base_url: str = "https://api.openaq.org/v3", pause_seconds: float = 0.25):
        if not api_key:
            raise ValueError("OPENAQ_API_KEY is required for real OpenAQ ingestion.")
        self.base_url = base_url.rstrip("/")
        self.pause_seconds = pause_seconds
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key, "Accept": "application/json"})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        response = self.session.get(url, params=params or {}, timeout=45)
        response.raise_for_status()
        return response.json()

    def paged(self, path: str, params: dict[str, Any] | None = None, max_pages: int | None = None) -> Iterable[dict[str, Any]]:
        page = 1
        while True:
            query = dict(params or {})
            query.setdefault("limit", 100)
            query["page"] = page
            payload = self._get(path, query)
            results = payload.get("results", [])
            if not results:
                break
            yield from results
            found = payload.get("meta", {}).get("found")
            if max_pages and page >= max_pages:
                break
            if isinstance(found, int) and page * int(query["limit"]) >= found:
                break
            page += 1
            time.sleep(self.pause_seconds)

    def parameter_ids(self, bbox: str) -> dict[str, int]:
        params = {
            "bbox": bbox,
            "parameter_type": "pollutant",
            "limit": 100,
            "order_by": "id",
            "sort_order": "asc",
        }
        found: dict[str, int] = {}
        for item in self.paged("/parameters", params=params, max_pages=2):
            name = str(item.get("name", "")).lower().replace(".", "")
            display = str(item.get("displayName", "")).lower().replace(".", "")
            if name in {"pm25", "pm10"}:
                found[name] = int(item["id"])
            if display in {"pm25", "pm10"}:
                found[display] = int(item["id"])
        return found

    def locations(self, bbox: str, parameter_ids: list[int], limit_locations: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "bbox": bbox,
            "iso": "VN",
            "limit": 100,
            "order_by": "id",
            "sort_order": "asc",
        }
        if parameter_ids:
            params["parameters_id"] = ",".join(str(pid) for pid in parameter_ids)

        records = []
        for location in self.paged("/locations", params=params):
            records.append(location)
            if limit_locations and len(records) >= limit_locations:
                break
        return records

    def sensor_measurements(
        self,
        sensor_id: int,
        datetime_from: datetime,
        datetime_to: datetime,
        max_pages: int = 20,
    ) -> list[dict[str, Any]]:
        params = {
            "datetime_from": datetime_from.astimezone(timezone.utc).isoformat(),
            "datetime_to": datetime_to.astimezone(timezone.utc).isoformat(),
            "limit": 100,
        }
        return list(self.paged(f"/sensors/{sensor_id}/measurements", params=params, max_pages=max_pages))
