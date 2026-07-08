from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.config import settings
from src.io import read_predictions_json


app = FastAPI(title="HCMC AQI Monitoring")
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")

AVAILABLE_MODELS = {"random_forest", "gbt"}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")


def _filter_points(data: list[dict], horizon: int, model: str) -> list[dict]:
    return [
        item
        for item in data
        if int(item.get("horizon_hour", 1)) == horizon and item.get("model", "random_forest") == model
    ]


@app.get("/api/models")
def models() -> dict:
    return {
        "default": "random_forest",
        "models": [
            {"id": "random_forest", "label": "Random Forest"},
            {"id": "gbt", "label": "GBTRegressor"},
        ],
    }


@app.get("/api/forecast")
def forecast(horizon: int = Query(1, ge=1, le=24), model: str = Query("random_forest")) -> dict:
    model = model if model in AVAILABLE_MODELS else "random_forest"
    data = read_predictions_json(settings.predictions_path)
    points = _filter_points(data, horizon, model)
    points.sort(key=lambda item: item.get("aqi") or 0, reverse=True)
    return {
        "city": "Ho Chi Minh City",
        "horizon_hour": horizon,
        "model": model,
        "count": len(points),
        "points": points,
    }


@app.get("/api/hotspots")
def hotspots(
    horizon: int = Query(1, ge=1, le=24),
    limit: int = Query(10, ge=1, le=50),
    model: str = Query("random_forest"),
) -> dict:
    model = model if model in AVAILABLE_MODELS else "random_forest"
    data = read_predictions_json(settings.predictions_path)
    points = _filter_points(data, horizon, model)
    points.sort(key=lambda item: item.get("aqi") or 0, reverse=True)
    return {"horizon_hour": horizon, "model": model, "hotspots": points[:limit]}
