from pathlib import Path


STATIC_DIR = Path(__file__).resolve().parents[1] / "app" / "static"


def test_frontend_runtime_assets_are_local():
    index = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    app_js = (STATIC_DIR / "app.js").read_text(encoding="utf-8")

    assert "https://unpkg.com" not in index
    assert "https://fonts.googleapis.com" not in index
    assert "https://fonts.gstatic.com" not in index
    assert "https://{s}.tile.openstreetmap.org" in app_js
    assert "Local AQI grid" in app_js
    assert "tileerror" in app_js

    assert "/static/vendor/leaflet.css" in index
    assert "/static/vendor/leaflet.js" in index
    assert "/static/vendor/leaflet-heat.js" in index
    assert (STATIC_DIR / "vendor" / "leaflet.css").exists()
    assert (STATIC_DIR / "vendor" / "leaflet.js").exists()
    assert (STATIC_DIR / "vendor" / "leaflet-heat.js").exists()
