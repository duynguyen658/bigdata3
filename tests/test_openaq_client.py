from src.openaq_client import OpenAQClient


def test_parameter_ids_do_not_send_bbox_to_global_parameters_endpoint(monkeypatch):
    client = OpenAQClient("test-key", pause_seconds=0)
    calls = []

    def fake_paged(path, params=None, max_pages=None):
        calls.append((path, params, max_pages))
        return iter(
            [
                {"id": 1, "name": "pm10", "displayName": "PM10"},
                {"id": 2, "name": "pm25", "displayName": "PM2.5"},
            ]
        )

    monkeypatch.setattr(client, "paged", fake_paged)

    assert client.parameter_ids("106.45,10.35,107.05,11.15") == {"pm10": 1, "pm25": 2}
    assert calls[0][0] == "/parameters"
    assert "bbox" not in calls[0][1]


def test_locations_keep_bbox_filter_and_parameter_ids(monkeypatch):
    client = OpenAQClient("test-key", pause_seconds=0)
    calls = []

    def fake_paged(path, params=None, max_pages=None):
        calls.append((path, params, max_pages))
        return iter([{"id": 2446}, {"id": 2447}])

    monkeypatch.setattr(client, "paged", fake_paged)

    locations = client.locations("106.45,10.35,107.05,11.15", [1, 2], limit_locations=1)

    assert locations == [{"id": 2446}]
    assert calls[0][0] == "/locations"
    assert calls[0][1]["bbox"] == "106.45,10.35,107.05,11.15"
    assert calls[0][1]["parameters_id"] == "1,2"
