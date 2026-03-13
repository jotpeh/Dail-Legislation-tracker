from fastapi.testclient import TestClient

from api import app


client = TestClient(app)


def test_reports_weekly_endpoint_shapes():
    resp = client.get("/reports/weekly", params={"chamber": "Dáil", "days": 30})
    assert resp.status_code in (200, 404)
    if resp.status_code == 200:
        data = resp.json()
        assert "markdown" in data
        assert "items" in data


def test_metrics_status_endpoint_shape():
    resp = client.get("/metrics/legislation/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "by_status" in data

