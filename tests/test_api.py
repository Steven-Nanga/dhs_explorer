"""Test API endpoints."""

import json


def test_search_api(authed_client):
    resp = authed_client.get("/api/search?q=education")
    assert resp.status_code == 200
    data = json.loads(resp.data)
    assert "results" in data
    assert "count" in data
    assert data["query"] == "education"


def test_search_too_short(authed_client):
    resp = authed_client.get("/api/search?q=a")
    data = json.loads(resp.data)
    assert data["results"] == []


def test_file_not_found(authed_client):
    resp = authed_client.get("/api/file/99999/preview")
    assert resp.status_code == 404


def test_data_export_json(authed_client):
    resp = authed_client.get("/api/search?q=caseid")
    data = json.loads(resp.data)
    if data["count"] > 0:
        fid = data["results"][0]["file_id"]
        resp2 = authed_client.get(
            f"/api/file/{fid}/data?columns=caseid&format=json&limit=2"
        )
        assert resp2.status_code == 200
        d2 = json.loads(resp2.data)
        assert "data" in d2
        assert len(d2["data"]) <= 2


def test_stats_not_found(authed_client):
    resp = authed_client.get("/api/file/99999/stats/v025")
    assert resp.status_code == 404


def test_frequency_missing_params(authed_client):
    resp = authed_client.get("/api/analysis/frequency")
    assert resp.status_code == 400
