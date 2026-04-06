"""Test that all pages load without errors."""

import os

import pytest

PUBLIC_PAGES = ["/", "/explore", "/search", "/analysis", "/compare"]


@pytest.mark.parametrize("path", PUBLIC_PAGES)
def test_public_page_loads(client, path):
    resp = client.get(path)
    assert resp.status_code == 200


def test_upload_requires_admin_login(client):
    resp = client.get("/upload", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers.get("Location", "")


def test_upload_loads_for_admin(authed_client):
    resp = authed_client.get("/upload")
    assert resp.status_code == 200


def test_admin_api_without_auth_rejected(client):
    resp = client.post("/api/upload", data={}, follow_redirects=False)
    assert resp.status_code == 401


def test_admin_api_with_token(client):
    token = os.environ.get("DHS_PASSWORD", "admin")
    resp = client.get(f"/api/job/noop?token={token}")
    assert resp.status_code == 404
