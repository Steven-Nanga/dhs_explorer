"""Test that all pages load without errors."""

import pytest

PAGES = ["/", "/upload", "/explore", "/search", "/analysis", "/compare"]


@pytest.mark.parametrize("path", PAGES)
def test_page_loads(authed_client, path):
    resp = authed_client.get(path)
    assert resp.status_code == 200
