"""Test authentication and access control."""


def test_public_dashboard(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Dashboard" in resp.data


def test_login_page_loads(client):
    resp = client.get("/login")
    assert resp.status_code == 200
    assert b"password" in resp.data.lower()


def test_login_wrong_password(client):
    resp = client.post("/login", data={"password": "wrong"})
    assert resp.status_code == 200
    assert b"Invalid" in resp.data


def test_login_correct_password(client):
    import os

    email = os.environ["DHS_ADMIN_EMAIL"]
    password = os.environ["DHS_PASSWORD"]
    resp = client.post(
        "/login",
        data={"email": email, "password": password},
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_dashboard_after_login(authed_client):
    resp = authed_client.get("/")
    assert resp.status_code == 200
    assert b"Dashboard" in resp.data


def test_public_search_api(client):
    resp = client.get("/api/search?q=test")
    assert resp.status_code == 200


def test_admin_api_accepts_token(client):
    import os
    token = os.environ.get("DHS_PASSWORD", "admin")
    resp = client.get(f"/api/search?q=test&token={token}")
    assert resp.status_code == 200


def test_logout(authed_client):
    resp = authed_client.get("/logout", follow_redirects=False)
    assert resp.status_code == 302
    resp2 = authed_client.get("/")
    assert resp2.status_code == 200
