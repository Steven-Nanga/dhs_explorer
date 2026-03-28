"""Test authentication and access control."""


def test_unauthenticated_redirect(client):
    resp = client.get("/")
    assert resp.status_code == 302
    assert "/login" in resp.headers["Location"]


def test_login_page_loads(client):
    resp = client.get("/login")
    assert resp.status_code == 200
    assert b"password" in resp.data.lower()


def test_login_wrong_password(client):
    resp = client.post("/login", data={"password": "wrong"})
    assert resp.status_code == 200
    assert b"Invalid" in resp.data


def test_login_correct_password(client):
    resp = client.post("/login", data={"password": "admin"}, follow_redirects=False)
    assert resp.status_code == 302


def test_dashboard_after_login(authed_client):
    resp = authed_client.get("/")
    assert resp.status_code == 200
    assert b"Dashboard" in resp.data


def test_api_rejects_without_token(client):
    resp = client.get("/api/search?q=test")
    assert resp.status_code == 401


def test_api_accepts_token(client):
    resp = client.get("/api/search?q=test&token=admin")
    assert resp.status_code == 200


def test_logout(authed_client):
    resp = authed_client.get("/logout", follow_redirects=False)
    assert resp.status_code == 302
    resp2 = authed_client.get("/")
    assert resp2.status_code == 302
