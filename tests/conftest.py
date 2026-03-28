"""Shared pytest fixtures for the DHS test suite."""

import pytest
from webapp.app import create_app


@pytest.fixture()
def app():
    app = create_app()
    app.config["TESTING"] = True
    yield app


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def authed_client(client):
    """A test client that is already authenticated."""
    client.post("/login", data={"password": "admin"})
    return client
