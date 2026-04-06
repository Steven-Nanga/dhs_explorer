"""Shared pytest fixtures for the DHS test suite."""

import os

import pytest

# Stable credentials for CI and local runs when env is unset (matches bootstrapped admin).
os.environ.setdefault("DHS_ADMIN_EMAIL", "admin@test.local")
os.environ.setdefault("DHS_PASSWORD", "admin")

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
    """A test client signed in as admin."""
    email = os.environ["DHS_ADMIN_EMAIL"]
    password = os.environ["DHS_PASSWORD"]
    client.post("/login", data={"email": email, "password": password})
    return client
