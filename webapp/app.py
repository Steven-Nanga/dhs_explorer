"""Flask application factory for the DHS Data Explorer."""

import logging
import os
import sys
from functools import wraps

from flask import Flask, jsonify, redirect, render_template, request, session, url_for

from webapp.db import close_db


def create_app():
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024
    app.secret_key = os.environ.get("DHS_SECRET", "dhs-dev-key-change-in-prod")

    auth_password = os.environ.get("DHS_PASSWORD", "admin")

    # ── Logging ───────────────────────────────────────────────────
    _setup_logging(app)

    # ── DB teardown ───────────────────────────────────────────────
    app.teardown_appcontext(close_db)

    # ── Template filters ──────────────────────────────────────────
    @app.template_filter("commas")
    def _commas(value):
        try:
            return f"{int(value):,}"
        except (ValueError, TypeError):
            return value

    # ── Authentication ────────────────────────────────────────────
    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            if request.form.get("password") == auth_password:
                session["auth"] = True
                return redirect(url_for("dashboard.dashboard"))
            error = "Invalid password."
        return render_template("login.html", error=error)

    @app.route("/logout")
    def logout():
        session.pop("auth", None)
        return redirect(url_for("login"))

    @app.before_request
    def _check_auth():
        open_endpoints = {"login", "logout", "static"}
        if request.endpoint in open_endpoints:
            return
        if request.path.startswith("/api/"):
            if not _api_auth_ok(auth_password):
                return jsonify(error="Unauthorized. Pass ?token= or X-API-Token header."), 401
            return
        if not session.get("auth"):
            return redirect(url_for("login"))

    # ── Register blueprints ───────────────────────────────────────
    from webapp.routes import register_all
    register_all(app)

    return app


def _api_auth_ok(password):
    if session.get("auth"):
        return True
    token = request.args.get("token") or request.headers.get("X-API-Token")
    return token == password


def _setup_logging(app):
    log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(log_dir, "dhs_explorer.log"), encoding="utf-8",
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
    )
    file_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(file_handler)

    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(logging.StreamHandler(sys.stderr))


# For backwards compatibility (python run_web.py)
app = create_app()
