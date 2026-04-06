"""Flask application factory for the DHS Data Explorer."""

import logging
import os
import sys
from datetime import datetime, timezone

import bcrypt
from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from webapp.db import close_db

# Pages and APIs that require an authenticated admin (session or API token).
ADMIN_ENDPOINTS = frozenset({
    "upload.upload_page",
    "upload.api_upload",
    "upload.api_job",
    "users.manage_users",
    "users.approve_user",
    "users.reject_user",
    "users.disable_user",
    "users.enable_user",
    "users.new_magic_link",
    "users.set_password",
    "users.delete_user",
    "manage.delete_file",
    "manage.delete_wave",
})


def create_app():
    app = Flask(__name__)
    # Render / nginx: trust X-Forwarded-Proto and Host so request.url_root is correct
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)
    app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024
    app.secret_key = os.environ.get("DHS_SECRET", "dhs-dev-key-change-in-prod")

    admin_email = os.environ.get("DHS_ADMIN_EMAIL", "stephennanga97@gmail.com")
    admin_password = os.environ.get("DHS_PASSWORD", "w2pldk8i")

    _setup_logging(app)
    app.teardown_appcontext(close_db)

    # ── Template filters ──────────────────────────────────────────
    @app.template_filter("commas")
    def _commas(value):
        try:
            return f"{int(value):,}"
        except (ValueError, TypeError):
            return value

    # ── Ensure admin user exists on first request ─────────────────
    @app.before_request
    def _ensure_admin():
        if getattr(app, "_admin_ensured", False):
            return
        try:
            from webapp.db import connect
            conn = connect()
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM catalog.app_user WHERE role = 'admin' LIMIT 1")
                if not cur.fetchone():
                    pw_hash = bcrypt.hashpw(admin_password.encode(), bcrypt.gensalt()).decode()
                    cur.execute("""
                        INSERT INTO catalog.app_user (email, display_name, role, status, password_hash)
                        VALUES (%s, 'Administrator', 'admin', 'approved', %s)
                    """, (admin_email, pw_hash))
            conn.commit()
            conn.close()
        except Exception:
            pass
        app._admin_ensured = True

    # ── Context processor ─────────────────────────────────────────
    @app.context_processor
    def inject_user():
        return {
            "current_user_role": session.get("role"),
            "current_user_name": session.get("user_name"),
            "current_user_email": session.get("user_email"),
        }

    # ── Auth routes ───────────────────────────────────────────────
    @app.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            email = (request.form.get("email") or "").strip().lower()
            password = request.form.get("password", "")

            from webapp.db import connect
            conn = connect()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, email, display_name, role, status, password_hash
                    FROM catalog.app_user
                    WHERE LOWER(email) = %s AND status = 'approved'
                """, (email,))
                user = cur.fetchone()

                if user and user[5] and bcrypt.checkpw(password.encode(), user[5].encode()):
                    if user[3] != "admin":
                        error = (
                            "Only administrators sign in here. "
                            "The data explorer is open to everyone without an account."
                        )
                    else:
                        cur.execute(
                            "UPDATE catalog.app_user SET last_login_at = NOW() WHERE id = %s",
                            (user[0],),
                        )
                        conn.commit()
                        conn.close()
                        session["auth"] = True
                        session["user_id"] = user[0]
                        session["user_email"] = user[1]
                        session["user_name"] = user[2]
                        session["role"] = user[3]
                        return redirect(url_for("dashboard.dashboard"))
                else:
                    error = "Invalid email or password."
            conn.close()

        return render_template("login.html", error=error)

    @app.route("/magic/<token>")
    def magic_login(token):
        """One-time magic link login for approved viewers."""
        from webapp.db import connect
        conn = connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, email, display_name, role, status, token_expires
                FROM catalog.app_user
                WHERE login_token = %s AND status = 'approved'
            """, (token,))
            user = cur.fetchone()

            if not user:
                conn.close()
                return render_template("login.html", error="Invalid or expired link. Please request a new one.")

            if user[5] and user[5] < datetime.now(timezone.utc):
                conn.close()
                return render_template("login.html", error="This link has expired. Please request a new one.")

            if user[3] != "admin":
                conn.close()
                flash(
                    "This site is open to everyone — you do not need an account to explore data. "
                    "The sign-in page is only for administrators.",
                    "info",
                )
                return redirect(url_for("dashboard.dashboard"))

            cur.execute("""
                UPDATE catalog.app_user
                SET login_token = NULL, token_expires = NULL, last_login_at = NOW()
                WHERE id = %s
            """, (user[0],))
            conn.commit()
            conn.close()

            session["auth"] = True
            session["user_id"] = user[0]
            session["user_email"] = user[1]
            session["user_name"] = user[2]
            session["role"] = user[3]
            return redirect(url_for("dashboard.dashboard"))

    @app.route("/request-access")
    def request_access():
        """Public explorer needs no account; kept as a short FAQ for anyone expecting a signup flow."""
        return render_template("request_access.html")

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("dashboard.dashboard"))

    # ── Auth guard ────────────────────────────────────────────────
    @app.before_request
    def _check_auth():
        open_endpoints = {"login", "logout", "request_access", "magic_login", "static"}
        if request.endpoint in open_endpoints:
            return
        if request.endpoint is None:
            return
        if request.endpoint in ADMIN_ENDPOINTS:
            if request.path.startswith("/api/"):
                if not _admin_api_ok():
                    return jsonify(
                        error="Unauthorized. Admin session or ?token= / X-API-Token required.",
                    ), 401
                return
            if session.get("role") != "admin":
                return redirect(url_for("login"))
            return

    # ── Register blueprints ───────────────────────────────────────
    from webapp.routes import register_all
    register_all(app)

    return app


def _admin_api_ok():
    """Admin JSON/API access: signed-in admin or shared admin password token."""
    if session.get("role") == "admin":
        return True
    token = request.args.get("token") or request.headers.get("X-API-Token")
    if not token:
        return False
    return token == os.environ.get("DHS_PASSWORD", "admin")


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


app = create_app()
