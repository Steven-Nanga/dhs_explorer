"""Flask application factory for the DHS Data Explorer."""

import logging
import os
import secrets
import sys
import threading
from datetime import datetime, timedelta, timezone

import bcrypt
from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix

from webapp.db import close_db
from webapp.email import is_configured as smtp_is_configured
from webapp.url_helpers import public_base_url


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
                    cur.execute("UPDATE catalog.app_user SET last_login_at = NOW() WHERE id = %s", (user[0],))
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

    @app.route("/request-access", methods=["GET", "POST"])
    def request_access():
        success = False
        error = None
        if request.method == "POST":
            email = (request.form.get("email") or "").strip().lower()
            name = (request.form.get("name") or "").strip()
            reason = (request.form.get("reason") or "").strip()

            if not email or not name:
                error = "Name and email are required."
            else:
                from webapp.db import connect
                conn = connect()
                with conn.cursor() as cur:
                    cur.execute("SELECT id, status FROM catalog.app_user WHERE LOWER(email) = %s", (email,))
                    existing = cur.fetchone()
                    if existing:
                        st = existing[1]
                        if st == "approved":
                            error = "This email already has access. Use the login page."
                        elif st == "pending":
                            error = "A request for this email is already pending."
                        elif st == "rejected":
                            error = "This email has been rejected. Contact the administrator."
                        else:
                            error = "This email is disabled. Contact the administrator."
                    else:
                        token = secrets.token_urlsafe(48)
                        expires = datetime.now(timezone.utc) + timedelta(days=7)
                        cur.execute("""
                            INSERT INTO catalog.app_user
                                (email, display_name, role, status, login_token, token_expires)
                            VALUES (%s, %s, 'viewer', 'approved', %s, %s)
                        """, (email, name, token, expires))
                        success = True
                conn.commit()
                conn.close()

                if success:
                    base = public_base_url(request)
                    link = f"{base}/magic/{token}"
                    manage_url = f"{base}/users"
                    expires_iso = expires.isoformat()

                    def _send_access_emails():
                        from webapp.email import send_magic_link, send_access_notification
                        try:
                            u_ok = send_magic_link(email, name, link, expires_iso)
                            a_ok = send_access_notification(admin_email, name, email, manage_url)
                            logging.getLogger(__name__).info(
                                "Access emails for %s: magic_link=%s admin_notify=%s",
                                email, u_ok, a_ok,
                            )
                        except Exception:
                            logging.getLogger(__name__).exception(
                                "Failed to send access emails for %s", email,
                            )

                    # Send in background so the browser gets a fast response; SMTP still runs immediately
                    threading.Thread(target=_send_access_emails, daemon=True).start()

        return render_template(
            "request_access.html",
            success=success,
            error=error,
            smtp_configured=smtp_is_configured(),
        )

    @app.route("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login"))

    # ── Auth guard ────────────────────────────────────────────────
    @app.before_request
    def _check_auth():
        open_endpoints = {"login", "logout", "request_access", "magic_login", "static"}
        if request.endpoint in open_endpoints:
            return
        if request.path.startswith("/api/"):
            if not _api_auth_ok():
                return jsonify(error="Unauthorized. Pass ?token= or X-API-Token header."), 401
            return
        if not session.get("auth"):
            return redirect(url_for("login"))

    # ── Register blueprints ───────────────────────────────────────
    from webapp.routes import register_all
    register_all(app)

    return app


def _api_auth_ok():
    """Check API auth via session or token header/param."""
    if session.get("auth"):
        return True
    from webapp.db import connect
    token = request.args.get("token") or request.headers.get("X-API-Token")
    if not token:
        return False
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, role FROM catalog.app_user
            WHERE password_hash IS NOT NULL AND status = 'approved'
        """)
        for uid, role in cur.fetchall():
            if role == "admin":
                admin_pw = os.environ.get("DHS_PASSWORD", "admin")
                if token == admin_pw:
                    conn.close()
                    return True
    conn.close()
    return False


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
