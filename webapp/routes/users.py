"""User management routes (admin only)."""

import logging
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
from flask import Blueprint, abort, jsonify, render_template, request, session

from webapp.db import get_db
from webapp.email import send_magic_link
from webapp.url_helpers import public_base_url

logger = logging.getLogger(__name__)

bp = Blueprint("users", __name__)


def _require_admin():
    if session.get("role") != "admin":
        abort(403)


@bp.route("/users")
def manage_users():
    _require_admin()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, email, display_name, role, status, created_at, last_login_at
            FROM catalog.app_user
            ORDER BY
                CASE status
                    WHEN 'pending' THEN 0
                    WHEN 'approved' THEN 1
                    WHEN 'rejected' THEN 2
                    WHEN 'disabled' THEN 3
                END,
                created_at DESC
        """)
        users = cur.fetchall()
    return render_template("users.html", users=users)


@bp.route("/api/users/<int:user_id>/approve", methods=["POST"])
def approve_user(user_id):
    _require_admin()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT id, email, display_name FROM catalog.app_user WHERE id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            return jsonify(error="User not found"), 404

        token = secrets.token_urlsafe(48)
        expires = datetime.now(timezone.utc) + timedelta(days=7)

        cur.execute("""
            UPDATE catalog.app_user
            SET status = 'approved', login_token = %s, token_expires = %s
            WHERE id = %s
        """, (token, expires, user_id))
    db.commit()

    base = public_base_url(request)
    link = f"{base}/magic/{token}"
    email_sent = send_magic_link(user[1], user[2] or user[1], link, expires.isoformat())

    return jsonify(ok=True, email=user[1], magic_link=link,
                   expires=expires.isoformat(), email_sent=email_sent)


@bp.route("/api/users/<int:user_id>/reject", methods=["POST"])
def reject_user(user_id):
    _require_admin()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("UPDATE catalog.app_user SET status = 'rejected' WHERE id = %s", (user_id,))
    db.commit()
    return jsonify(ok=True)


@bp.route("/api/users/<int:user_id>/disable", methods=["POST"])
def disable_user(user_id):
    _require_admin()
    if user_id == session.get("user_id"):
        return jsonify(error="Cannot disable yourself"), 400
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            UPDATE catalog.app_user
            SET status = 'disabled', login_token = NULL, token_expires = NULL
            WHERE id = %s
        """, (user_id,))
    db.commit()
    return jsonify(ok=True)


@bp.route("/api/users/<int:user_id>/enable", methods=["POST"])
def enable_user(user_id):
    _require_admin()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("UPDATE catalog.app_user SET status = 'approved' WHERE id = %s", (user_id,))
    db.commit()
    return jsonify(ok=True)


@bp.route("/api/users/<int:user_id>/new-link", methods=["POST"])
def new_magic_link(user_id):
    """Generate a fresh magic login link for an approved user."""
    _require_admin()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT id, email, display_name, status FROM catalog.app_user WHERE id = %s", (user_id,))
        user = cur.fetchone()
        if not user:
            return jsonify(error="User not found"), 404
        if user[3] != "approved":
            return jsonify(error="User must be approved first"), 400

        token = secrets.token_urlsafe(48)
        expires = datetime.now(timezone.utc) + timedelta(days=7)

        cur.execute("""
            UPDATE catalog.app_user SET login_token = %s, token_expires = %s WHERE id = %s
        """, (token, expires, user_id))
    db.commit()

    base = public_base_url(request)
    link = f"{base}/magic/{token}"
    email_sent = send_magic_link(user[1], user[2] or user[1], link, expires.isoformat())

    return jsonify(ok=True, email=user[1], magic_link=link,
                   expires=expires.isoformat(), email_sent=email_sent)


@bp.route("/api/users/<int:user_id>/set-password", methods=["POST"])
def set_password(user_id):
    """Admin sets a password for a user so they can log in with email + password."""
    _require_admin()
    data = request.get_json(silent=True) or {}
    new_pw = data.get("password", "").strip()
    if len(new_pw) < 4:
        return jsonify(error="Password must be at least 4 characters"), 400

    pw_hash = bcrypt.hashpw(new_pw.encode(), bcrypt.gensalt()).decode()
    db = get_db()
    with db.cursor() as cur:
        cur.execute("UPDATE catalog.app_user SET password_hash = %s WHERE id = %s", (pw_hash, user_id))
    db.commit()
    return jsonify(ok=True)


@bp.route("/api/users/<int:user_id>/delete", methods=["POST"])
def delete_user(user_id):
    _require_admin()
    if user_id == session.get("user_id"):
        return jsonify(error="Cannot delete yourself"), 400
    db = get_db()
    with db.cursor() as cur:
        cur.execute("DELETE FROM catalog.app_user WHERE id = %s", (user_id,))
    db.commit()
    return jsonify(ok=True)
