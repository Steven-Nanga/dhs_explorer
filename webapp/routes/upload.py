import os
from flask import Blueprint, abort, jsonify, render_template, request, session
from werkzeug.utils import secure_filename

from loader import config
from webapp.jobs import jobs, start_job

bp = Blueprint("upload", __name__)


def _require_admin():
    if session.get("role") != "admin":
        abort(403)


@bp.route("/upload")
def upload_page():
    _require_admin()
    return render_template("upload.html", jobs=jobs)


@bp.route("/api/upload", methods=["POST"])
def api_upload():
    _require_admin()
    if "file" not in request.files:
        return jsonify(error="No file provided"), 400
    f = request.files["file"]
    if not f.filename or not f.filename.lower().endswith(".zip"):
        return jsonify(error="Please upload a .zip file"), 400

    filename = secure_filename(f.filename)
    f.save(os.path.join(str(config.DATA_DIR), filename))

    job_id = start_job(filename)
    return jsonify(job_id=job_id)


@bp.route("/api/job/<job_id>")
def api_job(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify(error="Not found"), 404
    return jsonify(job)
