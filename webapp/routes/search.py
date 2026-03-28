from flask import Blueprint, jsonify, render_template, request
from webapp.db import get_db

bp = Blueprint("search", __name__)


@bp.route("/search")
def search_page():
    return render_template("search.html")


@bp.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify(results=[], query=q)

    db = get_db()
    pattern = f"%{q}%"
    with db.cursor() as cur:
        cur.execute("""
            SELECT vd.var_name, vd.var_label, vd.pg_type,
                   sf.id AS file_id, sf.file_stem, sf.recode_type,
                   c.name AS country, sp.code AS program, sw.year_label
            FROM microdata.variable_dictionary vd
            JOIN catalog.survey_file sf     ON sf.id = vd.survey_file_id
            JOIN catalog.survey_wave sw     ON sw.id = sf.survey_wave_id
            JOIN catalog.country c          ON c.id  = sw.country_id
            JOIN catalog.survey_program sp  ON sp.id = sw.program_id
            WHERE vd.var_name ILIKE %s OR vd.var_label ILIKE %s
            ORDER BY vd.var_name, sw.year_label
            LIMIT 500
        """, (pattern, pattern))
        rows = cur.fetchall()

    results = [
        {
            "var_name": r[0], "var_label": r[1], "pg_type": r[2],
            "file_id": r[3], "file_stem": r[4], "recode_type": r[5],
            "country": r[6], "program": r[7], "year": r[8],
        }
        for r in rows
    ]
    return jsonify(results=results, query=q, count=len(results))
