from flask import Blueprint, jsonify, render_template, request
from webapp.db import get_db

bp = Blueprint("search", __name__)


@bp.route("/search")
def search_page():
    return render_template("search.html")


@bp.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    country = request.args.get("country", "").strip()
    recode = request.args.get("recode", "").strip()
    year = request.args.get("year", "").strip()
    if len(q) < 2:
        return jsonify(results=[], query=q)

    db = get_db()
    pattern = f"%{q}%"
    sql = """
        SELECT vd.var_name, vd.var_label, vd.pg_type,
               sf.id AS file_id, sf.file_stem, sf.recode_type,
               c.name AS country, sp.code AS program, sw.year_label
        FROM microdata.variable_dictionary vd
        JOIN catalog.survey_file sf     ON sf.id = vd.survey_file_id
        JOIN catalog.survey_wave sw     ON sw.id = sf.survey_wave_id
        JOIN catalog.country c          ON c.id  = sw.country_id
        JOIN catalog.survey_program sp  ON sp.id = sw.program_id
        WHERE (vd.var_name ILIKE %s OR vd.var_label ILIKE %s)
    """
    params = [pattern, pattern]
    if country:
        sql += " AND c.name = %s"
        params.append(country)
    if recode:
        sql += " AND sf.recode_type = %s"
        params.append(recode)
    if year:
        sql += " AND sw.year_label = %s"
        params.append(year)
    sql += " ORDER BY vd.var_name, sw.year_label LIMIT 500"

    with db.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    results = [
        {
            "var_name": r[0], "var_label": r[1], "pg_type": r[2],
            "file_id": r[3], "file_stem": r[4], "recode_type": r[5],
            "country": r[6], "program": r[7], "year": r[8],
        }
        for r in rows
    ]
    return jsonify(
        results=results,
        query=q,
        count=len(results),
        applied_filters={"country": country, "recode": recode, "year": year},
    )


@bp.route("/api/search/filters/countries")
def api_filter_countries():
    """Return all distinct countries with loaded data."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT c.name
            FROM catalog.survey_wave sw
            JOIN catalog.country c ON c.id = sw.country_id
            ORDER BY c.name
        """)
        rows = cur.fetchall()
    countries = [r[0] for r in rows]
    return jsonify(countries=countries)


@bp.route("/api/search/filters/recodes")
def api_filter_recodes():
    """Return all distinct recode types with loaded data."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT recode_type
            FROM catalog.survey_file
            ORDER BY recode_type
        """)
        rows = cur.fetchall()
    recodes = [r[0] for r in rows]
    return jsonify(recodes=recodes)


@bp.route("/api/search/filters/years")
def api_filter_years():
    """Return all distinct survey years with loaded data."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT year_label
            FROM catalog.survey_wave
            ORDER BY year_label DESC
        """)
        rows = cur.fetchall()
    years = [r[0] for r in rows]
    return jsonify(years=years)
