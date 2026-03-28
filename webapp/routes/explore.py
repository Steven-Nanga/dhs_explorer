from flask import Blueprint, render_template
from webapp.db import get_db
from webapp.helpers import RECODE_LABELS

bp = Blueprint("explore", __name__)


@bp.route("/explore")
def explore_page():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.iso2, c.name,
                   COUNT(DISTINCT sw.id) AS waves,
                   COUNT(sf.id)          AS files,
                   COALESCE(SUM(sf.row_count), 0) AS total_rows
            FROM catalog.country c
            LEFT JOIN catalog.survey_wave sw ON sw.country_id = c.id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY c.id ORDER BY c.name
        """)
        countries = cur.fetchall()

        cur.execute("""
            SELECT sw.id, sw.country_id, sp.code, sw.year_label, sw.dhs_phase,
                   COUNT(sf.id) AS files,
                   COALESCE(SUM(sf.row_count), 0) AS rows
            FROM catalog.survey_wave sw
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY sw.id, sw.country_id, sp.code, sw.year_label, sw.dhs_phase
            ORDER BY sw.year_label
        """)
        all_waves = cur.fetchall()

    waves_by_country: dict = {}
    for w in all_waves:
        waves_by_country.setdefault(w[1], []).append(w)

    return render_template(
        "explore.html",
        countries=countries, waves_by_country=waves_by_country,
    )


@bp.route("/explore/wave/<int:wave_id>")
def wave_detail(wave_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sw.id, c.name, c.iso2, sp.code, sw.year_label, sw.dhs_phase
            FROM catalog.survey_wave sw
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sw.id = %s
        """, (wave_id,))
        wave = cur.fetchone()
        if not wave:
            return "Wave not found", 404

        cur.execute("""
            SELECT sf.id, sf.recode_type, sf.file_stem, sf.file_format,
                   sf.row_count, sf.column_count, sf.db_table_name
            FROM catalog.survey_file sf
            WHERE sf.survey_wave_id = %s
            ORDER BY sf.recode_type
        """, (wave_id,))
        files = cur.fetchall()

    return render_template(
        "wave_detail.html",
        wave=wave, files=files, recode_labels=RECODE_LABELS,
    )


@bp.route("/explore/file/<int:file_id>")
def file_detail(file_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.id, sf.recode_type, sf.file_stem, sf.file_format,
                   sf.row_count, sf.column_count, sf.db_table_name,
                   sf.survey_wave_id,
                   c.name, sp.code, sw.year_label
            FROM catalog.survey_file sf
            JOIN catalog.survey_wave sw    ON sw.id = sf.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sf.id = %s
        """, (file_id,))
        fi = cur.fetchone()
        if not fi:
            return "File not found", 404

        cur.execute("""
            SELECT var_name, var_label, stata_type, pg_type,
                   col_position, in_overflow
            FROM microdata.variable_dictionary
            WHERE survey_file_id = %s ORDER BY col_position
        """, (file_id,))
        variables = cur.fetchall()

        cur.execute("""
            SELECT var_name, count(*)
            FROM microdata.value_labels
            WHERE survey_file_id = %s GROUP BY var_name
        """, (file_id,))
        label_counts = dict(cur.fetchall())

    vars_json = [
        {"name": v[0], "label": v[1] or "", "overflow": bool(v[5])}
        for v in variables
    ]

    return render_template(
        "file_detail.html",
        fi=fi, variables=variables,
        label_counts=label_counts,
        recode_labels=RECODE_LABELS,
        vars_json=vars_json,
    )
