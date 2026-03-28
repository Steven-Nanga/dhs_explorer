from flask import Blueprint, render_template
from webapp.db import get_db

bp = Blueprint("dashboard", __name__)


@bp.route("/")
def dashboard():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT count(DISTINCT country_id) FROM catalog.survey_wave")
        n_countries = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM catalog.survey_wave")
        n_waves = cur.fetchone()[0]

        cur.execute(
            "SELECT count(*) FROM catalog.survey_file WHERE row_count IS NOT NULL"
        )
        n_files = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(row_count),0) FROM catalog.survey_file")
        n_rows = cur.fetchone()[0]

        cur.execute("""
            SELECT c.name || ' ' || sp.code || ' ' || sw.year_label,
                   COALESCE(SUM(sf.row_count), 0)
            FROM catalog.survey_wave sw
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY c.name, sp.code, sw.year_label, sw.id
            ORDER BY sw.id
        """)
        chart = cur.fetchall()

        cur.execute("""
            SELECT ib.id, c.name, sp.code, sw.year_label,
                   ib.status, ib.files_processed, ib.rows_loaded,
                   ib.started_at, ib.finished_at
            FROM catalog.import_batch ib
            JOIN catalog.survey_wave sw    ON sw.id = ib.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            ORDER BY ib.started_at DESC LIMIT 10
        """)
        batches = cur.fetchall()

    return render_template(
        "dashboard.html",
        n_countries=n_countries, n_waves=n_waves,
        n_files=n_files, n_rows=n_rows,
        chart_labels=[r[0] for r in chart],
        chart_values=[int(r[1]) for r in chart],
        batches=batches,
    )
