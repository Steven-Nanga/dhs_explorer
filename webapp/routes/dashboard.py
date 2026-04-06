from flask import Blueprint, render_template, session
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
        raw_batches = cur.fetchall()

        # Public-facing updates: successful loads with useful row counts.
        cur.execute("""
            SELECT c.name, sp.code, sw.year_label, ib.rows_loaded, ib.finished_at
            FROM catalog.import_batch ib
            JOIN catalog.survey_wave sw    ON sw.id = ib.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE ib.status = 'completed' AND COALESCE(ib.rows_loaded, 0) > 0
            ORDER BY ib.finished_at DESC NULLS LAST
            LIMIT 5
        """)
        latest_updates = cur.fetchall()

        # Coverage by country for better exploration context.
        cur.execute("""
            SELECT c.name,
                   COUNT(DISTINCT sw.id) AS wave_count,
                   COUNT(sf.id)          AS file_count,
                   COALESCE(SUM(sf.row_count), 0) AS row_count,
                   MAX(sw.year_label) AS latest_year
            FROM catalog.country c
            JOIN catalog.survey_wave sw ON sw.country_id = c.id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY c.name
            ORDER BY row_count DESC, c.name
            LIMIT 8
        """)
        coverage_rows = cur.fetchall()

    # Admin operations view: suppress obviously stale "running, zero progress" rows.
    batches = [
        b for b in raw_batches
        if b[4] != "running" or (b[5] or 0) > 0 or not b[7]
    ]
    if len(batches) > 8:
        batches = batches[:8]

    freshness_label = "No successful imports yet"
    if latest_updates and latest_updates[0][4]:
        from datetime import datetime
        now = datetime.now(latest_updates[0][4].tzinfo)
        delta = now - latest_updates[0][4]
        minutes = int(delta.total_seconds() // 60)
        if minutes < 60:
            freshness_label = f"Updated {minutes} min ago"
        elif minutes < 60 * 24:
            hours = round(minutes / 60)
            freshness_label = f"Updated {hours} hour{'s' if hours != 1 else ''} ago"
        else:
            days = round(minutes / (60 * 24))
            freshness_label = f"Updated {days} day{'s' if days != 1 else ''} ago"

    return render_template(
        "dashboard.html",
        n_countries=n_countries, n_waves=n_waves,
        n_files=n_files, n_rows=n_rows,
        chart_labels=[r[0] for r in chart],
        chart_values=[int(r[1]) for r in chart],
        latest_updates=latest_updates,
        coverage_rows=coverage_rows,
        latest_success=latest_updates[0] if latest_updates else None,
        freshness_label=freshness_label,
        is_admin=(session.get("role") == "admin"),
        batches=batches,
    )
