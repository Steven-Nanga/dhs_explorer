"""Data management: delete files and waves."""

import logging

from flask import Blueprint, jsonify
from webapp.db import get_db, validate_table

bp = Blueprint("manage", __name__)
logger = logging.getLogger(__name__)


@bp.route("/api/file/<int:file_id>/delete", methods=["POST"])
def delete_file(file_id):
    """Delete a single survey file and all its associated data."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.id, sf.file_stem, sf.db_table_name, sf.recode_type,
                   sf.survey_wave_id
            FROM catalog.survey_file sf WHERE sf.id = %s
        """, (file_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(error="File not found"), 404
        _, stem, table_name, recode, wave_id = row

        # Drop the dedicated wide table if it exists
        if table_name and table_name != "microdata.observation":
            try:
                validate_table(table_name)
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            except ValueError:
                logger.warning("Skipping drop for unsafe table name: %s", table_name)

        # Delete JSONB observation rows
        cur.execute("""
            DELETE FROM microdata.observation
            WHERE survey_wave_id = %s AND recode_type = %s
        """, (wave_id, recode))

        # Delete metadata
        cur.execute(
            "DELETE FROM microdata.value_labels WHERE survey_file_id = %s", (file_id,))
        cur.execute(
            "DELETE FROM microdata.variable_dictionary WHERE survey_file_id = %s",
            (file_id,))

        # Delete the catalog entry
        cur.execute("DELETE FROM catalog.survey_file WHERE id = %s", (file_id,))

    db.commit()
    logger.info("Deleted file %s (id=%d)", stem, file_id)
    return jsonify(ok=True, deleted=stem)


@bp.route("/api/wave/<int:wave_id>/delete", methods=["POST"])
def delete_wave(wave_id):
    """Delete a survey wave and ALL its files and data."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sw.id, c.name, sp.code, sw.year_label
            FROM catalog.survey_wave sw
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sw.id = %s
        """, (wave_id,))
        wave = cur.fetchone()
        if not wave:
            return jsonify(error="Wave not found"), 404
        label = f"{wave[1]} {wave[2]} {wave[3]}"

        # Get all files for this wave
        cur.execute("""
            SELECT id, db_table_name, recode_type
            FROM catalog.survey_file WHERE survey_wave_id = %s
        """, (wave_id,))
        files = cur.fetchall()

        for fid, table_name, recode in files:
            if table_name and table_name != "microdata.observation":
                try:
                    validate_table(table_name)
                    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                except ValueError:
                    pass

            cur.execute("""
                DELETE FROM microdata.observation
                WHERE survey_wave_id = %s AND recode_type = %s
            """, (wave_id, recode))

            cur.execute(
                "DELETE FROM microdata.value_labels WHERE survey_file_id = %s",
                (fid,))
            cur.execute(
                "DELETE FROM microdata.variable_dictionary WHERE survey_file_id = %s",
                (fid,))

        cur.execute(
            "DELETE FROM catalog.survey_file WHERE survey_wave_id = %s", (wave_id,))
        cur.execute(
            "DELETE FROM catalog.import_batch WHERE survey_wave_id = %s", (wave_id,))
        cur.execute(
            "DELETE FROM catalog.survey_wave WHERE id = %s", (wave_id,))

    db.commit()
    logger.info("Deleted wave %s (id=%d) with %d files", label, wave_id, len(files))
    return jsonify(ok=True, deleted=label, files_removed=len(files))
