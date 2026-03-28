"""Thin helpers that register metadata in the catalog.* schema."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_or_create_country(conn, iso2: str, name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO catalog.country (iso2, name) VALUES (%s, %s) "
            "ON CONFLICT (iso2) DO UPDATE SET name = EXCLUDED.name "
            "RETURNING id",
            (iso2, name),
        )
        conn.commit()
        return cur.fetchone()[0]


def get_or_create_program(conn, code: str) -> int:
    names = {
        "DHS": "Demographic and Health Survey",
        "MIS": "Malaria Indicator Survey",
        "AIS": "AIDS Indicator Survey",
        "SPA": "Service Provision Assessment",
        "KAP": "Knowledge, Attitudes, and Practices",
    }
    name = names.get(code, code)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO catalog.survey_program (code, name) VALUES (%s, %s) "
            "ON CONFLICT (code) DO NOTHING",
            (code, name),
        )
        conn.commit()
        cur.execute("SELECT id FROM catalog.survey_program WHERE code = %s", (code,))
        return cur.fetchone()[0]


def get_or_create_wave(
    conn, country_id: int, program_id: int,
    year_label: str, dhs_phase: Optional[str] = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO catalog.survey_wave "
            "  (country_id, program_id, year_label, dhs_phase) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (country_id, program_id, year_label) "
            "DO UPDATE SET dhs_phase = COALESCE(EXCLUDED.dhs_phase, "
            "  catalog.survey_wave.dhs_phase) "
            "RETURNING id",
            (country_id, program_id, year_label, dhs_phase),
        )
        conn.commit()
        return cur.fetchone()[0]


def register_file(
    conn, wave_id: int, recode_type: str,
    stem: str, fmt: str, zip_name: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO catalog.survey_file "
            "  (survey_wave_id, recode_type, file_stem, file_format, source_zip) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (survey_wave_id, file_stem) DO UPDATE SET "
            "  recode_type = EXCLUDED.recode_type, "
            "  file_format = EXCLUDED.file_format, "
            "  source_zip  = EXCLUDED.source_zip "
            "RETURNING id",
            (wave_id, recode_type, stem, fmt, zip_name),
        )
        conn.commit()
        return cur.fetchone()[0]


def update_file_stats(
    conn, file_id: int, row_count: int,
    col_count: int, db_table_name: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE catalog.survey_file "
            "SET row_count = %s, column_count = %s, db_table_name = %s "
            "WHERE id = %s",
            (row_count, col_count, db_table_name, file_id),
        )
        conn.commit()


def is_file_loaded(conn, wave_id: int, stem: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT row_count FROM catalog.survey_file "
            "WHERE survey_wave_id = %s AND file_stem = %s "
            "AND row_count IS NOT NULL",
            (wave_id, stem),
        )
        return cur.fetchone() is not None


def create_batch(conn, wave_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO catalog.import_batch (survey_wave_id, status) "
            "VALUES (%s, 'running') RETURNING id",
            (wave_id,),
        )
        conn.commit()
        return cur.fetchone()[0]


def finish_batch(
    conn, batch_id: int, files_processed: int,
    rows_loaded: int, status: str = "completed",
    errors: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE catalog.import_batch "
            "SET finished_at = NOW(), status = %s, "
            "    files_processed = %s, rows_loaded = %s, errors = %s "
            "WHERE id = %s",
            (status, files_processed, rows_loaded, errors, batch_id),
        )
        conn.commit()
