"""Read DHS data files and bulk‑load them into PostgreSQL.

Core recodes (HR, IR, PR, KR, BR, MR) → auto‑generated wide typed tables.
Everything else → generic ``microdata.observation`` as JSONB.
"""

import csv
import io
import json
import math
import os
import re
import tempfile
import zipfile
import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyreadstat
from psycopg2.extras import execute_values

from . import catalog
from .config import MAX_TYPED_COLUMNS, CORE_RECODE_TYPES
from .discover import DataFileInfo

logger = logging.getLogger(__name__)

# ── Stata → PostgreSQL type mapping ───────────────────────────────────

_STATA_TO_PG = {
    "byte":   "SMALLINT",
    "int":    "SMALLINT",
    "long":   "INTEGER",
    "float":  "REAL",
    "double": "DOUBLE PRECISION",
}


def _stata_type_to_pg(stata_type: str) -> str:
    if stata_type in _STATA_TO_PG:
        return _STATA_TO_PG[stata_type]
    if stata_type.startswith("str"):
        return "TEXT"
    return "TEXT"


def _sanitize_col(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    if not name or name[0].isdigit():
        name = "_" + name
    return name


def _make_table_name(country: str, program: str, year: str, recode: str) -> str:
    year_clean = re.sub(r"[^a-z0-9]", "_", year.lower())
    return f"microdata.{country.lower()}_{program.lower()}_{year_clean}_{recode.lower()}"


# ── File extraction helpers ──────────────────────────────────────────

def _extract(zip_path: str, entry_path: str, dest: str) -> str:
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extract(entry_path, dest)
    return os.path.join(dest, entry_path)


def _read_dta(path: str) -> Tuple[pd.DataFrame, object]:
    return pyreadstat.read_dta(path)


def _parse_dct(dct_path: str) -> List[Tuple[str, str, int, int]]:
    """Return [(stata_type, col_name, start_1based, end_1based), …]."""
    specs: list = []
    with open(dct_path, "r") as fh:
        for line in fh:
            m = re.match(r"\s+(\w+)\s+(\w+)\s+\d+:\s+(\d+)-(\d+)", line)
            if m:
                dtype, name, s, e = m.groups()
                specs.append((dtype, name.lower(), int(s), int(e)))
    return specs


def _read_dat(dat_path: str, dct_path: str) -> Tuple[pd.DataFrame, Dict[str, str]]:
    specs = _parse_dct(dct_path)
    if not specs:
        raise ValueError(f"Empty DCT: {dct_path}")
    colspecs = [(s - 1, e) for _, _, s, e in specs]
    names = [name for _, name, _, _ in specs]
    type_map = {name: dtype for dtype, name, _, _ in specs}
    df = pd.read_fwf(dat_path, colspecs=colspecs, header=None, names=names)
    return df, type_map


# ── Type‑map builders ────────────────────────────────────────────────

def _build_type_map(
    columns: List[str],
    meta=None,
    fallback_types: Optional[Dict[str, str]] = None,
    df: Optional[pd.DataFrame] = None,
) -> Dict[str, str]:
    tm: Dict[str, str] = {}
    if meta and hasattr(meta, "original_variable_types") and meta.original_variable_types:
        for col in columns:
            tm[col] = _stata_type_to_pg(meta.original_variable_types.get(col, "double"))
    elif fallback_types:
        for col in columns:
            tm[col] = _stata_type_to_pg(fallback_types.get(col, "double"))
    elif df is not None:
        for col in columns:
            dt = str(df[col].dtype)
            if "int" in dt:
                tm[col] = "INTEGER"
            elif "float" in dt:
                tm[col] = "DOUBLE PRECISION"
            else:
                tm[col] = "TEXT"
    return tm


# ── DDL generation ───────────────────────────────────────────────────

def _generate_ddl(
    table_name: str,
    typed_cols: List[str],
    type_map: Dict[str, str],
    has_overflow: bool,
) -> str:
    lines = [f"CREATE TABLE {table_name} ("]
    lines.append("    _row_id         BIGSERIAL PRIMARY KEY,")
    lines.append("    _survey_wave_id INTEGER NOT NULL,")
    lines.append("    _import_batch_id INTEGER,")

    last_idx = len(typed_cols) - 1
    for i, col in enumerate(typed_cols):
        pg_type = type_map.get(col, "TEXT")
        safe = _sanitize_col(col)
        trailing = "," if i < last_idx or has_overflow else ""
        lines.append(f'    "{safe}" {pg_type}{trailing}')

    if has_overflow:
        lines.append("    _overflow JSONB")

    lines.append(");")
    return "\n".join(lines)


def _create_indexes(conn, table_name: str, typed_cols: List[str]) -> None:
    short = table_name.replace("microdata.", "")
    safe_cols = [_sanitize_col(c) for c in typed_cols]
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS idx_{short}_wave '
            f'ON {table_name} (_survey_wave_id)'
        )
        for key in ("caseid", "hhid", "hv001", "hv002", "v001", "v002"):
            if key in safe_cols:
                cur.execute(
                    f'CREATE INDEX IF NOT EXISTS idx_{short}_{key} '
                    f'ON {table_name} ("{key}")'
                )
    conn.commit()


# ── Value helpers for CSV / JSONB serialisation ──────────────────────

def _is_null(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return False


def _native(val):
    """Convert numpy scalar → Python native (for JSON)."""
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        f = float(val)
        return None if math.isnan(f) else f
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    return val


# ── Wide‑table loader ────────────────────────────────────────────────

def _load_wide(
    conn, df: pd.DataFrame, table_name: str,
    type_map: Dict[str, str], wave_id: int, batch_id: int,
) -> int:
    columns = list(df.columns)
    has_overflow = len(columns) > MAX_TYPED_COLUMNS

    if has_overflow:
        typed_cols = columns[:MAX_TYPED_COLUMNS]
        overflow_cols = columns[MAX_TYPED_COLUMNS:]
        logger.info(
            "%s: %d typed + %d overflow columns",
            table_name, len(typed_cols), len(overflow_cols),
        )
    else:
        typed_cols = columns
        overflow_cols = []

    ddl = _generate_ddl(table_name, typed_cols, type_map, has_overflow)
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        cur.execute(ddl)
    conn.commit()

    if df.empty:
        return 0

    # Build column‑index lookup once
    col_idx = {c: i for i, c in enumerate(columns)}
    int_types = {"SMALLINT", "INTEGER", "BIGINT"}
    values = df.values

    buf = io.StringIO()
    writer = csv.writer(buf)

    for row in values:
        csv_row: list = [wave_id, batch_id]

        for col in typed_cols:
            val = row[col_idx[col]]
            if _is_null(val):
                csv_row.append(r"\N")
            elif isinstance(val, float) and type_map.get(col) in int_types:
                csv_row.append(int(val) if val == int(val) else val)
            elif isinstance(val, bytes):
                csv_row.append(val.decode("utf-8", errors="replace"))
            else:
                csv_row.append(val)

        if overflow_cols:
            overflow = {}
            for col in overflow_cols:
                val = row[col_idx[col]]
                if not _is_null(val):
                    overflow[col] = _native(val)
            csv_row.append(json.dumps(overflow) if overflow else r"\N")

        writer.writerow(csv_row)

    buf.seek(0)

    col_list = ['"_survey_wave_id"', '"_import_batch_id"']
    col_list.extend(f'"{_sanitize_col(c)}"' for c in typed_cols)
    if overflow_cols:
        col_list.append('"_overflow"')
    copy_sql = (
        f"COPY {table_name} ({', '.join(col_list)}) "
        r"FROM STDIN WITH (FORMAT csv, NULL '\N')"
    )

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

    _create_indexes(conn, table_name, typed_cols)
    return len(df)


# ── JSONB observation loader ─────────────────────────────────────────

def _load_observation(
    conn, df: pd.DataFrame,
    wave_id: int, recode_type: str, batch_id: int,
) -> int:
    if df.empty:
        return 0

    columns = list(df.columns)
    col_idx = {c: i for i, c in enumerate(columns)}
    caseid_col = next((c for c in ("caseid", "hhid", "whhid") if c in col_idx), None)

    buf = io.StringIO()
    writer = csv.writer(buf)
    values = df.values

    for row_num, row in enumerate(values, start=1):
        payload = {}
        for col in columns:
            val = row[col_idx[col]]
            if not _is_null(val):
                payload[col] = _native(val)

        caseid_val = r"\N"
        if caseid_col is not None:
            raw = row[col_idx[caseid_col]]
            if not _is_null(raw):
                caseid_val = str(raw).strip()

        writer.writerow([
            wave_id, recode_type, caseid_val,
            row_num, batch_id, json.dumps(payload),
        ])

    buf.seek(0)
    copy_sql = (
        "COPY microdata.observation "
        "(survey_wave_id, recode_type, caseid, source_row, "
        "import_batch_id, payload) "
        r"FROM STDIN WITH (FORMAT csv, NULL '\N')"
    )
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()
    return len(df)


# ── Metadata persistence ─────────────────────────────────────────────

def _save_variable_dict(
    conn, file_id: int, columns: List[str],
    type_map: Dict[str, str], meta=None,
) -> None:
    labels: dict = {}
    orig_types: dict = {}

    if meta:
        if hasattr(meta, "column_names_to_labels") and meta.column_names_to_labels:
            labels = meta.column_names_to_labels
        if hasattr(meta, "original_variable_types") and meta.original_variable_types:
            orig_types = meta.original_variable_types

    rows = [
        (
            file_id, col, labels.get(col, ""),
            orig_types.get(col, ""), type_map.get(col, "TEXT"),
            pos, pos >= MAX_TYPED_COLUMNS,
        )
        for pos, col in enumerate(columns)
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO microdata.variable_dictionary "
            "(survey_file_id, var_name, var_label, stata_type, "
            "pg_type, col_position, in_overflow) VALUES %s "
            "ON CONFLICT (survey_file_id, var_name) DO UPDATE SET "
            "  var_label    = EXCLUDED.var_label, "
            "  stata_type   = EXCLUDED.stata_type, "
            "  pg_type      = EXCLUDED.pg_type, "
            "  col_position = EXCLUDED.col_position, "
            "  in_overflow  = EXCLUDED.in_overflow",
            rows, page_size=500,
        )
    conn.commit()


def _save_value_labels(conn, file_id: int, meta) -> None:
    if not hasattr(meta, "variable_value_labels") or not meta.variable_value_labels:
        return

    rows = []
    for var_name, label_dict in meta.variable_value_labels.items():
        for code, label in label_dict.items():
            code_str = (
                str(int(code))
                if isinstance(code, float) and code == int(code)
                else str(code)
            )
            rows.append((file_id, var_name, code_str, str(label)))

    if not rows:
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO microdata.value_labels "
            "(survey_file_id, var_name, code, label) VALUES %s "
            "ON CONFLICT (survey_file_id, var_name, code) "
            "DO UPDATE SET label = EXCLUDED.label",
            rows, page_size=1000,
        )
    conn.commit()


# ── Public entry point ───────────────────────────────────────────────

def ingest_file(
    conn,
    file_info: DataFileInfo,
    wave_id: int,
    batch_id: int,
    file_id: int,
    year_label: str,
    program: str,
) -> int:
    """Load one data file into the database.  Returns row count."""
    with tempfile.TemporaryDirectory() as tmp:
        data_path = _extract(file_info.zip_path, file_info.entry_path, tmp)

        if file_info.file_format == "DTA":
            df, meta = _read_dta(data_path)
            type_map = _build_type_map(list(df.columns), meta=meta)
        elif file_info.file_format == "DAT":
            if not file_info.dct_entry_path:
                logger.error("No DCT for DAT file %s — skipping", file_info.file_stem)
                return 0
            dct_path = _extract(file_info.zip_path, file_info.dct_entry_path, tmp)
            df, dat_types = _read_dat(data_path, dct_path)
            type_map = _build_type_map(list(df.columns), fallback_types=dat_types)
            meta = None
        else:
            logger.error("Unsupported format %s", file_info.file_format)
            return 0

        logger.info(
            "  Read %s: %d rows × %d cols",
            file_info.file_stem, len(df), len(df.columns),
        )

        if file_info.recode_type in CORE_RECODE_TYPES:
            table_name = _make_table_name(
                file_info.country_code, program, year_label,
                file_info.recode_type,
            )
            row_count = _load_wide(conn, df, table_name, type_map, wave_id, batch_id)
            db_table = table_name
        else:
            row_count = _load_observation(
                conn, df, wave_id, file_info.recode_type, batch_id,
            )
            db_table = "microdata.observation"

        catalog.update_file_stats(conn, file_id, len(df), len(df.columns), db_table)

        _save_variable_dict(conn, file_id, list(df.columns), type_map, meta)
        if meta:
            _save_value_labels(conn, file_id, meta)

        return row_count
