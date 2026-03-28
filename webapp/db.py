"""Database connection helpers and SQL safety utilities."""

import decimal
import math
import re

import psycopg2
from psycopg2 import sql as psql
from flask import g

from loader import config

_SAFE_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
_SAFE_TABLE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


def connect():
    """Open a new database connection (caller must close)."""
    return psycopg2.connect(
        host=config.DB_HOST, port=config.DB_PORT,
        dbname=config.DB_NAME, user=config.DB_USER,
        password=config.DB_PASSWORD,
        sslmode=getattr(config, "DB_SSLMODE", "prefer"),
    )


def get_db():
    """Return the per-request database connection (auto-closed)."""
    if "db" not in g:
        g.db = connect()
    return g.db


def close_db(exc=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ── SQL safety ────────────────────────────────────────────────────────

def validate_ident(name: str) -> str:
    """Raise if *name* is not a safe SQL identifier (lowercase, alphanumeric + _)."""
    if not _SAFE_IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def validate_table(name: str) -> str:
    """Raise if *name* is not a safe ``schema.table`` reference."""
    if not _SAFE_TABLE.match(name):
        raise ValueError(f"Unsafe table name: {name!r}")
    return name


def quoted_table(schema_table: str):
    """Return a psycopg2.sql Composed object for ``schema.table``."""
    validate_table(schema_table)
    schema, table = schema_table.split(".", 1)
    return psql.SQL("{}.{}").format(psql.Identifier(schema), psql.Identifier(table))


def quoted_col(name: str):
    """Return a psycopg2.sql Identifier for a column name."""
    validate_ident(name)
    return psql.Identifier(name)


def quoted_cols(names: list[str]):
    """Return comma-joined quoted column identifiers."""
    return psql.SQL(", ").join(quoted_col(n) for n in names)


# ── Value helpers ─────────────────────────────────────────────────────

def safe_val(v):
    """Normalise a DB cell value for JSON serialisation."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def code_str(val):
    """Normalise a cell value to the string key used in value_labels."""
    if val is None:
        return None
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    if isinstance(val, decimal.Decimal) and val == int(val):
        return str(int(val))
    return str(val)


# ── Query builders ────────────────────────────────────────────────────

def col_expr(table_name, var_name, in_overflow, recode=None, wave_id=None):
    """Return (sql_expression, base_table, where_fragment) for a variable.

    All identifier interpolation goes through validate_ident / validate_table
    so column / table names that don't match ``[a-z_][a-z0-9_]*`` are rejected.
    """
    validate_ident(var_name)

    if table_name and table_name != "microdata.observation":
        validate_table(table_name)
        if in_overflow:
            return f"_overflow->>'{var_name}'", table_name, ""
        return f'"{var_name}"', table_name, ""

    return (
        f"payload->>'{var_name}'",
        "microdata.observation",
        f"survey_wave_id = {int(wave_id)} AND recode_type = '{validate_ident(recode)}'",
    )


def build_data_query(cur, file_id, selected, table_name, recode, wave_id):
    """Build a SELECT for *selected* columns, handling wide-table overflow."""
    for c in selected:
        validate_ident(c)

    if table_name and table_name != "microdata.observation":
        validate_table(table_name)
        cur.execute("""
            SELECT var_name, in_overflow
            FROM microdata.variable_dictionary
            WHERE survey_file_id = %s AND var_name = ANY(%s)
        """, (file_id, selected))
        overflow = dict(cur.fetchall())

        exprs = []
        for c in selected:
            if overflow.get(c, False):
                exprs.append(f"_overflow->>'{c}' AS \"{c}\"")
            else:
                exprs.append(f'"{c}"')
        return f"SELECT {', '.join(exprs)} FROM {table_name}", []

    exprs = ", ".join(f"payload->>'{c}' AS \"{c}\"" for c in selected)
    return (
        f"SELECT {exprs} FROM microdata.observation "
        f"WHERE survey_wave_id = %s AND recode_type = %s",
        [wave_id, recode],
    )


def label_columns_and_rows(cur, file_id, var_names, raw_rows):
    """Resolve variable labels -> column headers & value labels -> cell text."""
    cur.execute("""
        SELECT var_name, var_label
        FROM microdata.variable_dictionary
        WHERE survey_file_id = %s AND var_name = ANY(%s)
    """, (file_id, var_names))
    var_labels = dict(cur.fetchall())

    cur.execute("""
        SELECT var_name, code, label
        FROM microdata.value_labels
        WHERE survey_file_id = %s AND var_name = ANY(%s)
    """, (file_id, var_names))
    val_map: dict = {}
    for vname, code, label in cur.fetchall():
        val_map.setdefault(vname, {})[code] = label

    columns = [{"var": v, "label": var_labels.get(v, "")} for v in var_names]

    clean_rows = []
    for row in raw_rows:
        out = []
        for i, v in enumerate(var_names):
            cell = row[i]
            safe = safe_val(cell)
            key = code_str(cell)
            if key is not None and v in val_map and key in val_map[v]:
                out.append(val_map[v][key])
            else:
                out.append(safe)
        clean_rows.append(out)

    return columns, clean_rows
