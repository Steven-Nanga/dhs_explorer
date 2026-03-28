"""Data API: export (CSV/XLSX/JSON), preview, labels, variable stats."""

import csv as csvmod
import io

from flask import Blueprint, Response, jsonify, request
from webapp.db import (
    get_db, safe_val, code_str, col_expr, build_data_query,
    label_columns_and_rows, validate_ident, validate_table,
)
from webapp.helpers import NUMERIC_PG_TYPES

bp = Blueprint("data_api", __name__)


# ── Value labels ──────────────────────────────────────────────────────

@bp.route("/api/file/<int:file_id>/labels/<var_name>")
def api_labels(file_id, var_name):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT code, label FROM microdata.value_labels
            WHERE survey_file_id = %s AND var_name = %s ORDER BY code
        """, (file_id, var_name))
        rows = [{"code": r[0], "label": r[1]} for r in cur.fetchall()]
    return jsonify(labels=rows)


# ── Data preview ──────────────────────────────────────────────────────

@bp.route("/api/file/<int:file_id>/preview")
def api_preview(file_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT db_table_name, recode_type, survey_wave_id
            FROM catalog.survey_file WHERE id = %s
        """, (file_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(error="Not found"), 404
        table_name, recode, wave_id = row

        if table_name and table_name != "microdata.observation":
            try:
                validate_table(table_name)
            except ValueError:
                return jsonify(error="Bad table name"), 400
            short = table_name.split(".", 1)[1]
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'microdata' AND table_name = %s
                  AND substring(column_name,1,1) != '_'
                ORDER BY ordinal_position LIMIT 25
            """, (short,))
            var_names = [r[0] for r in cur.fetchall()]
            if not var_names:
                return jsonify(columns=[], rows=[])
            col_sql = ", ".join(f'"{c}"' for c in var_names)
            cur.execute(f"SELECT {col_sql} FROM {table_name} LIMIT 50")
            raw_rows = [list(r) for r in cur.fetchall()]
        else:
            cur.execute("""
                SELECT payload FROM microdata.observation
                WHERE survey_wave_id = %s AND recode_type = %s LIMIT 1
            """, (wave_id, recode))
            first = cur.fetchone()
            if not first:
                return jsonify(columns=[], rows=[])
            var_names = list(first[0].keys())[:25]
            extracts = ", ".join(
                f"payload->>'{validate_ident(k)}' AS \"{k}\"" for k in var_names
            )
            cur.execute(f"""
                SELECT {extracts} FROM microdata.observation
                WHERE survey_wave_id = %s AND recode_type = %s LIMIT 50
            """, (wave_id, recode))
            raw_rows = [list(r) for r in cur.fetchall()]

        columns, clean_rows = label_columns_and_rows(
            cur, file_id, var_names, raw_rows,
        )

    return jsonify(columns=columns, rows=clean_rows)


# ── Variable stats ────────────────────────────────────────────────────

@bp.route("/api/file/<int:file_id>/stats/<var_name>")
def api_var_stats(file_id, var_name):
    try:
        validate_ident(var_name)
    except ValueError:
        return jsonify(error="Invalid variable name"), 400

    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.db_table_name, sf.recode_type, sf.survey_wave_id, sf.row_count
            FROM catalog.survey_file sf WHERE sf.id = %s
        """, (file_id,))
        fi = cur.fetchone()
        if not fi:
            return jsonify(error="File not found"), 404
        table_name, recode, wave_id, total_rows = fi

        cur.execute("""
            SELECT var_label, pg_type, in_overflow
            FROM microdata.variable_dictionary
            WHERE survey_file_id = %s AND var_name = %s
        """, (file_id, var_name))
        vi = cur.fetchone()
        if not vi:
            return jsonify(error="Variable not found"), 404
        var_label, pg_type, in_overflow = vi

        is_numeric = (
            pg_type and pg_type.upper().split("(")[0].strip() in NUMERIC_PG_TYPES
            and not in_overflow
        )

        expr, base, where = col_expr(
            table_name, var_name, in_overflow, recode, wave_id)
        wc = f"WHERE {where}" if where else ""

        stats = {"var_name": var_name, "var_label": var_label, "pg_type": pg_type}

        if is_numeric:
            cur.execute(f"""
                SELECT count({expr}),
                       count(*) - count({expr}),
                       min({expr}), max({expr}),
                       avg({expr}), stddev({expr}),
                       percentile_cont(0.25) WITHIN GROUP (ORDER BY {expr}),
                       percentile_cont(0.50) WITHIN GROUP (ORDER BY {expr}),
                       percentile_cont(0.75) WITHIN GROUP (ORDER BY {expr})
                FROM {base} {wc}
            """)
            r = cur.fetchone()
            stats["type"] = "numeric"
            stats["count"] = r[0]
            stats["missing"] = r[1]
            stats["missing_pct"] = (
                round(r[1] / (r[0] + r[1]) * 100, 1) if (r[0] + r[1]) > 0 else 0
            )
            stats["min"] = float(r[2]) if r[2] is not None else None
            stats["max"] = float(r[3]) if r[3] is not None else None
            stats["mean"] = round(float(r[4]), 4) if r[4] is not None else None
            stats["std"] = round(float(r[5]), 4) if r[5] is not None else None
            stats["p25"] = float(r[6]) if r[6] is not None else None
            stats["p50"] = float(r[7]) if r[7] is not None else None
            stats["p75"] = float(r[8]) if r[8] is not None else None
        else:
            cur.execute(f"""
                SELECT count({expr}), count(*) - count({expr})
                FROM {base} {wc}
            """)
            r = cur.fetchone()
            stats["type"] = "categorical"
            stats["count"] = r[0]
            stats["missing"] = r[1]
            stats["missing_pct"] = (
                round(r[1] / (r[0] + r[1]) * 100, 1) if (r[0] + r[1]) > 0 else 0
            )

        cur.execute(f"""
            SELECT {expr}::text AS val, count(*) AS n
            FROM {base} {wc}
            GROUP BY val ORDER BY n DESC LIMIT 25
        """)
        freq = cur.fetchall()

        cur.execute("""
            SELECT code, label FROM microdata.value_labels
            WHERE survey_file_id = %s AND var_name = %s
        """, (file_id, var_name))
        val_labels = dict(cur.fetchall())

        total_for_pct = sum(r[1] for r in freq)
        stats["frequency"] = [
            {
                "value": r[0],
                "label": val_labels.get(r[0], "") if r[0] else "",
                "count": r[1],
                "pct": round(r[1] / total_for_pct * 100, 1) if total_for_pct else 0,
            }
            for r in freq
        ]

    return jsonify(stats)


# ── Data export / download ────────────────────────────────────────────

@bp.route("/api/file/<int:file_id>/data")
def api_data(file_id):
    db = get_db()
    fmt = request.args.get("format", "csv").lower()
    columns_param = request.args.get("columns", "")
    limit = request.args.get("limit", type=int)
    offset = request.args.get("offset", 0, type=int)
    apply_labels = request.args.get("labels", "true").lower() != "false"
    label_headers = request.args.get("header_labels", "true").lower() != "false"

    with db.cursor() as cur:
        cur.execute("""
            SELECT db_table_name, recode_type, survey_wave_id, file_stem
            FROM catalog.survey_file WHERE id = %s
        """, (file_id,))
        row = cur.fetchone()
        if not row:
            return jsonify(error="File not found"), 404
        table_name, recode, wave_id, stem = row

        if table_name and table_name != "microdata.observation":
            try:
                validate_table(table_name)
            except ValueError:
                return jsonify(error="Bad table"), 400

        cur.execute("""
            SELECT var_name, var_label
            FROM microdata.variable_dictionary
            WHERE survey_file_id = %s ORDER BY col_position
        """, (file_id,))
        all_vars = cur.fetchall()
        var_label_map = {r[0]: r[1] for r in all_vars}
        available = [r[0] for r in all_vars]

        if columns_param:
            requested = [c.strip() for c in columns_param.split(",") if c.strip()]
            selected = [c for c in requested if c in var_label_map]
        else:
            selected = available[:500]

        if not selected:
            return jsonify(error="No valid columns specified"), 400

        sql, params = build_data_query(
            cur, file_id, selected, table_name, recode, wave_id,
        )
        if limit:
            sql += f" LIMIT {int(limit)}"
        if offset:
            sql += f" OFFSET {int(offset)}"
        cur.execute(sql, params)
        raw_rows = cur.fetchall()

        val_map: dict = {}
        if apply_labels:
            cur.execute("""
                SELECT var_name, code, label
                FROM microdata.value_labels
                WHERE survey_file_id = %s AND var_name = ANY(%s)
            """, (file_id, selected))
            for vname, code, lbl in cur.fetchall():
                val_map.setdefault(vname, {})[code] = lbl

        rows = []
        for raw in raw_rows:
            out = []
            for i, var in enumerate(selected):
                cell = raw[i]
                safe = safe_val(cell)
                if apply_labels and var in val_map:
                    key = code_str(cell)
                    if key is not None and key in val_map[var]:
                        safe = val_map[var][key]
                out.append(safe)
            rows.append(out)

        if label_headers:
            headers = [var_label_map.get(c) or c for c in selected]
        else:
            headers = list(selected)

    filename = stem.lower()

    if fmt == "json":
        data_out = [
            {selected[i]: row[i] for i in range(len(selected))}
            for row in rows
        ]
        return jsonify(
            data=data_out,
            meta={"file": stem, "columns": selected,
                  "labels_applied": apply_labels, "count": len(rows)},
        )

    if fmt == "xlsx":
        import pandas as pd
        df = pd.DataFrame(rows, columns=headers)
        buf = io.BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        buf.seek(0)
        return Response(
            buf.getvalue(),
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}.xlsx"},
        )

    def _generate_csv():
        buf = io.StringIO()
        w = csvmod.writer(buf)
        w.writerow(headers)
        yield buf.getvalue()
        for row in rows:
            buf.seek(0)
            buf.truncate(0)
            w.writerow(row)
            yield buf.getvalue()

    return Response(
        _generate_csv(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}.csv"},
    )
