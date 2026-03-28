from flask import Blueprint, jsonify, render_template, request
from webapp.db import get_db, col_expr, validate_ident

bp = Blueprint("analysis", __name__)


@bp.route("/analysis")
def analysis_page():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.id, sf.file_stem, sf.recode_type,
                   c.name, sp.code, sw.year_label
            FROM catalog.survey_file sf
            JOIN catalog.survey_wave sw    ON sw.id = sf.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sf.row_count IS NOT NULL
            ORDER BY c.name, sw.year_label, sf.recode_type
        """)
        files = [
            {"id": r[0], "stem": r[1], "recode": r[2],
             "country": r[3], "program": r[4], "year": r[5]}
            for r in cur.fetchall()
        ]
    return render_template("analysis.html", files=files)


@bp.route("/api/file/<int:file_id>/variables")
def api_file_variables(file_id):
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT var_name, var_label FROM microdata.variable_dictionary
            WHERE survey_file_id = %s ORDER BY col_position
        """, (file_id,))
        return jsonify(variables=[
            {"name": r[0], "label": r[1] or ""} for r in cur.fetchall()
        ])


@bp.route("/api/analysis/frequency")
def api_frequency():
    file_id = request.args.get("file_id", type=int)
    var_name = request.args.get("var", "").strip()
    apply_labels = request.args.get("labels", "true").lower() != "false"
    if not file_id or not var_name:
        return jsonify(error="file_id and var required"), 400

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
            SELECT in_overflow FROM microdata.variable_dictionary
            WHERE survey_file_id = %s AND var_name = %s
        """, (file_id, var_name))
        vr = cur.fetchone()
        if not vr:
            return jsonify(error="Variable not found"), 404

        expr, base, where = col_expr(table_name, var_name, vr[0], recode, wave_id)
        where_clause = f"WHERE {where}" if where else ""

        cur.execute(f"""
            SELECT {expr}::text AS val, count(*) AS n
            FROM {base} {where_clause}
            GROUP BY val ORDER BY n DESC LIMIT 50
        """)
        freq = cur.fetchall()

        val_labels = {}
        if apply_labels:
            cur.execute("""
                SELECT code, label FROM microdata.value_labels
                WHERE survey_file_id = %s AND var_name = %s
            """, (file_id, var_name))
            val_labels = dict(cur.fetchall())

        total = sum(r[1] for r in freq)
        result = []
        for val, cnt in freq:
            display = val
            if val and apply_labels and val in val_labels:
                display = val_labels[val]
            result.append({
                "value": val, "display": display or "(missing)",
                "count": cnt,
                "pct": round(cnt / total * 100, 1) if total else 0,
            })

    return jsonify(frequency=result, total=total, variable=var_name)


@bp.route("/api/analysis/crosstab")
def api_crosstab():
    file_id = request.args.get("file_id", type=int)
    row_var = request.args.get("row_var", "").strip()
    col_var = request.args.get("col_var", "").strip()
    apply_labels = request.args.get("labels", "true").lower() != "false"
    if not file_id or not row_var or not col_var:
        return jsonify(error="file_id, row_var, col_var required"), 400

    try:
        validate_ident(row_var)
        validate_ident(col_var)
    except ValueError:
        return jsonify(error="Invalid variable name"), 400

    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.db_table_name, sf.recode_type, sf.survey_wave_id
            FROM catalog.survey_file sf WHERE sf.id = %s
        """, (file_id,))
        fi = cur.fetchone()
        if not fi:
            return jsonify(error="File not found"), 404
        table_name, recode, wave_id = fi

        cur.execute("""
            SELECT var_name, in_overflow FROM microdata.variable_dictionary
            WHERE survey_file_id = %s AND var_name IN (%s, %s)
        """, (file_id, row_var, col_var))
        overflow_map = dict(cur.fetchall())
        if row_var not in overflow_map or col_var not in overflow_map:
            return jsonify(error="Variable not found"), 404

        r_expr, base, where = col_expr(
            table_name, row_var, overflow_map[row_var], recode, wave_id)
        c_expr, _, _ = col_expr(
            table_name, col_var, overflow_map[col_var], recode, wave_id)
        where_clause = f"WHERE {where}" if where else ""

        cur.execute(f"""
            SELECT {r_expr}::text AS rval, {c_expr}::text AS cval, count(*) AS n
            FROM {base} {where_clause}
            GROUP BY rval, cval ORDER BY n DESC LIMIT 2000
        """)
        raw = cur.fetchall()

        val_labels = {}
        if apply_labels:
            cur.execute("""
                SELECT var_name, code, label FROM microdata.value_labels
                WHERE survey_file_id = %s AND var_name IN (%s, %s)
            """, (file_id, row_var, col_var))
            for vn, code, label in cur.fetchall():
                val_labels.setdefault(vn, {})[code] = label

        def lbl(var, val):
            if val is None:
                return "(missing)"
            if apply_labels and var in val_labels and val in val_labels[var]:
                return val_labels[var][val]
            return val

        row_vals = sorted(set(r[0] for r in raw), key=lambda x: (x is None, x))
        col_vals = sorted(set(r[1] for r in raw), key=lambda x: (x is None, x))

        counts = {(rv, cv): n for rv, cv, n in raw}

        grid = []
        for rv in row_vals:
            cells = [counts.get((rv, cv), 0) for cv in col_vals]
            grid.append({"label": lbl(row_var, rv), "raw": rv,
                         "cells": cells, "total": sum(cells)})

        col_totals = [
            sum(counts.get((rv, cv), 0) for rv in row_vals)
            for cv in col_vals
        ]

    return jsonify(
        row_var=row_var, col_var=col_var,
        col_labels=[lbl(col_var, cv) for cv in col_vals],
        rows=grid, col_totals=col_totals,
        grand_total=sum(col_totals),
    )
