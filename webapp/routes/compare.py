from flask import Blueprint, jsonify, render_template, request
from webapp.db import get_db
from webapp.helpers import RECODE_LABELS

bp = Blueprint("compare", __name__)


@bp.route("/compare")
def compare_page():
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT sf.recode_type FROM catalog.survey_file sf
            WHERE sf.row_count IS NOT NULL ORDER BY sf.recode_type
        """)
        recode_types = [r[0] for r in cur.fetchall()]

        cur.execute("""
            SELECT sf.id, sf.recode_type, sf.file_stem,
                   c.name, sp.code, sw.year_label, sw.id AS wave_id
            FROM catalog.survey_file sf
            JOIN catalog.survey_wave sw    ON sw.id = sf.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sf.row_count IS NOT NULL
            ORDER BY c.name, sw.year_label
        """)
        files = [
            {"id": r[0], "recode": r[1], "stem": r[2],
             "country": r[3], "program": r[4], "year": r[5], "wave_id": r[6]}
            for r in cur.fetchall()
        ]
    return render_template(
        "compare.html",
        recode_types=recode_types, files=files,
        recode_labels=RECODE_LABELS,
    )


@bp.route("/api/compare")
def api_compare():
    file_ids = request.args.get("file_ids", "")
    ids = [int(x) for x in file_ids.split(",") if x.strip().isdigit()]
    if len(ids) < 2:
        return jsonify(error="Provide at least 2 file_ids"), 400

    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT sf.id, sf.file_stem, c.name, sp.code, sw.year_label
            FROM catalog.survey_file sf
            JOIN catalog.survey_wave sw    ON sw.id = sf.survey_wave_id
            JOIN catalog.country c         ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            WHERE sf.id = ANY(%s)
            ORDER BY sw.year_label
        """, (ids,))
        file_info = {
            r[0]: {"stem": r[1], "label": f"{r[2]} {r[3]} {r[4]}"}
            for r in cur.fetchall()
        }

        cur.execute("""
            SELECT survey_file_id, var_name, var_label
            FROM microdata.variable_dictionary
            WHERE survey_file_id = ANY(%s)
            ORDER BY var_name
        """, (ids,))
        var_map: dict = {}
        for fid, vname, vlabel in cur.fetchall():
            if vname not in var_map:
                var_map[vname] = {"label": vlabel or "", "files": {}}
            var_map[vname]["files"][fid] = True

    ordered_ids = [fid for fid in ids if fid in file_info]
    variables = []
    for vname in sorted(var_map.keys()):
        v = var_map[vname]
        presence = [fid in v["files"] for fid in ordered_ids]
        variables.append({
            "name": vname, "label": v["label"],
            "presence": presence, "common": all(presence),
        })

    return jsonify(
        files=[{"id": fid, **file_info[fid]} for fid in ordered_ids],
        variables=variables,
        total=len(variables),
        common=sum(1 for v in variables if v["common"]),
    )
