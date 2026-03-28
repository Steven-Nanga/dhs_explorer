"""CLI entry point for the DHS data loader.

Usage
-----
    python -m loader migrate          # apply SQL migrations
    python -m loader load             # discover zips and load everything
    python -m loader status           # show what has been loaded
    python -m loader all              # migrate then load
"""

import argparse
import logging
import os
import sys

import psycopg2

from . import catalog, config, discover, ingest

logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        sslmode=getattr(config, "DB_SSLMODE", "prefer"),
    )


# ── Commands ─────────────────────────────────────────────────────────

def run_migrations(conn) -> None:
    mdir = config.MIGRATIONS_DIR
    files = sorted(f for f in os.listdir(mdir) if f.endswith(".sql"))
    with conn.cursor() as cur:
        for fname in files:
            path = os.path.join(mdir, fname)
            logger.info("Running migration: %s", fname)
            with open(path, "r", encoding="utf-8") as fh:
                cur.execute(fh.read())
    conn.commit()
    logger.info("Migrations complete (%d file(s))", len(files))


def load_all(conn, data_dir: str = None) -> None:
    data_dir = data_dir or str(config.DATA_DIR)
    bundles = discover.discover_zips(data_dir)

    if not bundles:
        logger.warning("No zip files found in %s", data_dir)
        return

    total_files = 0
    total_rows = 0

    for bundle in bundles:
        logger.info(
            "── %s  %s %s %s ──",
            bundle.zip_name, bundle.country_code,
            bundle.year_label, bundle.program_code,
        )

        country_name = config.COUNTRY_NAMES.get(
            bundle.country_code, bundle.country_code,
        )
        country_id = catalog.get_or_create_country(
            conn, bundle.country_code, country_name,
        )
        program_id = catalog.get_or_create_program(conn, bundle.program_code)

        phase = bundle.data_files[0].dhs_phase if bundle.data_files else None
        wave_id = catalog.get_or_create_wave(
            conn, country_id, program_id, bundle.year_label, phase,
        )

        batch_id = catalog.create_batch(conn, wave_id)
        bundle_files = 0
        bundle_rows = 0
        errors: list = []

        for fi in bundle.data_files:
            if catalog.is_file_loaded(conn, wave_id, fi.file_stem):
                logger.info("  Skipping (already loaded): %s", fi.file_stem)
                continue

            logger.info("  Loading %s (%s) …", fi.file_stem, fi.recode_type)
            try:
                file_id = catalog.register_file(
                    conn, wave_id, fi.recode_type,
                    fi.file_stem, fi.file_format, bundle.zip_name,
                )
                rows = ingest.ingest_file(
                    conn, fi, wave_id, batch_id, file_id,
                    bundle.year_label, bundle.program_code,
                )
                bundle_files += 1
                bundle_rows += rows
                logger.info("    → %d rows", rows)
            except Exception:
                logger.exception("    ERROR on %s", fi.file_stem)
                errors.append(fi.file_stem)
                conn.rollback()

        status = "completed" if not errors else "completed_with_errors"
        catalog.finish_batch(
            conn, batch_id, bundle_files, bundle_rows,
            status, "; ".join(errors) if errors else None,
        )
        total_files += bundle_files
        total_rows += bundle_rows

    logger.info("Done — %d file(s), %d row(s) loaded", total_files, total_rows)


def show_status(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.name, sp.code, sw.year_label, sw.dhs_phase,
                   COUNT(sf.id)        AS files,
                   SUM(sf.row_count)   AS total_rows,
                   SUM(sf.column_count) AS total_cols
            FROM catalog.survey_wave sw
            JOIN catalog.country c        ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY c.name, sp.code, sw.year_label, sw.dhs_phase
            ORDER BY c.name, sw.year_label
        """)
        rows = cur.fetchall()

    if not rows:
        print("\nNo surveys loaded yet.\n")
        return

    hdr = f"{'Country':<20} {'Prog':<6} {'Year':<10} {'Phase':<7} {'Files':>6} {'Rows':>12}"
    print(f"\n{hdr}")
    print("-" * len(hdr))
    for name, prog, year, phase, files, total_rows, _ in rows:
        print(
            f"{name:<20} {prog:<6} {year:<10} {phase or '':<7} "
            f"{files or 0:>6} {total_rows or 0:>12,}"
        )

    # Per‑file detail
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.iso2, sp.code, sw.year_label,
                   sf.recode_type, sf.file_stem,
                   sf.row_count, sf.column_count, sf.db_table_name
            FROM catalog.survey_file sf
            JOIN catalog.survey_wave sw   ON sw.id = sf.survey_wave_id
            JOIN catalog.country c        ON c.id  = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            ORDER BY c.iso2, sw.year_label, sf.recode_type
        """)
        detail = cur.fetchall()

    if detail:
        print(f"\n{'ISO':<5} {'Prog':<6} {'Year':<10} {'Recode':<8} "
              f"{'Stem':<14} {'Rows':>10} {'Cols':>6} {'Table'}")
        print("-" * 90)
        for iso2, prog, year, recode, stem, rc, cc, tbl in detail:
            print(
                f"{iso2:<5} {prog:<6} {year:<10} {recode:<8} {stem:<14} "
                f"{rc or 0:>10,} {cc or 0:>6} {tbl or ''}"
            )
    print()


# ── CLI ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="DHS Data Loader")
    parser.add_argument(
        "command", choices=["migrate", "load", "status", "all"],
        help="migrate | load | status | all (migrate+load)",
    )
    parser.add_argument("--data-dir", help="Override data directory")
    parser.add_argument("--db-name", help="Override database name")
    parser.add_argument("--db-user", help="Override database user")
    parser.add_argument("--db-password", help="Override database password")
    parser.add_argument("--db-host", help="Override database host")
    parser.add_argument("--db-port", type=int, help="Override database port")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.db_name:
        config.DB_NAME = args.db_name
    if args.db_user:
        config.DB_USER = args.db_user
    if args.db_password:
        config.DB_PASSWORD = args.db_password
    if args.db_host:
        config.DB_HOST = args.db_host
    if args.db_port:
        config.DB_PORT = args.db_port

    conn = _connect()
    try:
        if args.command in ("migrate", "all"):
            run_migrations(conn)
        if args.command in ("load", "all"):
            load_all(conn, args.data_dir)
        if args.command == "status":
            show_status(conn)
    finally:
        conn.close()
