# DHS Data Explorer

A PostgreSQL-backed system for loading, exploring, and analysing Demographic and Health Survey (DHS) microdata. Includes a Python ETL pipeline, a Flask web interface with multi-user authentication, and a REST API.

## Features

- **ETL Loader** — Reads DHS `.zip` archives containing Stata `.DTA` files, extracts metadata (variable dictionaries, value labels), and bulk-loads data into PostgreSQL.
- **Web Dashboard** — Summary statistics, interactive treemap of data volume by survey, recent import history.
- **Variable Search** — Find variables by name or label across all loaded datasets.
- **Analysis** — Frequency tables and cross-tabulations with automatic value label decoding.
- **Wave Comparison** — Side-by-side variable comparison across different survey years.
- **Data Export & API** — Download subsets in CSV, Excel, or JSON. Column selection, value label application, and row limits supported. Full REST API with token auth.
- **Data Management** — Delete individual files or entire survey waves through the UI.
- **Multi-User Auth** — Role-based access control (admin/viewer), email + password login, magic login links, self-service access requests with auto-approval and email notifications.
- **User Management** — Admin panel to approve/reject requests, generate login links, set passwords, disable/enable accounts.
- **Responsive Design** — Fully responsive layout across desktop, tablet, and mobile devices with offcanvas sidebar, adaptive tables, and scrollable tabs.
- **Email Notifications** — SMTP integration (Gmail-compatible) for sending magic login links and access request notifications.

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 14+

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Create the Database

```bash
createdb dhs
```

### 3. Configure

Set environment variables (or create a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(none)* | Full connection string (overrides DHS_DB_* vars) |
| `DHS_DB_HOST` | `localhost` | PostgreSQL host |
| `DHS_DB_PORT` | `5432` | PostgreSQL port |
| `DHS_DB_NAME` | `dhs` | Database name |
| `DHS_DB_USER` | `postgres` | Database user |
| `DHS_DB_PASSWORD` | *(none)* | Database password |
| `DHS_DATA_DIR` | `./data` | Directory containing DHS `.zip` files |
| `DHS_SECRET` | `dhs-dev-key...` | Flask session secret key |
| `DHS_PASSWORD` | `admin` | Admin login password |
| `DHS_ADMIN_EMAIL` | *(none)* | Admin email address (created on first run) |
| `SMTP_HOST` | `smtp.gmail.com` | SMTP server host |
| `SMTP_PORT` | `587` | SMTP server port |
| `SMTP_USER` | *(none)* | SMTP username (e.g. Gmail address) |
| `SMTP_PASSWORD` | *(none)* | SMTP password or app password |
| `SMTP_FROM` | *(same as SMTP_USER)* | Sender email address |
| `PUBLIC_BASE_URL` | *(auto from request)* | Public site URL for links in emails (set on Render, e.g. `https://your-app.onrender.com`) |

### 4. Load Data (CLI)

Place DHS `.zip` files in the `data/` directory, then:

```bash
python -m loader all
```

This runs migrations and loads all discovered files.

**Resume one survey (e.g. finish Malawi 2024 DHS after a failed run):** set `DATABASE_URL` to your cloud database, then:

```bash
python -m loader load --only-year 2024 --only-program DHS
```

Already-loaded files are skipped; only missing recodes are ingested.

**Storage (important):** Neon’s free tier is about **512 MB** total. A full Malawi **2024 DHS** (all recodes, wide tables + overflow) often needs **more than that** by itself. If you see `DiskFull` / `project size limit` in the loader log, either delete other surveys in the app (and run `VACUUM` on the server if you use Postgres directly), **upgrade Neon** for more space, or keep the full dataset on a **local Postgres** instance only.

### 5. Start the Web Interface

```bash
python run_web.py
```

# DHS Data Explorer

A focused toolkit for loading, cleaning, exploring, and exporting Demographic and Health Survey (DHS) microdata. The project combines a robust ETL loader with a Flask web UI and a small REST API to make DHS microdata usable for analysis.

**What's in this README**
- Problem: common DHS data cleaning challenges
- Solution: what this project provides
- Quick start: run locally or in Docker
- Demo & screenshots: how to link or add images

## Problem — why DHS data cleaning is hard

- DHS microdata comes as many recode files (Stata `.dta`) packaged in ZIPs with inconsistent naming and variable conventions across countries and years.
- Variables are frequently renamed, recoded, or moved between recodes (e.g. HR/IR/PR), so matching the same concept across waves is non-trivial.
- Value labels are stored separately from numeric values and must be applied to produce human-readable outputs.
- Large surveys produce wide tables that can exceed free cloud-tier storage limits; loading must be resumable and idempotent.
- Analysts need consistent variable dictionaries, searchable metadata, and repeatable exports to avoid manual, error-prone cleaning.

## Solution — what this project offers

- Automated discovery and extraction of DHS ZIP recode files with metadata parsing (variable dictionaries and value labels).
- A catalog layer to register surveys, files, and import batches so ingestion is auditable and resumable.
- Bulk-loading logic that creates dynamic wide tables for core recodes and stores auxiliary recodes as JSONB when appropriate.
- Value-label decoding during export and analysis so frequency tables and cross-tabs show readable labels.
- A searchable variable dictionary, wave-comparison tools, and lightweight analysis endpoints to reduce manual cleaning work.

## Key features

- Loader: CLI to discover and ingest DHS ZIP files
- Web UI: dashboard, variable search, wave comparison, upload/delete
- API: export CSV/Excel/JSON with optional label application
- Auth: admin/viewer roles, magic links, password login
- Deployable to Docker, Render + Neon (Postgres)

## Quick start (local)

### Prerequisites

- Python 3.11+
- PostgreSQL 14+

### Install

```bash
pip install -r requirements.txt
```

### Configure

Set environment variables (or use a `.env`): `DATABASE_URL` or the `DHS_DB_*` vars, `DHS_SECRET`, `DHS_ADMIN_EMAIL`, `DHS_PASSWORD`. See `run_web.py` and `loader/config.py` for details.

### Initialize DB and load data

Create the database and run the loader to discover and import files placed in `data/`:

```bash
createdb dhs
python -m loader all
```

To load a specific survey or resume a failed run:

```bash
python -m loader load --only-year 2024 --only-program DHS
```

### Run the web app

```bash
python run_web.py
```

Open http://localhost:5000 and sign in with the admin email/password set earlier.

## Docker

Start locally with Docker Compose:

```bash
docker compose up --build
```

Place ZIPs in `./data/` and use the Upload page or CLI.

## Demo and screenshots

- Live demo: replace the placeholder below with your deployed URL (e.g. Render):

- Live Demo: https://your-app.onrender.com

- Screenshots: add images to `webapp/static/screenshots/` (create the folder if needed) and reference them here. Example markdown to embed an image:

```markdown
![Dashboard screenshot](webapp/static/screenshots/dashboard.png)
```

If you don't yet have screenshots, the `webapp/static/screenshots/` folder is a good place to add them; the README will show them once committed.

## Project structure (high level)

- `loader/` — ETL pipeline and CLI
- `webapp/` — Flask app, blueprints, templates, static
- `migrations/` — SQL migration scripts
- `tests/` — unit and integration tests

## Deploy (Render + Neon)

This project includes a `render.yaml` blueprint. Typical steps:

1. Create a Neon Postgres database and copy the connection string.
2. Push this repo to GitHub and connect Render to the repo.
3. Set `DATABASE_URL`, `DHS_SECRET`, `DHS_ADMIN_EMAIL`, `DHS_PASSWORD`, and optional SMTP vars in the Render service settings.

## Contributing

Contributions welcome. Please open issues for bugs or feature requests and submit pull requests against `main`.

## License

See the `LICENSE` file at the repository root.
