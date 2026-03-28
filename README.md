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

### 4. Load Data (CLI)

Place DHS `.zip` files in the `data/` directory, then:

```bash
python -m loader all
```

This runs migrations and loads all discovered files.

### 5. Start the Web Interface

```bash
python run_web.py
```

Open http://localhost:5000 and log in with the admin email and password.

## Authentication

The app supports two user roles:

| Role | Capabilities |
|---|---|
| **Admin** | Full access: upload data, delete files/waves, manage users, view all data |
| **Viewer** | Read-only: explore data, search variables, run analysis, export data |

### Login Methods

- **Email + Password** — Admin users and viewers with a set password.
- **Magic Link** — One-time login URLs sent via email, valid for 7 days.

### Access Requests

New users can request access at `/request-access`. Requests are auto-approved and a magic login link is emailed immediately. An admin notification email is also sent. Admins can later disable or delete users from the Manage Users page.

### Email Setup (Gmail)

To enable email notifications:

1. Go to your Google Account → Security → 2-Step Verification → App passwords
2. Generate an app password for "Mail"
3. Set `SMTP_USER` to your Gmail address and `SMTP_PASSWORD` to the app password

If SMTP is not configured, magic links are displayed in the admin UI for manual sharing.

## Docker (local)

```bash
docker compose up --build
```

This starts PostgreSQL and the web app. Place `.zip` files in `./data/` and use the Upload page or CLI.

## Deploy Free (Render + Neon)

### Step 1: Create a Neon database

1. Sign up at [neon.tech](https://neon.tech) (free, no credit card)
2. Create a new project, pick any region
3. Copy the **connection string** — it looks like:
   ```
   postgresql://user:pass@ep-xxx.region.aws.neon.tech/neondb?sslmode=require
   ```

### Step 2: Push to GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USER/DHS_Project.git
git push -u origin main
```

### Step 3: Deploy on Render

**Option A — One-click (uses render.yaml):**

1. Go to [render.com](https://render.com) and sign up (free)
2. Click **New** → **Blueprint**
3. Connect your GitHub repo
4. Render reads `render.yaml` and creates the web service + database automatically
5. In the service environment, set `DATABASE_URL` to your **Neon** connection string (instead of using Render's built-in DB, which is only free for 90 days)

**Option B — Manual setup:**

1. Go to Render → **New** → **Web Service**
2. Connect your GitHub repo
3. Settings:
   - **Build command:** `./build.sh`
   - **Start command:** `gunicorn webapp.app:app -b 0.0.0.0:$PORT -w 2 --timeout 120`
   - **Environment:** Python 3
4. Add environment variables:

| Variable | Value |
|---|---|
| `DATABASE_URL` | Your Neon connection string |
| `DHS_SECRET` | Any random string (e.g. `openssl rand -hex 32`) |
| `DHS_PASSWORD` | Your chosen admin password |
| `DHS_ADMIN_EMAIL` | Your admin email address |
| `SMTP_USER` | Gmail address for sending login emails |
| `SMTP_PASSWORD` | Gmail app password |

5. Click **Deploy**

### Step 4: Load data

Once deployed, open your Render URL, log in, and use the **Upload** page to upload DHS `.zip` files. The app runs migrations automatically on first deploy.

### Free tier limits

| Service | Limit | Notes |
|---|---|---|
| **Neon** | 0.5 GB storage | Enough for ~5-10 DHS survey waves |
| **Render** | 750 hrs/month | Spins down after 15 min idle, ~30s cold start |

> **Tip:** You can delete older survey waves from the admin UI to free up Neon storage before uploading new data. The `DROP TABLE` used for wide tables reclaims space immediately; row deletions are reclaimed by Neon's autovacuum.

## Project Structure

```
DHS_Project/
├── loader/              # ETL pipeline
│   ├── config.py        # Database & path configuration
│   ├── discover.py      # Zip file scanning & metadata extraction
│   ├── catalog.py       # Catalog table helpers
│   ├── ingest.py        # Data reading & bulk loading
│   └── main.py          # CLI entry point
├── webapp/              # Flask web application
│   ├── app.py           # Application factory, auth routes
│   ├── db.py            # DB helpers & SQL safety
│   ├── email.py         # SMTP email sending (magic links, notifications)
│   ├── helpers.py       # Shared constants
│   ├── jobs.py          # Background job management
│   ├── routes/          # Route blueprints
│   │   ├── dashboard.py
│   │   ├── upload.py
│   │   ├── explore.py
│   │   ├── search.py
│   │   ├── analysis.py
│   │   ├── compare.py
│   │   ├── data_api.py  # Export, stats, preview
│   │   ├── manage.py    # Delete operations
│   │   └── users.py     # User management (admin)
│   ├── templates/       # Jinja2 HTML templates
│   └── static/          # CSS
├── migrations/          # SQL schema migrations
├── tests/               # Test suite
├── Dockerfile
├── docker-compose.yml
├── render.yaml          # Render deployment blueprint
└── requirements.txt
```

## Database Schema

**Catalog layer** (`catalog` schema):
- `country` — ISO codes and names
- `survey_program` — DHS, MIS, AIS, SPA, KAP
- `survey_wave` — One per country + program + year
- `survey_file` — Individual recode files within a wave
- `import_batch` — Audit trail with logs
- `app_user` — User accounts with roles, status, login tokens, and password hashes

**Microdata layer** (`microdata` schema):
- Dynamic wide tables for core recodes (HR, IR, PR, KR, BR, MR)
- `observation` — JSONB store for auxiliary recode types
- `variable_dictionary` — Variable metadata from Stata headers
- `value_labels` — Code-to-label mappings

## API Reference

All API endpoints require authentication. Pass `?token=<password>` or set the `X-API-Token` header. Session-based auth (from the web UI) also works.

### Data Export

```
GET /api/file/<file_id>/data
```

| Parameter | Type | Description |
|---|---|---|
| `columns` | string | Comma-separated variable names (default: first 500) |
| `format` | string | `csv`, `xlsx`, or `json` (default: `csv`) |
| `labels` | bool | Apply value labels (default: `true`) |
| `header_labels` | bool | Use labels as column headers (default: `true`) |
| `limit` | int | Max rows |
| `offset` | int | Skip first N rows |

### Variable Search

```
GET /api/search?q=<query>
```

### Frequency Table

```
GET /api/analysis/frequency?file_id=<id>&var=<name>
```

### Cross-tabulation

```
GET /api/analysis/crosstab?file_id=<id>&row_var=<name>&col_var=<name>
```

### Variable Statistics

```
GET /api/file/<file_id>/stats/<var_name>
```

### Data Management (admin only)

```
POST /api/file/<file_id>/delete
POST /api/wave/<wave_id>/delete
```

### User Management (admin only)

```
POST /api/users/<id>/approve
POST /api/users/<id>/reject
POST /api/users/<id>/disable
POST /api/users/<id>/enable
POST /api/users/<id>/new-link
POST /api/users/<id>/set-password
POST /api/users/<id>/delete
```
