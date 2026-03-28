#!/usr/bin/env bash
set -o errexit

pip install --upgrade pip
pip install -r requirements.txt

# Run database migrations
python -c "
from loader.main import run_migrations, _connect
conn = _connect()
run_migrations(conn)
conn.close()
print('Migrations complete.')
"

# Ensure data directory exists
mkdir -p data
