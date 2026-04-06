#!/usr/bin/env python3
"""Test database connection directly."""

import sys
import os
sys.path.insert(0, '.')

try:
    # Try direct connection first
    import psycopg2
    print("Testing database connection...")
    
    # Try connection with default Docker Compose credentials
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='dhs',
        user='postgres',
        password='changeme'
    )
    print("✓ Connected to database!")
    
    # Check if tables exist
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'catalog'")
    catalog_tables = cur.fetchall()
    print(f"Catalog tables: {len(catalog_tables)}")
    
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'microdata'")
    microdata_tables = cur.fetchall()
    print(f"Microdata tables: {len(microdata_tables)}")
    
    # Show some data if exists
    cur.execute("SELECT COUNT(*) FROM catalog.survey_wave")
    wave_count = cur.fetchone()[0]
    print(f"Survey waves: {wave_count}")
    
    if wave_count > 0:
        cur.execute("""
            SELECT c.name, sp.code, sw.year_label 
            FROM catalog.survey_wave sw
            JOIN catalog.country c ON c.id = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            ORDER BY c.name, sw.year_label
        """)
        surveys = cur.fetchall()
        print("Loaded surveys:")
        for country, program, year in surveys:
            print(f"  {country} {program} {year}")
    
    cur.close()
    conn.close()
    
except psycopg2.OperationalError as e:
    print(f"✗ Database connection failed: {e}")
    print("\nPossible solutions:")
    print("1. Make sure PostgreSQL is running on localhost:5432")
    print("2. Check if the 'dhs' database exists")
    print("3. Verify username/password (postgres/changeme)")
    print("\nYou can start PostgreSQL with Docker:")
    print("  docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=changeme -e POSTGRES_DB=dhs postgres:16-alpine")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()