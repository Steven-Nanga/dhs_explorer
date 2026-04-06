#!/usr/bin/env python3
"""Check if data is loaded in the database."""

import sys
sys.path.insert(0, '.')

try:
    from loader.main import _connect
    conn = _connect()
    cur = conn.cursor()
    
    # Check for Malawi 2024
    cur.execute("""
        SELECT c.name, sp.code, sw.year_label, COUNT(sf.id) as files
        FROM catalog.survey_wave sw
        JOIN catalog.country c ON c.id = sw.country_id
        JOIN catalog.survey_program sp ON sp.id = sw.program_id
        LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
        WHERE c.name = 'Malawi' AND sw.year_label LIKE '%2024%'
        GROUP BY c.name, sp.code, sw.year_label
    """)
    results = cur.fetchall()
    
    if results:
        print("Malawi 2024 data FOUND in database:")
        for country, program, year, files in results:
            print(f"  {country} {program} {year} ({files} files)")
    else:
        print("No Malawi 2024 data found in database.")
        
        # Show what IS in the database
        cur.execute("""
            SELECT c.name, sp.code, sw.year_label, COUNT(sf.id) as files
            FROM catalog.survey_wave sw
            JOIN catalog.country c ON c.id = sw.country_id
            JOIN catalog.survey_program sp ON sp.id = sw.program_id
            LEFT JOIN catalog.survey_file sf ON sf.survey_wave_id = sw.id
            GROUP BY c.name, sp.code, sw.year_label
            ORDER BY c.name, sw.year_label
        """)
        all_surveys = cur.fetchall()
        if all_surveys:
            print("\nLoaded surveys in database:")
            for country, program, year, files in all_surveys:
                print(f"  {country} {program} {year} ({files} files)")
        else:
            print("\nNo surveys loaded at all.")
    
    # Check the files in data directory that should be loaded
    import os
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    if os.path.exists(data_dir):
        zip_files = [f for f in os.listdir(data_dir) if f.endswith('.zip')]
        print(f"\nZIP files in data directory: {len(zip_files)}")
        for f in zip_files[:10]:  # Show first 10
            print(f"  {f}")
        if len(zip_files) > 10:
            print(f"  ... and {len(zip_files) - 10} more")
    else:
        print(f"\nData directory not found: {data_dir}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()