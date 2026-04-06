#!/usr/bin/env python3
"""Quick test of the new filter API endpoints."""

import sys
sys.path.insert(0, '.')

from webapp.app import create_app

app = create_app()
with app.test_client() as client:
    print("Testing new filter API endpoints...")
    
    # Test countries endpoint
    response = client.get('/api/search/filters/countries')
    print(f"Countries endpoint status: {response.status_code}")
    if response.status_code == 200:
        data = response.get_json()
        print(f"Countries returned: {data.get('countries', [])}")
    
    # Test recodes endpoint
    response = client.get('/api/search/filters/recodes')
    print(f"Recodes endpoint status: {response.status_code}")
    if response.status_code == 200:
        data = response.get_json()
        print(f"Recodes returned: {data.get('recodes', [])}")
    
    # Test years endpoint
    response = client.get('/api/search/filters/years')
    print(f"Years endpoint status: {response.status_code}")
    if response.status_code == 200:
        data = response.get_json()
        print(f"Years returned: {data.get('years', [])}")
    
    # Also test the main search endpoint still works
    response = client.get('/api/search?q=age')
    print(f"\nSearch endpoint (q=age) status: {response.status_code}")
    if response.status_code == 200:
        data = response.get_json()
        print(f"Found {data.get('count', 0)} results")

if __name__ == '__main__':
    print("Filter API test complete.")