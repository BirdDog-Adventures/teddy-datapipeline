#!/usr/bin/env python3
"""
Test script to troubleshoot Regrid API coordinate queries
"""

import requests
import json

# Your API key
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJyZWdyaWQuY29tIiwiaWF0IjoxNzU0NTA0ODkyLCJleHAiOjE3ODYwNjE4NDQsImciOjkyMTkzLCJ0IjoxLCJjYXAiOiJwYTp0cyIsInRpIjo2OTIsInRucCI6MX0.AmqtzuOOaCaJ4TSRaR2CUj2_J2ln1Ugyn4iH-IRsSok"

# Test coordinates
lat = 29.941759
lon = -103.568044

print(f"Testing Regrid API with coordinates: lat={lat}, lon={lon}")

# Headers
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}

# Test different API endpoints and parameter formats
test_cases = [
    {
        "name": "Standard parcels endpoint with lat/lon",
        "url": "https://app.regrid.com/api/v1/parcels",
        "params": {"lat": lat, "lon": lon, "limit": 1}
    },
    {
        "name": "Standard parcels endpoint with latitude/longitude", 
        "url": "https://app.regrid.com/api/v1/parcels",
        "params": {"latitude": lat, "longitude": lon, "limit": 1}
    },
    {
        "name": "Search endpoint with coordinates",
        "url": "https://app.regrid.com/api/v1/search",
        "params": {"lat": lat, "lon": lon, "limit": 1}
    },
    {
        "name": "Parcels endpoint with geom parameter",
        "url": "https://app.regrid.com/api/v1/parcels",
        "params": {"geom": f"POINT({lon} {lat})", "limit": 1}
    },
    {
        "name": "Parcels endpoint with point parameter",
        "url": "https://app.regrid.com/api/v1/parcels",
        "params": {"point": f"{lon},{lat}", "limit": 1}
    }
]

for test_case in test_cases:
    print(f"\n--- Testing: {test_case['name']} ---")
    print(f"URL: {test_case['url']}")
    print(f"Params: {test_case['params']}")
    
    try:
        response = requests.get(test_case['url'], headers=headers, params=test_case['params'])
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"SUCCESS! Found {len(data.get('results', []))} results")
            if data.get('results'):
                parcel = data['results'][0]
                print(f"First parcel ID: {parcel.get('id', 'N/A')}")
                print(f"Address: {parcel.get('properties', {}).get('address', 'N/A')}")
            break
        else:
            print(f"Error: {response.status_code} - {response.text[:200]}")
            
    except Exception as e:
        print(f"Exception: {e}")

print("\n--- Testing with swapped coordinates (common issue) ---")
# Sometimes lat/lon are swapped
swapped_lat = -103.568044
swapped_lon = 29.941759

test_swapped = {
    "name": "Swapped coordinates test",
    "url": "https://app.regrid.com/api/v1/parcels", 
    "params": {"lat": swapped_lat, "lon": swapped_lon, "limit": 1}
}

print(f"Testing with swapped: lat={swapped_lat}, lon={swapped_lon}")
try:
    response = requests.get(test_swapped['url'], headers=headers, params=test_swapped['params'])
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"SUCCESS with swapped coordinates! Found {len(data.get('results', []))} results")
    else:
        print(f"Error with swapped: {response.status_code}")
        
except Exception as e:
    print(f"Exception with swapped: {e}")
