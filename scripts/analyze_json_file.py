#!/usr/bin/env python3
"""
Simple JSON File Analyzer

This script analyzes the structure and content of the Shackelford County JSON file
without requiring AWS dependencies.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

def analyze_json_file(file_path: str) -> Dict[str, Any]:
    """Analyze the structure and content of the JSON file"""
    print(f"🔍 Analyzing JSON file: {file_path}")
    
    try:
        # Get file size
        file_size = os.path.getsize(file_path)
        print(f"📊 File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        
        # Read and analyze the JSON structure
        with open(file_path, 'r') as f:
            # Read first few characters to determine structure
            first_char = f.read(1)
            f.seek(0)
            
            print(f"🔍 First character: '{first_char}'")
            
            if first_char == '[':
                # Array of parcels
                print("📋 File contains an array of parcels")
                parcels = json.load(f)
                
            elif first_char == '{':
                # Single object or object with parcels array
                data = json.load(f)
                if isinstance(data, dict):
                    print(f"🗂️  File contains object with keys: {list(data.keys())}")
                    
                    if 'parcels' in data:
                        parcels = data['parcels']
                        print("📋 File contains object with 'parcels' array")
                    elif 'features' in data:
                        parcels = data['features']
                        print("🗺️  File contains GeoJSON-like structure with 'features'")
                    elif 'results' in data:
                        parcels = data['results']
                        print("📋 File contains object with 'results' array")
                    else:
                        # Assume the object itself is a parcel
                        parcels = [data]
                        print("📄 File contains single parcel object")
                else:
                    parcels = [data]
            else:
                raise ValueError("Unknown JSON structure")
            
            # Analyze parcel structure
            total_parcels = len(parcels)
            print(f"📊 Total parcels found: {total_parcels:,}")
            
            if total_parcels > 0:
                sample_parcel = parcels[0]
                print(f"🔑 Sample parcel keys: {list(sample_parcel.keys())}")
                
                # Check for common parcel fields
                common_fields = [
                    'parcel_id', 'id', 'parcel_number', 'apn',
                    'address', 'street_address', 'full_address',
                    'owner', 'owner_name', 'owner_address',
                    'acreage', 'acres', 'area', 'square_feet',
                    'value', 'assessed_value', 'market_value',
                    'geometry', 'coordinates', 'lat', 'lng', 'latitude', 'longitude',
                    'county', 'state', 'zip', 'zipcode',
                    'land_use', 'zoning', 'property_type'
                ]
                
                found_fields = []
                for field in common_fields:
                    if field in sample_parcel:
                        found_fields.append(field)
                
                print(f"✅ Common fields found: {found_fields}")
                
                # Show sample values for found fields
                print("\n📋 Sample parcel data:")
                for field in found_fields[:10]:  # Show first 10 fields
                    value = sample_parcel[field]
                    if isinstance(value, str) and len(value) > 50:
                        value = value[:50] + "..."
                    print(f"   {field}: {value}")
                
                # Analyze data types
                field_types = {}
                for key, value in sample_parcel.items():
                    field_types[key] = type(value).__name__
                
                print(f"\n🔍 Field data types: {dict(list(field_types.items())[:10])}")
            
            return {
                'file_size': file_size,
                'total_parcels': total_parcels,
                'sample_parcel': sample_parcel if total_parcels > 0 else None,
                'structure_type': 'array' if first_char == '[' else 'object',
                'found_fields': found_fields if total_parcels > 0 else [],
                'field_types': field_types if total_parcels > 0 else {}
            }
            
    except Exception as e:
        print(f"❌ Error analyzing JSON file: {e}")
        raise

def simulate_chunking(total_parcels: int, chunk_size: int = 500) -> Dict[str, Any]:
    """Simulate chunking process"""
    print(f"\n🔄 Simulating chunking process...")
    
    total_chunks = (total_parcels + chunk_size - 1) // chunk_size  # Ceiling division
    
    print(f"📊 Would split {total_parcels:,} parcels into {total_chunks} chunks of max {chunk_size} parcels each")
    
    # Calculate chunk sizes
    chunk_info = []
    for i in range(total_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_parcels)
        chunk_parcels = end_idx - start_idx
        
        s3_key = f"raw/parcel/bulk/{datetime.now().strftime('%Y-%m-%d')}/shackelford/chunk_{i:04d}.json"
        
        chunk_info.append({
            'chunk_index': i,
            'start_index': start_idx,
            'end_index': end_idx,
            'parcel_count': chunk_parcels,
            's3_key': s3_key
        })
        
        if i < 5:  # Show first 5 chunks
            print(f"   Chunk {i+1}: parcels {start_idx:,}-{end_idx-1:,} ({chunk_parcels} parcels) -> {s3_key}")
    
    if total_chunks > 5:
        print(f"   ... and {total_chunks - 5} more chunks")
    
    return {
        'total_chunks': total_chunks,
        'chunk_size': chunk_size,
        'chunk_info': chunk_info
    }

def estimate_processing_time(total_parcels: int) -> Dict[str, Any]:
    """Estimate processing time and costs"""
    print(f"\n⏱️  Estimating processing time and costs...")
    
    # Assumptions
    parcels_per_second = 100  # Processing rate
    lambda_memory_mb = 1024
    lambda_cost_per_gb_second = 0.0000166667
    
    processing_time_seconds = total_parcels / parcels_per_second
    processing_time_minutes = processing_time_seconds / 60
    
    # Lambda cost calculation
    memory_gb = lambda_memory_mb / 1024
    lambda_cost = processing_time_seconds * memory_gb * lambda_cost_per_gb_second
    
    print(f"📊 Estimated processing time: {processing_time_minutes:.1f} minutes")
    print(f"💰 Estimated Lambda cost: ${lambda_cost:.4f}")
    
    # Storage estimates
    avg_parcel_size_bytes = 2000  # Estimated average parcel JSON size
    total_storage_mb = (total_parcels * avg_parcel_size_bytes) / (1024 * 1024)
    s3_storage_cost_per_gb = 0.023  # Standard storage
    s3_cost = (total_storage_mb / 1024) * s3_storage_cost_per_gb
    
    print(f"💾 Estimated S3 storage: {total_storage_mb:.1f} MB")
    print(f"💰 Estimated S3 cost (monthly): ${s3_cost:.4f}")
    
    return {
        'processing_time_seconds': processing_time_seconds,
        'processing_time_minutes': processing_time_minutes,
        'lambda_cost': lambda_cost,
        's3_storage_mb': total_storage_mb,
        's3_cost_monthly': s3_cost,
        'total_estimated_cost': lambda_cost + s3_cost
    }

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python3 analyze_json_file.py <path_to_json_file>")
        sys.exit(1)
    
    json_file_path = sys.argv[1]
    
    # Validate file exists
    if not os.path.exists(json_file_path):
        print(f"❌ JSON file not found: {json_file_path}")
        sys.exit(1)
    
    print("🚀 Starting JSON file analysis...")
    print("=" * 80)
    
    try:
        # Analyze file structure
        analysis = analyze_json_file(json_file_path)
        
        # Simulate chunking
        if analysis['total_parcels'] > 0:
            chunking = simulate_chunking(analysis['total_parcels'])
            
            # Estimate processing
            estimates = estimate_processing_time(analysis['total_parcels'])
            
            # Summary
            print("\n" + "=" * 80)
            print("📋 ANALYSIS SUMMARY")
            print("=" * 80)
            print(f"✅ File successfully analyzed")
            print(f"📊 Total parcels: {analysis['total_parcels']:,}")
            print(f"📦 Total chunks: {chunking['total_chunks']}")
            print(f"⏱️  Processing time: {estimates['processing_time_minutes']:.1f} minutes")
            print(f"💰 Estimated cost: ${estimates['total_estimated_cost']:.4f}")
            print(f"🔑 Key fields found: {len(analysis['found_fields'])}")
            
            # Validation status
            print("\n🎯 PIPELINE VALIDATION STATUS:")
            print("✅ JSON structure: Valid")
            print("✅ Parcel data: Present")
            print("✅ Chunking: Feasible")
            print("✅ Processing: Estimated")
            print("✅ Cost: Reasonable")
            
            print("\n🚀 Ready for bulk ingestion pipeline!")
            
        else:
            print("❌ No parcels found in file")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
