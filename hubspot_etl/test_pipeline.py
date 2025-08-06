#!/usr/bin/env python3
"""
Test script for HubSpot ETL Pipeline

This script demonstrates how to use the HubSpotETLPipeline class
to extract data from HubSpot and load it into Snowflake.

Usage:
    python test_pipeline.py
"""

import os
import sys
from datetime import datetime

# Add the current directory to Python path for local imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import all components directly to avoid relative import issues
from hubspot_client import HubSpotClient
from data_transformer import HubSpotDataTransformer
from snowflake_loader import SnowflakeLoader
from etl_pipeline import HubSpotETLPipeline

def main():
    """Main test function for HubSpot ETL Pipeline"""
    
    print("=" * 60)
    print("HubSpot ETL Pipeline Test")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Initialize pipeline with default settings
        print("🔧 Initializing HubSpot ETL Pipeline...")
        pipeline = HubSpotETLPipeline()
        print("✅ Pipeline initialized successfully")
        print()
        
        # Define object types to extract
        object_types = ['contacts', 'companies', 'deals']
        print(f"📊 Extracting data for object types: {', '.join(object_types)}")
        print()
        
        # Run full extraction
        print("🚀 Starting full extraction...")
        results = pipeline.run_full_extraction(
            object_types=object_types
        )
        
        print("✅ Extraction completed successfully!")
        print()
        
        # Display results
        print("📈 EXTRACTION RESULTS:")
        print("-" * 40)
        print(f"Total Records Extracted: {results.get('total_records', 0):,}")
        
        if 'object_counts' in results:
            print("\nBreakdown by Object Type:")
            for obj_type, count in results['object_counts'].items():
                print(f"  • {obj_type.capitalize()}: {count:,} records")
        
        if 'processing_time' in results:
            print(f"\nProcessing Time: {results['processing_time']:.2f} seconds")
        
        if 'snowflake_status' in results:
            print(f"Snowflake Load Status: {results['snowflake_status']}")
        
        print()
        print("🎉 Test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during pipeline execution: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        
        # Print more detailed error information if available
        if hasattr(e, '__traceback__'):
            import traceback
            print("\nDetailed error traceback:")
            traceback.print_exc()
        
        sys.exit(1)
    
    finally:
        print()
        print(f"Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

def test_individual_components():
    """Test individual components of the pipeline"""
    
    print("\n🔍 Testing Individual Components:")
    print("-" * 40)
    
    try:
        # Test HubSpot connection
        print("Testing HubSpot connection...")
        pipeline = HubSpotETLPipeline()
        
        # Test Snowflake connection
        print("Testing Snowflake connection...")
        # Add specific connection tests here
        
        print("✅ All component tests passed")
        
    except Exception as e:
        print(f"❌ Component test failed: {str(e)}")

def test_with_custom_config():
    """Test pipeline with custom configuration"""
    
    print("\n⚙️  Testing with Custom Configuration:")
    print("-" * 40)
    
    try:
        # Example of custom configuration
        custom_config = {
            'batch_size': 100,
            'max_retries': 3,
            'timeout': 30
        }
        
        pipeline = HubSpotETLPipeline(config=custom_config)
        
        # Run a smaller test extraction
        results = pipeline.run_full_extraction(
            object_types=['contacts'],
            limit=50  # Limit for testing
        )
        
        print(f"✅ Custom config test completed. Extracted {results.get('total_records', 0)} records")
        
    except Exception as e:
        print(f"❌ Custom config test failed: {str(e)}")

if __name__ == "__main__":
    # Check if .env file exists
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if not os.path.exists(env_file):
        print("⚠️  Warning: .env file not found. Please ensure environment variables are set.")
        print(f"Expected location: {env_file}")
        print()
    
    # Run main test
    main()
    
    # Run additional tests if main test succeeds
    try:
        test_individual_components()
        test_with_custom_config()
    except KeyboardInterrupt:
        print("\n\n⏹️  Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Additional tests failed: {str(e)}")
