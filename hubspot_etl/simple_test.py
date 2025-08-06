#!/usr/bin/env python3
"""
Simple test script for HubSpot ETL Pipeline

This script demonstrates how to use the HubSpotETLPipeline class
to extract data from HubSpot and load it into Snowflake.

Usage:
    cd birddog-datascraping-service/scraper
    python -m hubspot_etl.simple_test
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

def main():
    """Main test function for HubSpot ETL Pipeline"""
    
    print("=" * 60)
    print("HubSpot ETL Pipeline Test")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Import the pipeline using the package structure
        from hubspot_etl import HubSpotETLPipeline
        
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
        
        if 'object_results' in results:
            print("\nBreakdown by Object Type:")
            for obj_type, result in results['object_results'].items():
                if isinstance(result, dict) and 'records_loaded' in result:
                    print(f"  • {obj_type.capitalize()}: {result['records_loaded']:,} records")
                else:
                    print(f"  • {obj_type.capitalize()}: {result}")
        
        if 'duration' in results:
            print(f"\nProcessing Time: {results['duration']:.2f} seconds")
        
        if 'status' in results:
            print(f"Status: {results['status']}")
        
        print()
        print("🎉 Test completed successfully!")
        
    except ImportError as e:
        print(f"❌ Import Error: {str(e)}")
        print("\n💡 To fix this, run the script from the parent directory:")
        print("   cd birddog-datascraping-service/scraper")
        print("   python -m hubspot_etl.simple_test")
        print("\n   Or ensure the hubspot_etl package is properly installed.")
        sys.exit(1)
        
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
        from hubspot_etl import HubSpotClient, HubSpotDataTransformer, SnowflakeLoader
        
        # Test HubSpot client
        print("Testing HubSpot client initialization...")
        hubspot_client = HubSpotClient()
        print("✅ HubSpot client initialized")
        
        # Test data transformer
        print("Testing data transformer initialization...")
        transformer = HubSpotDataTransformer()
        print("✅ Data transformer initialized")
        
        # Test Snowflake loader
        print("Testing Snowflake loader initialization...")
        snowflake_loader = SnowflakeLoader()
        print("✅ Snowflake loader initialized")
        
        print("✅ All component tests passed")
        
    except Exception as e:
        print(f"❌ Component test failed: {str(e)}")

def test_with_custom_config():
    """Test pipeline with custom configuration"""
    
    print("\n⚙️  Testing with Custom Configuration:")
    print("-" * 40)
    
    try:
        from hubspot_etl import HubSpotETLPipeline
        
        # Example of custom configuration
        custom_config = {
            'batch_size': 100,
            'max_retries': 3,
            'chunk_size': 500,
            'object_types': ['contacts']  # Just test contacts
        }
        
        pipeline = HubSpotETLPipeline(config=custom_config)
        
        print(f"✅ Custom config pipeline initialized with config: {custom_config}")
        
        # You could run a smaller test extraction here if needed
        # results = pipeline.run_full_extraction(
        #     object_types=['contacts'],
        #     max_records_per_type=10  # Limit for testing
        # )
        
        print("✅ Custom config test completed")
        
    except Exception as e:
        print(f"❌ Custom config test failed: {str(e)}")

if __name__ == "__main__":
    # Load environment variables from .env file
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        load_dotenv(env_file)
        print(f"✅ Loaded environment variables from {env_file}")
    else:
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
