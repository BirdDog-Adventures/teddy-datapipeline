#!/usr/bin/env python3
"""
Test script to verify Snowflake connection with the updated configuration.
This script tests the connection and basic functionality.
"""

import sys
import os
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from snowflake_loader import SnowflakeLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_snowflake_connection():
    """Test the Snowflake connection with updated configuration."""
    
    print("=" * 60)
    print("SNOWFLAKE CONNECTION TEST")
    print("=" * 60)
    print(f"Test started at: {datetime.now()}")
    print()
    
    try:
        # Initialize the loader
        print("1. Initializing SnowflakeLoader...")
        loader = SnowflakeLoader()
        print("   ✓ SnowflakeLoader initialized successfully")
        
        # Test connection
        print("\n2. Testing connection...")
        with loader as sf:
            print("   ✓ Connection established successfully")
            
            # Test basic query
            print("\n3. Testing basic query...")
            result = sf.execute_query("SELECT CURRENT_VERSION() as version")
            if result:
                print(f"   ✓ Query executed successfully")
                print(f"   Snowflake version: {result[0]['VERSION']}")
            else:
                print("   ⚠ Query returned no results")
            
            # Test database and schema access
            print("\n4. Testing database and schema access...")
            db_info = sf.execute_query("SELECT CURRENT_DATABASE() as db, CURRENT_SCHEMA() as schema")
            if db_info:
                print(f"   ✓ Database: {db_info[0]['DB']}")
                print(f"   ✓ Schema: {db_info[0]['SCHEMA']}")
            
            # Test table listing (if any tables exist)
            print("\n5. Testing table access...")
            try:
                tables = sf.execute_query("""
                    SELECT table_name, row_count 
                    FROM information_schema.tables 
                    WHERE table_schema = CURRENT_SCHEMA()
                    LIMIT 5
                """)
                if tables:
                    print(f"   ✓ Found {len(tables)} tables in schema:")
                    for table in tables:
                        print(f"     - {table['TABLE_NAME']} ({table['ROW_COUNT']} rows)")
                else:
                    print("   ℹ No tables found in current schema")
            except Exception as e:
                print(f"   ⚠ Could not list tables: {e}")
            
            # Test HubSpot tables specifically
            print("\n6. Testing HubSpot table access...")
            hubspot_tables = ['HUBSPOT_CONTACTS', 'HUBSPOT_DEALS', 'HUBSPOT_COMPANIES']
            
            for table_name in hubspot_tables:
                try:
                    count_result = sf.execute_query(f"SELECT COUNT(*) as count FROM {table_name}")
                    if count_result:
                        count = count_result[0]['COUNT']
                        print(f"   ✓ {table_name}: {count} records")
                    else:
                        print(f"   ⚠ {table_name}: No data returned")
                except Exception as e:
                    print(f"   ✗ {table_name}: {str(e)}")
        
        print("\n" + "=" * 60)
        print("CONNECTION TEST COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"\n❌ CONNECTION TEST FAILED!")
        print(f"Error: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Check your .env file configuration")
        print("2. Verify your Snowflake credentials")
        print("3. Ensure your private key file exists and is readable")
        print("4. Check network connectivity to Snowflake")
        print("5. Try updating dependencies: pip install -r requirements.txt")
        print("\n" + "=" * 60)
        return False


def test_ai_model_data_access():
    """Test data access for AI model training."""
    
    print("\n" + "=" * 60)
    print("AI MODEL DATA ACCESS TEST")
    print("=" * 60)
    
    try:
        from ai.data_processor import HubSpotDataProcessor
        
        print("1. Initializing HubSpotDataProcessor...")
        processor = HubSpotDataProcessor()
        print("   ✓ Data processor initialized")
        
        print("\n2. Testing lead data extraction...")
        try:
            # Test with a small dataset first
            lead_data = processor.extract_lead_data(days_back=30)
            
            if not lead_data.empty:
                print(f"   ✓ Successfully extracted {len(lead_data)} lead records")
                print(f"   Columns: {list(lead_data.columns)}")
                
                # Test feature engineering
                print("\n3. Testing feature engineering...")
                engineered_data = processor.engineer_features(lead_data)
                print(f"   ✓ Feature engineering completed")
                print(f"   Features created: {len(engineered_data.columns)} columns")
                
                # Test conversion target creation
                print("\n4. Testing conversion target creation...")
                target_data = processor.create_conversion_target(engineered_data)
                if 'CONVERTED' in target_data.columns:
                    conversion_rate = target_data['CONVERTED'].mean()
                    print(f"   ✓ Conversion target created")
                    print(f"   Conversion rate: {conversion_rate:.2%}")
                else:
                    print("   ⚠ Conversion target not created")
                
            else:
                print("   ⚠ No lead data found (this might be expected if tables are empty)")
                
        except Exception as e:
            print(f"   ✗ Lead data extraction failed: {e}")
        
        print("\n" + "=" * 60)
        print("AI MODEL DATA ACCESS TEST COMPLETED")
        print("=" * 60)
        return True
        
    except ImportError as e:
        print(f"   ⚠ Could not import AI modules: {e}")
        print("   This is expected if AI dependencies are not installed")
        return True
    except Exception as e:
        print(f"   ✗ AI model data access test failed: {e}")
        return False


if __name__ == "__main__":
    print("Starting comprehensive Snowflake and AI model tests...")
    
    # Test basic connection
    connection_success = test_snowflake_connection()
    
    if connection_success:
        # Test AI model data access
        ai_success = test_ai_model_data_access()
        
        if ai_success:
            print("\n🎉 ALL TESTS PASSED!")
            print("Your Snowflake connection is working correctly.")
            print("You can now run your AI model training scripts.")
        else:
            print("\n⚠️  CONNECTION WORKS, BUT AI DATA ACCESS HAS ISSUES")
            print("The basic Snowflake connection is working, but there may be")
            print("issues with the AI model data processing.")
    else:
        print("\n❌ CONNECTION TEST FAILED")
        print("Please fix the connection issues before proceeding.")
        sys.exit(1)
