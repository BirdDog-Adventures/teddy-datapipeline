#!/usr/bin/env python3
"""
Test Snowflake Connectivity with Proven Approach
This script tests the exact same connection logic we implemented in the Lambda function
"""

import os
import sys
import json
import logging
from pathlib import Path

# Add the lambda directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent / 'lambda'))

# Import our proven connection function
from s3_to_snowflake_loader import get_snowflake_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_snowflake_connection():
    """Test the Snowflake connection using our proven approach"""
    
    print("🔧 Testing Snowflake Connectivity with Proven Approach")
    print("=" * 60)
    
    # Check environment variables
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER']
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("Please set these in your .env file or environment")
        return False
    
    # Check authentication method
    has_password = bool(os.environ.get('SNOWFLAKE_PASSWORD'))
    has_private_key = os.path.exists(os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', '/var/task/rsa_key.p8'))
    
    print(f"🔑 Authentication methods available:")
    print(f"   - Password: {'✅' if has_password else '❌'}")
    print(f"   - Private Key: {'✅' if has_private_key else '❌'}")
    
    if not has_password and not has_private_key:
        print("❌ No authentication method available!")
        print("Please set SNOWFLAKE_PASSWORD or provide a private key file")
        return False
    
    try:
        # Test the connection using our proven approach
        print("\n🔧 Attempting Snowflake connection...")
        conn = get_snowflake_connection()
        
        if conn:
            print("✅ Successfully connected to Snowflake!")
            
            # Test basic query
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
            result = cursor.fetchone()
            
            print(f"📊 Connection Details:")
            print(f"   - Database: {result[0]}")
            print(f"   - Schema: {result[1]}")
            print(f"   - Warehouse: {result[2]}")
            
            # Test table creation (same as Lambda function)
            print("\n🏗️ Testing table creation...")
            create_table_query = """
            CREATE TABLE IF NOT EXISTS PARCEL_DATA_RAW (
                FILE_NAME VARCHAR(500),
                JSON_PAYLOAD VARIANT,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                S3_BUCKET VARCHAR(100),
                S3_KEY VARCHAR(500)
            )
            """
            
            cursor.execute(create_table_query)
            print("✅ Table creation/verification successful!")
            
            # Test data insertion
            print("\n📝 Testing data insertion...")
            test_data = {"test": "data", "timestamp": "2025-08-05T23:20:00Z"}
            insert_query = """
            INSERT INTO PARCEL_DATA_RAW (FILE_NAME, JSON_PAYLOAD, S3_BUCKET, S3_KEY)
            VALUES (%s, PARSE_JSON(%s), %s, %s)
            """
            
            cursor.execute(insert_query, (
                'test_file.json',
                json.dumps(test_data),
                'test-bucket',
                'test/path/test_file.json'
            ))
            
            print("✅ Data insertion successful!")
            
            # Verify the data was inserted
            cursor.execute("SELECT COUNT(*) FROM PARCEL_DATA_RAW WHERE FILE_NAME = 'test_file.json'")
            count = cursor.fetchone()[0]
            print(f"✅ Verified: {count} test record(s) found in table")
            
            # Clean up test data
            cursor.execute("DELETE FROM PARCEL_DATA_RAW WHERE FILE_NAME = 'test_file.json'")
            print("🧹 Cleaned up test data")
            
            cursor.close()
            conn.close()
            
            print("\n🎉 All tests passed! Snowflake connectivity is working perfectly!")
            print("The Lambda function should work with the same connection approach.")
            return True
            
    except Exception as e:
        print(f"❌ Connection test failed: {str(e)}")
        logger.exception("Detailed error information:")
        return False

if __name__ == "__main__":
    # Load environment variables from .env file if it exists
    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        print(f"📁 Loading environment variables from {env_file}")
        from dotenv import load_dotenv
        load_dotenv(env_file)
    
    success = test_snowflake_connection()
    sys.exit(0 if success else 1)
