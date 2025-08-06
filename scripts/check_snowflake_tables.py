#!/usr/bin/env python3
"""
Check what tables exist in Snowflake and their schemas
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from utils.snowflake_connector import get_snowflake_connector

def main():
    print("🔍 Checking Snowflake tables and schemas...")
    
    try:
        with get_snowflake_connector('dev') as sf:
            # Test connection
            test_result = sf.test_connection()
            print(f"✅ Connection test: {test_result}")
            
            if test_result['status'] != 'success':
                print("❌ Connection failed, cannot check tables")
                return
            
            # Check available databases
            print("\n📊 Available databases:")
            databases = sf.execute_query("SHOW DATABASES")
            for db in databases:
                print(f"  - {db['name']}")
            
            # Check schemas in BIRDDOG_DATA
            print("\n📊 Schemas in BIRDDOG_DATA:")
            try:
                schemas = sf.execute_query("SHOW SCHEMAS IN DATABASE BIRDDOG_DATA")
                for schema in schemas:
                    print(f"  - {schema['name']}")
            except Exception as e:
                print(f"  ❌ Error accessing BIRDDOG_DATA: {e}")
            
            # Check schemas in TEDDY_DATA if it exists
            print("\n📊 Schemas in TEDDY_DATA:")
            try:
                schemas = sf.execute_query("SHOW SCHEMAS IN DATABASE TEDDY_DATA")
                for schema in schemas:
                    print(f"  - {schema['name']}")
            except Exception as e:
                print(f"  ❌ Error accessing TEDDY_DATA: {e}")
            
            # Check tables in various schemas
            schemas_to_check = [
                ('BIRDDOG_DATA', 'RAW'),
                ('BIRDDOG_DATA', 'STAGING'),
                ('BIRDDOG_DATA', 'CURATED'),
                ('TEDDY_DATA', 'RAW'),
                ('TEDDY_DATA', 'STAGING'),
                ('TEDDY_DATA', 'CURATED')
            ]
            
            for database, schema in schemas_to_check:
                print(f"\n📊 Tables in {database}.{schema}:")
                try:
                    tables = sf.execute_query(f"SHOW TABLES IN SCHEMA {database}.{schema}")
                    if tables:
                        for table in tables:
                            print(f"  - {table['name']}")
                    else:
                        print("  (no tables found)")
                except Exception as e:
                    print(f"  ❌ Error accessing {database}.{schema}: {e}")
            
            # Look for parcel-related tables specifically
            print(f"\n🔍 Searching for parcel-related tables...")
            try:
                # Search across all accessible schemas
                parcel_search = sf.execute_query("""
                    SELECT 
                        table_catalog as database_name,
                        table_schema as schema_name,
                        table_name
                    FROM information_schema.tables 
                    WHERE UPPER(table_name) LIKE '%PARCEL%'
                    ORDER BY table_catalog, table_schema, table_name
                """)
                
                if parcel_search:
                    for table in parcel_search:
                        print(f"  - {table['DATABASE_NAME']}.{table['SCHEMA_NAME']}.{table['TABLE_NAME']}")
                else:
                    print("  (no parcel tables found)")
                    
            except Exception as e:
                print(f"  ❌ Error searching for parcel tables: {e}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
