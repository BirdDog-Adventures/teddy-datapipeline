#!/usr/bin/env python3
"""
Load S3 Parcel Data to Snowflake
Loads the Shackelford County data from S3 directly into Snowflake
"""

import json
import boto3
import snowflake.connector
import os
from datetime import datetime
import argparse

class S3ToSnowflakeLoader:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.snowflake_conn = None
        
    def connect_to_snowflake(self):
        """Connect to Snowflake using environment variables or parameters"""
        try:
            # Read private key from file
            with open('snowflake_private_key.pem', 'r') as f:
                private_key = f.read()
            
            self.snowflake_conn = snowflake.connector.connect(
                account='JJODRXK-BIRDDOGAWS',
                user='TEDDY_PIPELINE_USER',
                private_key=private_key,
                warehouse='COMPUTE_WH',
                database='TEDDY_DATA',
                schema='RAW'
            )
            print("✅ Connected to Snowflake successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def setup_snowflake_tables(self):
        """Create necessary tables in Snowflake"""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Create raw parcel data table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS BIRDDOG_DATA.RAW.PARCEL_DATA_RAW (
                FILE_NAME VARCHAR(500),
                COUNTY VARCHAR(100),
                STATE VARCHAR(50),
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                JSON_PAYLOAD VARIANT
            )
            """
            
            cursor.execute(create_table_sql)
            print("✅ Created/verified PARCEL_DATA_RAW table")
            
            # Create staging table for processed data
            create_staging_sql = """
            CREATE TABLE IF NOT EXISTS TEDDY_DATA.RAW.PARCEL_DATA_STAGING (
                PARCEL_ID VARCHAR(100),
                COUNTY VARCHAR(100),
                STATE VARCHAR(50),
                ADDRESS VARCHAR(500),
                OWNER_NAME VARCHAR(500),
                PROPERTY_TYPE VARCHAR(100),
                LAND_USE VARCHAR(100),
                ACRES DECIMAL(10,4),
                SQUARE_FEET DECIMAL(15,2),
                ASSESSED_VALUE DECIMAL(15,2),
                MARKET_VALUE DECIMAL(15,2),
                YEAR_BUILT INTEGER,
                BEDROOMS INTEGER,
                BATHROOMS DECIMAL(3,1),
                GEOMETRY VARIANT,
                RAW_DATA VARIANT,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            cursor.execute(create_staging_sql)
            print("✅ Created/verified PARCEL_DATA_STAGING table")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to setup Snowflake tables: {e}")
            return False
    
    def load_s3_file_to_snowflake(self, bucket_name, s3_key):
        """Load a specific S3 file into Snowflake"""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Download file from S3
            print(f"📥 Downloading {s3_key} from S3...")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            file_content = response['Body'].read().decode('utf-8')
            
            # Parse JSON content
            try:
                json_data = json.loads(file_content)
            except json.JSONDecodeError:
                # If it's not valid JSON, treat as raw text
                json_data = {"raw_content": file_content}
            
            # Extract metadata
            county = s3_key.split('/')[3] if len(s3_key.split('/')) > 3 else 'unknown'
            state = 'texas'  # Assuming Texas for Shackelford
            
            # Insert raw data
            insert_sql = """
            INSERT INTO TEDDY_DATA.RAW.PARCEL_DATA_RAW 
            (FILE_NAME, COUNTY, STATE, JSON_PAYLOAD)
            VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (s3_key, county, state, json.dumps(json_data)))
            
            # If the JSON contains parcel array, also insert into staging
            if isinstance(json_data, list):
                parcels = json_data
            elif isinstance(json_data, dict) and 'parcels' in json_data:
                parcels = json_data['parcels']
            else:
                parcels = [json_data]  # Single parcel
            
            # Insert parsed parcel data
            staging_records = []
            for parcel in parcels:
                if isinstance(parcel, dict):
                    staging_record = (
                        parcel.get('parcel_id', parcel.get('id')),
                        county,
                        state,
                        parcel.get('address', parcel.get('property_address')),
                        parcel.get('owner_name', parcel.get('owner')),
                        parcel.get('property_type', parcel.get('use_code_description')),
                        parcel.get('land_use', parcel.get('zoning')),
                        parcel.get('acres', parcel.get('lot_size_acres')),
                        parcel.get('square_feet', parcel.get('building_area')),
                        parcel.get('assessed_value', parcel.get('assessed_total_value')),
                        parcel.get('market_value', parcel.get('market_total_value')),
                        parcel.get('year_built'),
                        parcel.get('bedrooms'),
                        parcel.get('bathrooms'),
                        json.dumps(parcel.get('geometry')) if parcel.get('geometry') else None,
                        json.dumps(parcel)
                    )
                    staging_records.append(staging_record)
            
            if staging_records:
                staging_sql = """
                INSERT INTO TEDDY_DATA.RAW.PARCEL_DATA_STAGING 
                (PARCEL_ID, COUNTY, STATE, ADDRESS, OWNER_NAME, PROPERTY_TYPE, 
                 LAND_USE, ACRES, SQUARE_FEET, ASSESSED_VALUE, MARKET_VALUE, 
                 YEAR_BUILT, BEDROOMS, BATHROOMS, GEOMETRY, RAW_DATA)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(staging_sql, staging_records)
                print(f"✅ Inserted {len(staging_records)} parcel records into staging")
            
            cursor.close()
            print(f"✅ Successfully loaded {s3_key} to Snowflake")
            return True
            
        except Exception as e:
            print(f"❌ Failed to load {s3_key}: {e}")
            return False
    
    def load_shackelford_data(self, bucket_name='teddy-data-lake-dev'):
        """Load all Shackelford data from S3 to Snowflake"""
        try:
            # List all Shackelford files in S3
            print(f"🔍 Looking for Shackelford data in bucket: {bucket_name}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix='raw/parcel/shackelford/'
            )
            
            if 'Contents' not in response:
                print("❌ No Shackelford data found in S3")
                return False
            
            files = response['Contents']
            print(f"📁 Found {len(files)} files to process")
            
            success_count = 0
            for file_obj in files:
                s3_key = file_obj['Key']
                if s3_key.endswith('.json'):
                    print(f"\n📄 Processing: {s3_key}")
                    if self.load_s3_file_to_snowflake(bucket_name, s3_key):
                        success_count += 1
            
            print(f"\n🎉 Successfully loaded {success_count}/{len(files)} files to Snowflake")
            return success_count > 0
            
        except Exception as e:
            print(f"❌ Failed to load Shackelford data: {e}")
            return False
    
    def verify_data_in_snowflake(self):
        """Verify that data was loaded successfully"""
        try:
            cursor = self.snowflake_conn.cursor()
            
            # Check raw data count
            cursor.execute("SELECT COUNT(*) FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW WHERE COUNTY = 'shackelford'")
            raw_count = cursor.fetchone()[0]
            print(f"📊 Raw records in Snowflake: {raw_count}")
            
            # Check staging data count
            cursor.execute("SELECT COUNT(*) FROM TEDDY_DATA.RAW.PARCEL_DATA_STAGING WHERE COUNTY = 'shackelford'")
            staging_count = cursor.fetchone()[0]
            print(f"📊 Staging records in Snowflake: {staging_count}")
            
            # Show sample data
            if staging_count > 0:
                cursor.execute("""
                    SELECT PARCEL_ID, ADDRESS, OWNER_NAME, PROPERTY_TYPE, ACRES, ASSESSED_VALUE 
                    FROM TEDDY_DATA.RAW.PARCEL_DATA_STAGING 
                    WHERE COUNTY = 'shackelford' 
                    LIMIT 5
                """)
                
                print("\n📋 Sample parcel data:")
                for row in cursor.fetchall():
                    print(f"  ID: {row[0]}, Address: {row[1]}, Owner: {row[2]}, Type: {row[3]}, Acres: {row[4]}, Value: ${row[5]}")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to verify data: {e}")
            return False
    
    def close_connection(self):
        """Close Snowflake connection"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            print("✅ Closed Snowflake connection")

def main():
    parser = argparse.ArgumentParser(description='Load S3 parcel data to Snowflake')
    parser.add_argument('--bucket', default='teddy-data-lake-dev', help='S3 bucket name')
    parser.add_argument('--verify-only', action='store_true', help='Only verify existing data')
    
    args = parser.parse_args()
    
    loader = S3ToSnowflakeLoader()
    
    try:
        # Connect to Snowflake
        if not loader.connect_to_snowflake():
            return 1
        
        if args.verify_only:
            # Just verify existing data
            loader.verify_data_in_snowflake()
        else:
            # Setup tables
            if not loader.setup_snowflake_tables():
                return 1
            
            # Load data
            if loader.load_shackelford_data(args.bucket):
                print("\n🎉 Data loading completed successfully!")
                
                # Verify the loaded data
                print("\n🔍 Verifying loaded data...")
                loader.verify_data_in_snowflake()
            else:
                print("\n❌ Data loading failed")
                return 1
        
        return 0
        
    finally:
        loader.close_connection()

if __name__ == "__main__":
    exit(main())
