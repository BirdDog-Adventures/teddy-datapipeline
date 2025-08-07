"""
S3 to Snowflake Automatic Data Loader - Enhanced with Proven Connection Approach
Triggered by S3 events to automatically load parcel data into Snowflake
"""

import json
import boto3
import snowflake.connector
import os
import logging
from datetime import datetime
from urllib.parse import unquote_plus
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_snowflake_connection():
    """Create Snowflake connection using proven birddog-geodata-viewer approach"""
    try:
        # Base configuration - same as working implementation
        config = {
            'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
            'user': os.environ.get('SNOWFLAKE_USER'),
            'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.environ.get('SNOWFLAKE_DATABASE', 'BIRDDOG_DATA'),
            'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
            'authenticator': 'SNOWFLAKE_JWT',  # Force JWT like working implementation
            'password': None  # Explicitly disable password auth initially
        }
        
        # Add role if specified
        if os.environ.get('SNOWFLAKE_ROLE'):
            config['role'] = os.environ.get('SNOWFLAKE_ROLE')
        
        # Try private key authentication first (like working implementation)
        private_key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', '/var/task/rsa_key.p8')
        
        if os.path.exists(private_key_path):
            logger.info(f"🔑 Using private key authentication from: {private_key_path}")
            try:
                # Read and parse private key (exact same approach as working code)
                with open(private_key_path, "rb") as key_file:
                    private_key_data = key_file.read()
                
                private_key = serialization.load_pem_private_key(
                    private_key_data,
                    password=None,
                    backend=default_backend()
                )
                
                # Convert to DER format for Snowflake (same as working code)
                pkcs8_key = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                config['private_key'] = pkcs8_key
                logger.info("✅ Private key loaded successfully")
                
            except Exception as e:
                logger.error(f"❌ Error loading private key: {e}")
                # Fall back to password if available
                if os.environ.get('SNOWFLAKE_PASSWORD'):
                    logger.info("🔄 Falling back to password authentication")
                    config['password'] = os.environ.get('SNOWFLAKE_PASSWORD')
                    config['authenticator'] = 'SNOWFLAKE'  # Use default auth for password
                else:
                    raise Exception(f"No valid authentication method available: {e}")
        else:
            # Use password authentication if no private key
            if os.environ.get('SNOWFLAKE_PASSWORD'):
                logger.info("🔑 Using password authentication")
                config['password'] = os.environ.get('SNOWFLAKE_PASSWORD')
                config['authenticator'] = 'SNOWFLAKE'  # Use default auth for password
            else:
                raise Exception("No authentication method available (no private key or password)")
        
        # Create connection using proven approach
        logger.info(f"🔧 Connecting to Snowflake: {config['account']}")
        conn = snowflake.connector.connect(**config)
        
        # Verify connection (same as working implementation)
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
        user_role = cursor.fetchone()
        logger.info(f"✅ Connected as user: {user_role[0]}, role: {user_role[1]}")
        cursor.close()
        
        return conn
        
    except Exception as e:
        logger.error(f"❌ Snowflake connection failed: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Lambda function triggered by S3 events to automatically load data into Snowflake
    Enhanced with proven connection approach
    """
    
    logger.info(f"S3 to Snowflake loader triggered with event: {json.dumps(event)}")
    
    # Initialize clients
    s3_client = boto3.client('s3')
    
    results = []
    
    # Process each S3 event record
    for record in event.get('Records', []):
        try:
            # Extract S3 event details
            bucket_name = record['s3']['bucket']['name']
            s3_key = unquote_plus(record['s3']['object']['key'])
            event_name = record['eventName']
            
            logger.info(f"Processing S3 event: {event_name} for {bucket_name}/{s3_key}")
            
            # Only process JSON files in parcel directories
            if not s3_key.endswith('.json') or 'parcel' not in s3_key.lower():
                logger.info(f"Skipping non-parcel JSON file: {s3_key}")
                continue
            
            # Only process ObjectCreated events
            if not event_name.startswith('ObjectCreated'):
                logger.info(f"Skipping non-creation event: {event_name}")
                continue
            
            # Load the file into Snowflake using proven approach
            result = load_s3_file_to_snowflake(s3_client, bucket_name, s3_key)
            results.append(result)
            
        except Exception as e:
            logger.error(f"Error processing S3 record: {str(e)}")
            results.append({
                'status': 'error',
                'error': str(e),
                's3_key': s3_key if 's3_key' in locals() else 'unknown'
            })
    
    return {
        'statusCode': 200,
        'body': {
            'processed_files': len(results),
            'results': results
        }
    }

def load_s3_file_to_snowflake(s3_client, bucket_name, s3_key):
    """
    Load a specific S3 file into Snowflake using proven connection approach
    """
    snowflake_conn = None
    
    try:
        logger.info(f"Loading {s3_key} from {bucket_name} into Snowflake")
        
        # Connect to Snowflake using proven approach
        snowflake_conn = get_snowflake_connection()
        if not snowflake_conn:
            raise Exception("Failed to connect to Snowflake")
        
        # Ensure tables exist
        setup_snowflake_tables(snowflake_conn)
        
        # Download file from S3
        logger.info(f"Downloading {s3_key} from S3...")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON content
        try:
            json_data = json.loads(file_content)
            record_count = len(json_data) if isinstance(json_data, list) else 1
            logger.info(f"Successfully processed {s3_key}: {record_count} records")
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in {s3_key}, storing as raw content")
            json_data = {"raw_content": file_content}
            record_count = 1
        
        # Insert data into Snowflake using proven approach
        insert_count = insert_data_into_snowflake(snowflake_conn, s3_key, bucket_name, json_data)
        
        logger.info(f"Successfully loaded {s3_key}: {insert_count} records inserted")
        
        return {
            'status': 'success',
            's3_key': s3_key,
            'bucket': bucket_name,
            'records': record_count,
            'inserted': insert_count,
            'snowflake_status': 'inserted'
        }
        
    except Exception as e:
        logger.error(f"Error loading {s3_key} into Snowflake: {str(e)}")
        return {
            'status': 'error',
            's3_key': s3_key,
            'error': str(e)
        }
    
    finally:
        if snowflake_conn:
            snowflake_conn.close()

def setup_snowflake_tables(conn):
    """
    Create necessary tables in Snowflake if they don't exist
    Using simplified approach like working implementation
    """
    cursor = conn.cursor()
    
    try:
        # Create raw parcel data table (simplified like working implementation)
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
        logger.info("✅ Table creation/verification completed")
        
    except Exception as e:
        logger.error(f"Error setting up Snowflake tables: {e}")
        raise
    
    finally:
        cursor.close()

def insert_data_into_snowflake(conn, s3_key, bucket_name, json_data):
    """
    Insert data into Snowflake using proven approach
    """
    cursor = conn.cursor()
    
    try:
        # Insert data using PARSE_JSON like working implementation
        insert_query = """
        INSERT INTO PARCEL_DATA_RAW (FILE_NAME, JSON_PAYLOAD, S3_BUCKET, S3_KEY, INGESTION_TIMESTAMP)
        VALUES (%s, PARSE_JSON(%s), %s, %s, CURRENT_TIMESTAMP())
        """
        
        if isinstance(json_data, list):
            # Insert each item in the list
            insert_count = 0
            for item in json_data:
                cursor.execute(insert_query, (
                    s3_key,
                    json.dumps(item),
                    bucket_name,
                    s3_key
                ))
                insert_count += 1
                logger.info(f"✅ Inserted record {insert_count} from {s3_key}")
        else:
            # Insert single record
            cursor.execute(insert_query, (
                s3_key,
                json.dumps(json_data),
                bucket_name,
                s3_key
            ))
            insert_count = 1
            logger.info(f"✅ Inserted single record from {s3_key}")
        
        logger.info(f"Successfully inserted {insert_count} records into Snowflake")
        return insert_count
        
    except Exception as e:
        logger.error(f"Error inserting data into Snowflake: {e}")
        raise
    
    finally:
        cursor.close()
