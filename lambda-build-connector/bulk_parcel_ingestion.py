"""
Bulk Parcel Data Ingestion Lambda Function

This Lambda function handles county-wide parcel data ingestion from Regrid API
with rate limiting, chunking, and S3 storage for event-driven Snowflake processing.
"""

import json
import boto3
import os
import logging
from datetime import datetime
from typing import List, Dict, Any
import sys
sys.path.append('/opt')
from utils.snowflake_connector import SnowflakeConnector

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Bulk parcel data ingestion - loads S3 data to Snowflake
    Triggered by Step Functions for processing uploaded parcel data
    """
    
    logger.info(f"Bulk parcel ingestion triggered with event: {json.dumps(event)}")
    
    # Extract parameters from Step Functions
    counties = event.get('counties', [])
    batch_size = event.get('batch_size', 1000)
    chunk_size = event.get('chunk_size', 500)
    
    # Initialize clients
    s3_client = boto3.client('s3')
    snowflake_connector = SnowflakeConnector()
    
    results = []
    
    try:
        # Connect to Snowflake
        if not snowflake_connector.connect():
            raise Exception("Failed to connect to Snowflake")
        
        # Process each county
        for county in counties:
            try:
                logger.info(f"Processing S3 data for county: {county}")
                
                # Find S3 files for this county
                bucket_name = os.environ.get('DATA_BUCKET', 'teddy-data-lake-dev')
                s3_files = find_county_files_in_s3(s3_client, bucket_name, county)
                
                if not s3_files:
                    logger.warning(f"No S3 files found for county: {county}")
                    results.append({
                        'county': county,
                        'status': 'warning',
                        'message': 'No S3 files found',
                        'files_processed': 0
                    })
                    continue
                
                # Process each S3 file
                files_processed = 0
                total_records = 0
                
                for s3_key in s3_files:
                    try:
                        logger.info(f"Processing S3 file: {s3_key}")
                        
                        # Download and parse JSON from S3
                        json_data = download_and_parse_s3_file(s3_client, bucket_name, s3_key)
                        
                        # Load data into Snowflake
                        raw_count, staging_count = load_data_to_snowflake(
                            snowflake_connector, s3_key, county, json_data
                        )
                        
                        files_processed += 1
                        total_records += staging_count
                        
                        logger.info(f"Loaded {s3_key}: {raw_count} raw, {staging_count} staging records")
                        
                    except Exception as file_error:
                        logger.error(f"Error processing file {s3_key}: {str(file_error)}")
                        continue
                
                results.append({
                    'county': county,
                    'status': 'success',
                    'files_processed': files_processed,
                    'total_records': total_records,
                    's3_files': s3_files
                })
                
                logger.info(f"Completed processing for {county}: {files_processed} files, {total_records} records")
                
            except Exception as county_error:
                logger.error(f"Error processing county {county}: {str(county_error)}")
                results.append({
                    'county': county,
                    'status': 'error',
                    'error': str(county_error)
                })
        
        return {
            'statusCode': 200,
            'body': {
                'processed_counties': len(results),
                'results': results,
                'message': 'Bulk parcel ingestion completed'
            }
        }
        
    except Exception as e:
        logger.error(f"Critical error in bulk parcel ingestion: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Bulk parcel ingestion failed'
            }
        }
    
    finally:
        # Close Snowflake connection
        if snowflake_connector:
            snowflake_connector.close()

def find_county_files_in_s3(s3_client, bucket_name, county):
    """
    Find all JSON files for a specific county in S3
    """
    try:
        s3_files = []
        
        # Search in multiple possible paths
        prefixes = [
            f"raw/parcel/{county}/",
            f"raw/parcel/bulk/{county}/",
            f"raw/parcel/{county.lower()}/",
            f"raw/parcel/bulk/{county.lower()}/"
        ]
        
        for prefix in prefixes:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if obj['Key'].endswith('.json'):
                            s3_files.append(obj['Key'])
                            
            except Exception as e:
                logger.debug(f"No files found with prefix {prefix}: {e}")
                continue
        
        # Remove duplicates
        s3_files = list(set(s3_files))
        logger.info(f"Found {len(s3_files)} files for county {county}")
        
        return s3_files
        
    except Exception as e:
        logger.error(f"Error finding S3 files for county {county}: {e}")
        return []

def download_and_parse_s3_file(s3_client, bucket_name, s3_key):
    """
    Download and parse JSON file from S3
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON content
        json_data = json.loads(file_content)
        return json_data
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {s3_key}: {e}")
        return {"raw_content": file_content, "parse_error": str(e)}
    except Exception as e:
        logger.error(f"Error downloading {s3_key}: {e}")
        raise

def load_data_to_snowflake(snowflake_connector, s3_key, county, json_data):
    """
    Load parsed JSON data into Snowflake tables
    """
    try:
        # Extract state from S3 key or county name
        state = extract_state_from_data(s3_key, county)
        
        # Insert raw data
        raw_count = snowflake_connector.insert_raw_data(s3_key, county, state, json_data)
        
        # Process and insert staging data
        staging_count = 0
        
        # Determine parcel data structure
        if isinstance(json_data, list):
            parcels = json_data
        elif isinstance(json_data, dict) and 'parcels' in json_data:
            parcels = json_data['parcels']
        elif isinstance(json_data, dict) and any(key in json_data for key in ['parcel_id', 'id', 'address']):
            parcels = [json_data]  # Single parcel
        else:
            parcels = []  # No recognizable parcel data
        
        # Insert staging data
        if parcels:
            staging_count = snowflake_connector.insert_staging_data(county, state, parcels)
        
        return raw_count, staging_count
        
    except Exception as e:
        logger.error(f"Error loading data to Snowflake: {e}")
        raise

def extract_state_from_data(s3_key, county):
    """
    Extract state information from S3 key or county name
    """
    s3_key_lower = s3_key.lower()
    county_lower = county.lower()
    
    # Check for state indicators
    if 'tx_' in s3_key_lower or 'texas' in s3_key_lower or 'tx' in county_lower:
        return 'texas'
    elif 'ca_' in s3_key_lower or 'california' in s3_key_lower or 'ca' in county_lower:
        return 'california'
    elif 'fl_' in s3_key_lower or 'florida' in s3_key_lower or 'fl' in county_lower:
        return 'florida'
    
    return 'unknown'

async def fetch_county_parcels_bulk(client, county: str, batch_size: int, cache_manager = None) -> List[Dict[str, Any]]:
    """
    Fetch all parcels for a county with rate limiting and pagination
    """
    all_parcels = []
    offset = 0
    rate_limit_delay = 3.6  # Respect 1000 requests/hour limit
    
    while True:
        try:
            # Add rate limiting delay
            await asyncio.sleep(rate_limit_delay)
            
            # Fetch batch of parcels
            batch = await client.search_by_county(
                county=county,
                limit=batch_size,
                offset=offset
            )
            
            if not batch or len(batch) == 0:
                break
                
            all_parcels.extend(batch)
            offset += batch_size
            
            logger.info(f"Fetched {len(batch)} parcels for {county}, total: {len(all_parcels)}")
            
            # Break if we got less than batch_size (end of data)
            if len(batch) < batch_size:
                break
                
        except Exception as e:
            logger.error(f"Error fetching batch for {county} at offset {offset}: {str(e)}")
            # Continue with next batch on error
            offset += batch_size
            continue
    
    return all_parcels

def chunk_parcels(parcels: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    """Split parcels into chunks for S3 storage"""
    return [parcels[i:i + chunk_size] for i in range(0, len(parcels), chunk_size)]

def publish_s3_event(sns_client, detail):
    """Publish S3 event for Snowpipe processing via SNS"""
    try:
        topic_arn = os.environ.get('PARCEL_DATA_TOPIC_ARN')
        if not topic_arn:
            logger.error("PARCEL_DATA_TOPIC_ARN environment variable not set")
            return
            
        sns_client.publish(
            TopicArn=topic_arn,
            Subject='Parcel Data S3 Upload',
            Message=json.dumps(detail),
            MessageAttributes={
                'event_type': {
                    'DataType': 'String',
                    'StringValue': 's3_upload'
                },
                'county': {
                    'DataType': 'String',
                    'StringValue': detail.get('county', 'unknown')
                },
                'ingestion_type': {
                    'DataType': 'String',
                    'StringValue': detail.get('ingestion_type', 'bulk')
                }
            }
        )
        logger.info(f"Published S3 event for {detail.get('s3_key')}")
        
    except Exception as e:
        logger.error(f"Error publishing S3 event: {e}")

def publish_error_event(sns_client, detail):
    """Publish error event for monitoring via SNS"""
    try:
        topic_arn = os.environ.get('PIPELINE_STATUS_TOPIC_ARN')
        if not topic_arn:
            logger.error("PIPELINE_STATUS_TOPIC_ARN environment variable not set")
            return
            
        sns_client.publish(
            TopicArn=topic_arn,
            Subject='Parcel Ingestion Error',
            Message=json.dumps(detail),
            MessageAttributes={
                'status': {
                    'DataType': 'String',
                    'StringValue': 'error'
                },
                'county': {
                    'DataType': 'String',
                    'StringValue': detail.get('county', 'unknown')
                },
                'ingestion_type': {
                    'DataType': 'String',
                    'StringValue': detail.get('ingestion_type', 'bulk')
                }
            }
        )
        logger.info(f"Published error event for county {detail.get('county')}")
        
    except Exception as e:
        logger.error(f"Error publishing error event: {e}")
