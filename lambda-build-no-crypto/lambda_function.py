"""
API Parcel Data Ingestion Lambda Function - Fixed Version

This Lambda function handles individual parcel data requests via REST API
with auto-detection of query type and all dependencies resolved.
"""

import json
import boto3
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import sys
sys.path.append('/opt')
from clients.regrid import RegridClient
from utils.snowflake_connector import SnowflakeConnector
from utils.dynamodb_cache import DynamoDBCacheManager

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients with hardcoded values
regrid_client = RegridClient(api_key='eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJyZWdyaWQuY29tIiwiaWF0IjoxNzU0NTA0ODkyLCJleHAiOjE3ODYwNjE4NDQsImciOjkyMTkzLCJ0IjoxLCJjYXAiOiJwYTp0cyIsInRpIjo2OTIsInRucCI6MX0.AmqtzuOOaCaJ4TSRaR2CUj2_J2ln1Ugyn4iH-IRsSok')
s3_client = boto3.client('s3')
cache_manager = DynamoDBCacheManager(environment='dev')

def lambda_handler(event, context):
    """
    API endpoint for individual parcel data requests
    Auto-detects query type from request body
    """
    
    try:
        # Debug the incoming event
        logger.info(f"DEBUG: Full event received: {json.dumps(event)}")
        
        # Parse request body with multiple fallback methods
        body = {}
        
        # Method 1: Standard API Gateway format
        if 'body' in event and event['body']:
            try:
                if isinstance(event['body'], str):
                    body = json.loads(event['body'])
                    logger.info(f"DEBUG: Parsed body from string: {body}")
                else:
                    body = event['body']
                    logger.info(f"DEBUG: Used body directly: {body}")
            except json.JSONDecodeError as e:
                logger.error(f"DEBUG: JSON decode error: {e}")
                logger.info(f"DEBUG: Raw body content: {event['body']}")
        
        # Method 2: Direct event format (for testing)
        if not body and any(key in event for key in ['address', 'parcel_id', 'coordinates', 'latitude', 'longitude']):
            body = {k: v for k, v in event.items() if k in ['address', 'parcel_id', 'coordinates', 'latitude', 'longitude', 'lat', 'lon', 'lng', 'type']}
            logger.info(f"DEBUG: Extracted body from event keys: {body}")
        
        # Method 3: Check if event itself is the body
        if not body and 'httpMethod' not in event and 'requestContext' not in event:
            body = event
            logger.info(f"DEBUG: Using entire event as body: {body}")
        
        logger.info(f"DEBUG: Final parsed body: {body}")
        
        # Auto-detect query type
        query_type = detect_query_type(body)
        
        if not query_type:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Unable to determine query type. Please provide address, coordinates (latitude/longitude), or parcel_id'})
            }
        
        # Generate cache key
        cache_key = generate_cache_key(query_type, body)
        
        # Check DynamoDB cache first
        cached_result = cache_manager.get_cached_parcel(cache_key)
        if cached_result:
            logger.info(f"Cache hit for key: {cache_key}")
            return {
                'statusCode': 200,
                'body': json.dumps(cached_result),
                'headers': {
                    'Content-Type': 'application/json',
                    'X-Cache': 'HIT'
                }
            }
        
        # Fetch from Regrid API
        parcel_data = None
        
        if query_type == 'address':
            address = body.get('address')
            parcel_data = regrid_client.get_parcel_by_address(address)
            
        elif query_type == 'coordinates':
            lat = body.get('latitude') or body.get('lat')
            lon = body.get('longitude') or body.get('lon') or body.get('lng')
            # Handle coordinates array format
            if not lat and not lon and body.get('coordinates'):
                coords = body.get('coordinates')
                if isinstance(coords, list) and len(coords) >= 2:
                    lat, lon = coords[0], coords[1]
            parcel_data = regrid_client.get_parcel_by_coordinates(lat, lon)
            
        elif query_type == 'parcel_id':
            parcel_id = body.get('parcel_id')
            parcel_data = regrid_client.get_parcel_by_id(parcel_id)
        
        if not parcel_data:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Parcel not found'})
            }
        
        # Prepare response
        response_data = {
            'query': body,
            'query_type': query_type,
            'result': parcel_data,
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'regrid_api'
        }
        
        # Store to S3 for data lake
        s3_key = f"raw/parcel/api/{datetime.now().strftime('%Y-%m-%d')}/{cache_key}.json"
        s3_client.put_object(
            Bucket='teddy-data-pipeline-bucket-dev',
            Key=s3_key,
            Body=json.dumps(response_data),
            ContentType='application/json',
            Metadata={
                'query_type': query_type,
                'ingestion_type': 'api',
                'cache_key': cache_key
            }
        )
        
        # Load data to Snowflake in real-time
        try:
            snowflake_connector = SnowflakeConnector()
            logger.info(f"Snowflake connector: {snowflake_connector}")
            
            # Debug: Show Snowflake parameters being used
            if hasattr(snowflake_connector, 'secrets') and snowflake_connector.secrets:
                logger.info("🔍 Snowflake Connection Parameters:")
                logger.info(f"   • Account: {snowflake_connector.secrets.get('snowflake_account', 'NOT_SET')}")
                logger.info(f"   • Environment: {snowflake_connector.environment}")
                logger.info(f"   • User: {snowflake_connector.secrets.get('snowflake_user', 'NOT_SET')}")
                logger.info(f"   • Password: {snowflake_connector.secrets.get('snowflake_password', 'NOT_SET')}")
                logger.info(f"   • Base URL: {getattr(snowflake_connector, 'base_url', 'NOT_SET')}")
                logger.info(f"   • Available secrets: {list(snowflake_connector.secrets.keys()) if snowflake_connector.secrets else 'None'}")
            else:
                logger.warning("⚠️  Snowflake secrets not loaded")
            
            if snowflake_connector.connect():
                # Extract county and state from parcel data
                county = extract_county_from_parcel_data(parcel_data)
                state = extract_state_from_parcel_data(parcel_data)
                
                # Insert raw data
                snowflake_connector.insert_raw_data(s3_key, county, state, response_data)
                
                # Insert staging data
                if isinstance(parcel_data, list) and len(parcel_data) > 0:
                    snowflake_connector.insert_staging_data(county, state, parcel_data)
                elif isinstance(parcel_data, dict):
                    snowflake_connector.insert_staging_data(county, state, [parcel_data])
                
                snowflake_connector.close()
                logger.info(f"Successfully loaded API data to Snowflake: {cache_key}")
            else:
                logger.warning("Failed to connect to Snowflake for real-time loading")
        except Exception as sf_error:
            logger.error(f"Snowflake loading error: {str(sf_error)}")
            # Continue processing even if Snowflake fails
        
        # Cache result in DynamoDB (24 hour TTL)
        try:
            cache_manager.cache_parcel_data(cache_key, response_data, ttl_hours=24)
            logger.info(f"Successfully cached result: {cache_key}")
        except Exception as cache_error:
            logger.warning(f"Failed to cache result: {str(cache_error)}")
            # Continue processing even if caching fails
        
        return {
            'statusCode': 200,
            'body': json.dumps(response_data),
            'headers': {
                'Content-Type': 'application/json',
                'X-Cache': 'MISS'
            }
        }
        
    except Exception as e:
        logger.error(f"Error in API ingestion: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }

def detect_query_type(body: Dict[str, Any]) -> Optional[str]:
    """Auto-detect query type from request body - SIMPLE VERSION"""
    
    logger.info(f"DEBUG: Detecting query type for body: {body}")
    
    # Check for explicit type field first
    if body.get('type'):
        logger.info(f"DEBUG: Found explicit type: {body.get('type')}")
        return body.get('type')
    
    # SIMPLE: If address field exists and has any content, use it
    if 'address' in body:
        address = body.get('address')
        logger.info(f"DEBUG: Found address field: '{address}'")
        if address:  # Any non-None value
            logger.info("DEBUG: Returning 'address' type")
            return 'address'
    
    # SIMPLE: If parcel_id field exists and has content, use it
    if 'parcel_id' in body:
        parcel_id = body.get('parcel_id')
        logger.info(f"DEBUG: Found parcel_id field: '{parcel_id}'")
        if parcel_id:  # Any non-None, non-empty value
            logger.info("DEBUG: Returning 'parcel_id' type")
            return 'parcel_id'
    
    # SIMPLE: Check for coordinates
    if (body.get('latitude') and body.get('longitude')) or \
       (body.get('lat') and body.get('lon')):
        logger.info("DEBUG: Found lat/lon coordinates")
        return 'coordinates'
    
    # Check for coordinates array with actual values
    coords = body.get('coordinates')
    if coords and isinstance(coords, list) and len(coords) >= 2:
        if coords[0] and coords[1]:  # Both values exist
            logger.info("DEBUG: Found coordinates array with values")
            return 'coordinates'
    
    # FALLBACK: If we have any of these fields, try them even if empty
    if 'address' in body:
        logger.info("DEBUG: Fallback to address (field exists)")
        return 'address'
    
    if 'parcel_id' in body:
        logger.info("DEBUG: Fallback to parcel_id (field exists)")
        return 'parcel_id'
    
    if 'coordinates' in body:
        logger.info("DEBUG: Fallback to coordinates (field exists)")
        return 'coordinates'
    
    logger.error(f"DEBUG: Could not detect query type for body: {body}")
    return None

def generate_cache_key(query_type: str, body: Dict[str, Any]) -> str:
    """Generate DynamoDB cache key based on query parameters"""
    if query_type == 'address':
        return f"parcel:address:{hash(body.get('address', ''))}"
    elif query_type == 'coordinates':
        lat = body.get('latitude') or body.get('lat')
        lon = body.get('longitude') or body.get('lon') or body.get('lng')
        if not lat and not lon and body.get('coordinates'):
            coords = body.get('coordinates')
            if isinstance(coords, list) and len(coords) >= 2:
                lat, lon = coords[0], coords[1]
        return f"parcel:coords:{lat}:{lon}"
    elif query_type == 'parcel_id':
        return f"parcel:id:{body.get('parcel_id')}"
    else:
        return f"parcel:unknown:{hash(str(body))}"

# EventBridge functions removed - using SQS/SNS instead
# These functions are no longer needed as we removed EventBridge dependencies

def extract_county_from_parcel_data(parcel_data):
    """Extract county from parcel data"""
    if isinstance(parcel_data, list) and len(parcel_data) > 0:
        parcel = parcel_data[0]
    elif isinstance(parcel_data, dict):
        parcel = parcel_data
    else:
        return 'unknown'
    
    # Try various field names for county
    county_fields = ['county', 'county_name', 'admin_county', 'jurisdiction']
    for field in county_fields:
        if field in parcel and parcel[field]:
            return str(parcel[field]).lower()
    
    # Try to extract from address
    if 'address' in parcel and parcel['address']:
        address = str(parcel['address']).lower()
        # Look for common county patterns
        if 'county' in address:
            parts = address.split()
            for i, part in enumerate(parts):
                if part == 'county' and i > 0:
                    return parts[i-1]
    
    return 'unknown'

def extract_state_from_parcel_data(parcel_data):
    """Extract state from parcel data"""
    if isinstance(parcel_data, list) and len(parcel_data) > 0:
        parcel = parcel_data[0]
    elif isinstance(parcel_data, dict):
        parcel = parcel_data
    else:
        return 'unknown'
    
    # Try various field names for state
    state_fields = ['state', 'state_code', 'admin_state', 'region']
    for field in state_fields:
        if field in parcel and parcel[field]:
            state_value = str(parcel[field]).lower()
            # Convert state codes to full names
            if state_value == 'tx':
                return 'texas'
            elif state_value == 'ca':
                return 'california'
            elif state_value == 'fl':
                return 'florida'
            else:
                return state_value
    
    # Try to extract from address
    if 'address' in parcel and parcel['address']:
        address = str(parcel['address']).lower()
        if 'tx' in address or 'texas' in address:
            return 'texas'
        elif 'ca' in address or 'california' in address:
            return 'california'
        elif 'fl' in address or 'florida' in address:
            return 'florida'
    
    return 'unknown'
