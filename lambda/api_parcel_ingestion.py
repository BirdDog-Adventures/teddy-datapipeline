"""
API Parcel Data Ingestion Lambda Function

This Lambda function handles individual parcel data requests via REST API
with DynamoDB caching, S3 storage, and real-time Snowflake processing.
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
from utils.snowflake_jwt_connector import SnowflakeJWTConnector
from utils.dynamodb_cache import DynamoDBCacheManager

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize clients - get REGRID_API_KEY from secrets
def get_regrid_api_key():
    try:
        secrets_client = boto3.client('secretsmanager')
        secret_name = f'teddy-data-pipeline-secrets-{os.environ.get("ENVIRONMENT", "dev")}'
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secrets = json.loads(response['SecretString'])
        return secrets.get('REGRID_API_KEY', 'test_key_placeholder')
    except Exception as e:
        logger.warning(f"Could not get REGRID_API_KEY from secrets: {e}")
        return os.environ.get('REGRID_API_KEY', 'test_key_placeholder')

regrid_api_key = get_regrid_api_key()
regrid_client = RegridClient(api_key=regrid_api_key)
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')  # Replace EventBridge with SNS
sqs_client = boto3.client('sqs')  # Add SQS client
cache_manager = DynamoDBCacheManager(environment=os.environ.get('ENVIRONMENT', 'dev'))

def lambda_handler(event, context):
    """
    API endpoint for individual parcel data requests
    Supports address, coordinates, and parcel ID lookups
    """
    
    try:
        # Parse request - handle both API Gateway and direct Lambda invocation
        if 'body' in event:
            # API Gateway format: {"body": "{\"type\":\"coordinates\",...}"}
            body = json.loads(event.get('body', '{}'))
        else:
            # Direct Lambda console format: {"type":"coordinates",...}
            body = event
        
        logger.info(f"Parsed request body: {body}")
        query_type = body.get('type')  # 'address', 'coordinates', 'parcel_id'
        
        # Auto-detect query type if not provided
        if not query_type:
            logger.info(f"Auto-detecting query type from body: {body}")
            if body.get('coordinates') and isinstance(body.get('coordinates'), list) and len(body.get('coordinates')) == 2:
                query_type = 'coordinates'
                coords = body['coordinates']
                # Handle both [lat, lon] and [lon, lat] formats
                # Check if first value looks like latitude (between -90 and 90)
                if -90 <= coords[0] <= 90 and abs(coords[1]) > 90:
                    # Format: [latitude, longitude]
                    body['latitude'] = coords[0]
                    body['longitude'] = coords[1]
                elif -90 <= coords[1] <= 90 and abs(coords[0]) > 90:
                    # Format: [longitude, latitude] (GeoJSON standard)
                    body['latitude'] = coords[1]
                    body['longitude'] = coords[0]
                else:
                    # Default assumption: [latitude, longitude]
                    body['latitude'] = coords[0]
                    body['longitude'] = coords[1]
                logger.info(f"Auto-detected coordinates: lat={body['latitude']}, lon={body['longitude']}")
            elif body.get('address') and body.get('address').strip():
                query_type = 'address'
                logger.info("Auto-detected address query")
            elif body.get('parcel_id') and body.get('parcel_id').strip():
                query_type = 'parcel_id'
                logger.info("Auto-detected parcel_id query")
            else:
                logger.warning(f"Unable to auto-detect query type from: {body}")
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing query type or unable to auto-detect from request'})
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
            if not address:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing address parameter'})
                }
            parcel_data = regrid_client.get_parcel_by_address(address)
            
        elif query_type == 'coordinates':
            lat = body.get('latitude')
            lon = body.get('longitude')
            if not lat or not lon:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing latitude or longitude'})
                }
            parcel_data = regrid_client.get_parcel_by_coordinates(lat, lon)
            
        elif query_type == 'parcel_id':
            parcel_id = body.get('parcel_id')
            if not parcel_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'Missing parcel_id parameter'})
                }
            parcel_data = regrid_client.get_parcel_by_id(parcel_id)
        
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid query type'})
            }
        
        if not parcel_data:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Parcel not found'})
            }
        
        # Prepare response
        response_data = {
            'query': body,
            'result': parcel_data,
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'regrid_api'
        }
        
        # Store to S3 for data lake
        s3_key = f"raw/parcel/api/{datetime.now().strftime('%Y-%m-%d')}/{cache_key}.json"
        s3_client.put_object(
            Bucket=os.environ['DATA_BUCKET'],
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
        logger.info("Starting Snowflake connection attempt...")
        try:
            logger.info("Creating SnowflakeConnector instance...")
            with SnowflakeJWTConnector() as snowflake_connector:
                logger.info("SnowflakeConnector created successfully, testing connection...")
                # Test connection first
                test_result = snowflake_connector.test_connection()
                logger.info(f"Snowflake test_connection returned: {test_result}")
                if test_result['status'] == 'success':
                    logger.info(f"✅ Successfully connected to Snowflake for API data: {cache_key}")
                    logger.info(f"Connection info: {test_result['connection_info']}")
                    
                    # Insert data into Snowflake
                    try:
                        county = extract_county_from_parcel_data(parcel_data)
                        state = extract_state_from_parcel_data(parcel_data)
                        
                        rows_inserted = snowflake_connector.insert_raw_data(
                            s3_key=s3_key,
                            county=county, 
                            state=state,
                            data=response_data
                        )
                        logger.info(f"✅ Successfully inserted {rows_inserted} rows to Snowflake for {cache_key}")
                        
                    except Exception as insert_error:
                        logger.error(f"❌ Error inserting to Snowflake: {str(insert_error)}")
                        
                else:
                    logger.warning(f"❌ Failed to connect to Snowflake: {test_result.get('error', 'Unknown error')}")
        except Exception as sf_error:
            logger.error(f"❌ Snowflake loading error: {str(sf_error)}")
            # Continue processing even if Snowflake fails
        
        # Cache result in DynamoDB (24 hour TTL)
        cache_manager.cache_parcel_data(cache_key, response_data, ttl_hours=24)
        
        # Publish event for real-time processing using SNS
        publish_api_event(sns_client, {
            'bucket': os.environ.get('DATA_BUCKET', 'teddy-data-pipeline-dev'),
            's3_key': s3_key,
            'query_type': query_type,
            'cache_key': cache_key,
            'ingestion_type': 'api'
        })
        
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
        
        # Publish error event using SNS
        publish_error_event(sns_client, {
            'error': str(e),
            'request_body': body,
            'ingestion_type': 'api'
        })
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'})
        }

def generate_cache_key(query_type: str, body: Dict[str, Any]) -> str:
    """Generate DynamoDB cache key based on query parameters"""
    if query_type == 'address':
        return f"parcel:address:{hash(body.get('address', ''))}"
    elif query_type == 'coordinates':
        lat = body.get('latitude')
        lon = body.get('longitude')
        return f"parcel:coords:{lat}:{lon}"
    elif query_type == 'parcel_id':
        return f"parcel:id:{body.get('parcel_id')}"
    else:
        return f"parcel:unknown:{hash(str(body))}"

def publish_api_event(sns_client, detail):
    """Publish API event for real-time processing using SNS"""
    try:
        # Use SNS to publish event (will need topic ARN in environment)
        topic_arn = os.environ.get('PARCEL_PROCESSING_TOPIC_ARN')
        if topic_arn:
            sns_client.publish(
                TopicArn=topic_arn,
                Message=json.dumps({
                    'Source': 'birddog.parcel-api',
                    'DetailType': 'Parcel API Request',
                    'Detail': detail
                }),
                Subject='Parcel API Request'
            )
        else:
            logger.info(f"SNS topic not configured, would publish: {detail}")
    except Exception as e:
        logger.error(f"Failed to publish API event via SNS: {e}")

def publish_error_event(sns_client, detail):
    """Publish error event for monitoring using SNS"""
    try:
        # Use SNS to publish error event
        error_topic_arn = os.environ.get('ERROR_TOPIC_ARN')
        if error_topic_arn:
            sns_client.publish(
                TopicArn=error_topic_arn,
                Message=json.dumps({
                    'Source': 'birddog.parcel-api',
                    'DetailType': 'Parcel API Error',
                    'Detail': detail
                }),
                Subject='Parcel API Error'
            )
        else:
            logger.warning(f"Error topic not configured, would publish error: {detail}")
    except Exception as e:
        logger.error(f"Failed to publish error event via SNS: {e}")

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
