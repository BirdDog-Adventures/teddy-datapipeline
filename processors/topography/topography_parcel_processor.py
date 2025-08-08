"""
Topography Parcel Processor Lambda Function

This Lambda function processes USGS topography/elevation data for individual parcels
on-demand. It can be triggered via API Gateway, direct invocation, SNS events, or batch processing.

Environment Variables:
- ENVIRONMENT: dev/prod (default: dev)  
- FORCE_REFRESH: Force refresh even if recent data exists (default: false)
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional

# Import our topography integration
from topography_client import get_topography_integration

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for topography parcel processing
    
    Expected event formats:
    
    1. API Gateway (GET /parcels/{parcel_id}/topography):
    {
        "pathParameters": {"parcel_id": "12345"},
        "queryStringParameters": {"force_refresh": "false"}
    }
    
    2. Direct invocation:
    {
        "parcel_id": "12345", 
        "force_refresh": false
    }
    
    3. Batch processing:
    {
        "parcel_ids": ["12345", "67890"],
        "force_refresh": false
    }
    
    4. SNS event from parcel ingestion:
    {
        "Records": [{
            "Sns": {
                "Message": "{\"Source\":\"birddog.parcel-api\",\"Detail\":{...}}"
            }
        }]
    }
    """
    
    try:
        logger.info(f"Processing topography request: {json.dumps(event)}")
        
        # Parse input parameters
        params = parse_event_parameters(event)
        
        if not params:
            return create_error_response(400, "Invalid request parameters")
        
        # Get environment configuration
        environment = os.environ.get('ENVIRONMENT', 'dev')
        default_force_refresh = os.environ.get('FORCE_REFRESH', 'false').lower() == 'true'
        
        # Initialize topography integration
        topo_integration = get_topography_integration(environment)
        
        # Process single parcel or batch
        if 'parcel_id' in params:
            # Single parcel processing
            result = process_single_parcel(
                topo_integration,
                params['parcel_id'],
                params.get('force_refresh', default_force_refresh)
            )
            
            return create_success_response(result)
            
        elif 'parcel_ids' in params:
            # Batch processing
            results = process_parcel_batch(
                topo_integration,
                params['parcel_ids'],
                params.get('force_refresh', default_force_refresh)
            )
            
            return create_success_response(results)
            
        else:
            return create_error_response(400, "Missing parcel_id or parcel_ids parameter")
    
    except Exception as e:
        logger.error(f"Error processing topography request: {str(e)}", exc_info=True)
        return create_error_response(500, f"Internal server error: {str(e)}")

def parse_event_parameters(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse parameters from different event sources
    
    Args:
        event: Lambda event dictionary
        
    Returns:
        Parsed parameters or None if invalid
    """
    
    try:
        # API Gateway format
        if 'pathParameters' in event:
            params = {}
            
            # Get parcel_id from path
            if event.get('pathParameters') and 'parcel_id' in event['pathParameters']:
                params['parcel_id'] = event['pathParameters']['parcel_id']
            
            # Get query parameters
            query_params = event.get('queryStringParameters') or {}
            
            if 'force_refresh' in query_params:
                params['force_refresh'] = query_params['force_refresh'].lower() == 'true'
            
            return params
        
        # Direct invocation format
        elif 'parcel_id' in event or 'parcel_ids' in event:
            params = {}
            
            if 'parcel_id' in event:
                params['parcel_id'] = event['parcel_id']
            
            if 'parcel_ids' in event:
                params['parcel_ids'] = event['parcel_ids']
            
            if 'force_refresh' in event:
                params['force_refresh'] = bool(event['force_refresh'])
            
            return params
        
        # SQS/SNS format (body contains JSON)
        elif 'Records' in event:
            # Handle SQS/SNS messages
            for record in event['Records']:
                if 'body' in record:
                    body = json.loads(record['body'])
                    return parse_event_parameters(body)
                elif 'Sns' in record and 'Message' in record['Sns']:
                    message = json.loads(record['Sns']['Message'])
                    return parse_event_parameters(message)
        
        # Handle parcel ingestion event format from SNS
        elif 'Source' in event and event.get('Source') == 'birddog.parcel-api':
            detail = event.get('Detail', {})
            if detail.get('s3_key'):
                # Extract parcel_id from s3_key or cache_key
                cache_key = detail.get('cache_key', '')
                parcel_id = extract_parcel_id_from_cache_key(cache_key)
                if parcel_id:
                    return {
                        'parcel_id': parcel_id,
                        's3_key': detail.get('s3_key'),
                        'bucket': detail.get('bucket'),
                        'query_type': detail.get('query_type'),
                        'ingestion_type': detail.get('ingestion_type')
                    }
        
        # Handle direct SNS message format
        elif 'Detail' in event:
            detail = event.get('Detail', {})
            if detail.get('s3_key'):
                cache_key = detail.get('cache_key', '')
                parcel_id = extract_parcel_id_from_cache_key(cache_key)
                if parcel_id:
                    return {
                        'parcel_id': parcel_id,
                        's3_key': detail.get('s3_key'),
                        'bucket': detail.get('bucket'),
                        'query_type': detail.get('query_type'),
                        'ingestion_type': detail.get('ingestion_type')
                    }
        
        return None
        
    except Exception as e:
        logger.error(f"Error parsing event parameters: {e}")
        return None

def extract_parcel_id_from_cache_key(cache_key: str) -> Optional[str]:
    """
    Extract parcel_id from cache key formats like:
    - parcel:coords:35.0135:-80.7434
    - parcel:address:hash_value
    - parcel:id:actual_parcel_id
    
    Args:
        cache_key: Cache key string
        
    Returns:
        Extracted parcel_id or generated identifier
    """
    
    if not cache_key:
        return None
        
    try:
        # Handle different cache key formats
        if cache_key.startswith('parcel:coords:'):
            # Format: parcel:coords:lat:lon
            parts = cache_key.split(':')
            if len(parts) >= 4:
                lat, lon = parts[2], parts[3]
                return f"coords_{lat}_{lon}"
                
        elif cache_key.startswith('parcel:address:'):
            # Format: parcel:address:hash_value
            parts = cache_key.split(':')
            if len(parts) >= 3:
                return f"address_{parts[2]}"
                
        elif cache_key.startswith('parcel:id:'):
            # Format: parcel:id:actual_parcel_id
            parts = cache_key.split(':', 2)  # Split only on first 2 colons
            if len(parts) >= 3:
                return parts[2]
        
        # Fallback: use entire cache key as identifier
        return cache_key.replace(':', '_')
        
    except Exception as e:
        logger.error(f"Error extracting parcel_id from cache_key {cache_key}: {e}")
        return cache_key.replace(':', '_') if cache_key else None

def process_single_parcel(topo_integration, parcel_id: str, force_refresh: bool) -> Dict[str, Any]:
    """
    Process topography data for a single parcel
    
    Args:
        topo_integration: Topography integration instance
        parcel_id: Parcel identifier
        force_refresh: Force refresh flag
        
    Returns:
        Processing result dictionary
    """
    
    logger.info(f"Processing single parcel: {parcel_id}, force_refresh: {force_refresh}")
    
    # Process the parcel
    result = topo_integration.process_parcel_topography(
        parcel_id=parcel_id,
        force_refresh=force_refresh
    )
    
    if result['success']:
        # Get detailed JSON data
        detailed_data = topo_integration.get_parcel_topography_json(parcel_id)
        
        return {
            'parcel_id': parcel_id,
            'success': True,
            'source': result['source'],
            'processing_method': result.get('processing_method'),
            'topography_data': result['data'],
            'detailed_data': detailed_data
        }
    else:
        return {
            'parcel_id': parcel_id,
            'success': False,
            'error': result['error']
        }

def process_parcel_batch(topo_integration, parcel_ids: list, force_refresh: bool) -> Dict[str, Any]:
    """
    Process topography data for a batch of parcels
    
    Args:
        topo_integration: Topography integration instance
        parcel_ids: List of parcel identifiers
        force_refresh: Force refresh flag
        
    Returns:
        Batch processing results
    """
    
    logger.info(f"Processing batch of {len(parcel_ids)} parcels, force_refresh: {force_refresh}")
    
    results = []
    successful = 0
    failed = 0
    
    for parcel_id in parcel_ids:
        try:
            result = process_single_parcel(topo_integration, parcel_id, force_refresh)
            results.append(result)
            
            if result['success']:
                successful += 1
            else:
                failed += 1
                
        except Exception as e:
            logger.error(f"Error processing parcel {parcel_id}: {e}")
            results.append({
                'parcel_id': parcel_id,
                'success': False,
                'error': str(e)
            })
            failed += 1
    
    return {
        'batch_summary': {
            'total_parcels': len(parcel_ids),
            'successful': successful,
            'failed': failed
        },
        'results': results
    }

def create_success_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a successful Lambda response
    
    Args:
        data: Response data
        
    Returns:
        Lambda response dictionary
    """
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
        },
        'body': json.dumps(data, default=str)
    }

def create_error_response(status_code: int, message: str) -> Dict[str, Any]:
    """
    Create an error Lambda response
    
    Args:
        status_code: HTTP status code
        message: Error message
        
    Returns:
        Lambda error response dictionary
    """
    
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
        },
        'body': json.dumps({
            'error': message,
            'statusCode': status_code
        })
    }

# Health check endpoint
def health_check() -> Dict[str, Any]:
    """
    Health check for the Lambda function
    
    Returns:
        Health status response
    """
    
    try:
        # Test Snowflake connection
        environment = os.environ.get('ENVIRONMENT', 'dev')
        topo_integration = get_topography_integration(environment)
        
        # Simple connection test (this will test Snowflake connectivity)
        test_result = topo_integration.check_existing_topography('health_check_test')
        
        return {
            'status': 'healthy',
            'environment': environment,
            'snowflake_connection': 'ok',
            'timestamp': str(datetime.now())
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': str(datetime.now())
        }

# Example usage and testing
if __name__ == "__main__":
    # Test events for local development
    
    # Test single parcel
    test_event_single = {
        "parcel_id": "test_parcel_001",
        "force_refresh": False
    }
    
    # Test batch processing
    test_event_batch = {
        "parcel_ids": ["test_parcel_001", "test_parcel_002"],
        "force_refresh": False
    }
    
    # Test API Gateway format
    test_event_api = {
        "pathParameters": {
            "parcel_id": "test_parcel_001"
        },
        "queryStringParameters": {
            "force_refresh": "false"
        }
    }
    
    # Mock context
    class MockContext:
        def __init__(self):
            self.function_name = "topography-parcel-processor"
            self.memory_limit_in_mb = 512
            self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:topography-parcel-processor"
            self.aws_request_id = "test-request-id"
    
    context = MockContext()
    
    print("Testing single parcel processing...")
    result = lambda_handler(test_event_single, context)
    print(f"Result: {json.dumps(result, indent=2)}")
    
    print("\nTesting API Gateway format...")
    result = lambda_handler(test_event_api, context)
    print(f"Result: {json.dumps(result, indent=2)}")