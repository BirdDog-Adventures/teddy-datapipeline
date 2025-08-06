import json
import boto3
import logging
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Simple S3 to data processing function
    """
    logger.info(f"S3 event received: {json.dumps(event)}")
    
    results = []
    s3_client = boto3.client('s3')
    
    for record in event.get('Records', []):
        try:
            bucket_name = record['s3']['bucket']['name']
            s3_key = unquote_plus(record['s3']['object']['key'])
            event_name = record['eventName']
            
            logger.info(f"Processing: {event_name} for {bucket_name}/{s3_key}")
            
            # Only process JSON files in parcel directories
            if not s3_key.endswith('.json') or 'parcel' not in s3_key.lower():
                logger.info(f"Skipping non-parcel JSON file: {s3_key}")
                continue
            
            # Download and process the file
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            file_content = response['Body'].read().decode('utf-8')
            
            try:
                json_data = json.loads(file_content)
                record_count = len(json_data) if isinstance(json_data, list) else 1
                logger.info(f"Successfully processed {s3_key}: {record_count} records")
                
                results.append({
                    'status': 'success',
                    's3_key': s3_key,
                    'records': record_count
                })
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {s3_key}: {e}")
                results.append({
                    'status': 'error',
                    's3_key': s3_key,
                    'error': 'Invalid JSON'
                })
                
        except Exception as e:
            logger.error(f"Error processing S3 record: {str(e)}")
            results.append({
                'status': 'error',
                's3_key': s3_key if 's3_key' in locals() else 'unknown',
                'error': str(e)
            })
    
    return {
        'statusCode': 200,
        'body': {
            'processed_files': len(results),
            'results': results
        }
    }
