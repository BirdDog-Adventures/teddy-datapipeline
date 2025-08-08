"""
Unified NLCD Processor for Teddy Data Pipeline

This module provides a unified interface for NLCD land cover processing
that integrates seamlessly with existing Lambda functions and workflows.

It supports:
- Single parcel processing (API calls)
- Batch processing (SQS messages)
- State-level bulk processing
- Event-driven enrichment
- Configuration-based processing
"""

import json
import logging
import os
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

from .nlcd_client import get_nlcd_integration
from .snowflake_connector import get_snowflake_connector

# Configure logging
logger = logging.getLogger(__name__)

# Processing configuration
PROCESSING_CONFIG = {
    'nlcd': {
        'enabled': True,
        'auto_enrich': {
            's3_ingestion': True,      # Auto-enrich S3 ingested parcels
            'api_ingestion': False,    # Manual trigger for API
            'bulk_ingestion': True     # Auto-enrich bulk ingested parcels
        },
        'batch_size': 100,
        'rate_limit': 1.0,
        'max_age_days': 30,
        'max_retries': 3,
        'sqs_queue_url': os.environ.get('NLCD_PROCESSING_QUEUE_URL'),
        'sns_topic_arn': os.environ.get('NLCD_ENRICHMENT_TOPIC_ARN')
    }
}

class UnifiedNLCDProcessor:
    """
    Unified processor for all NLCD land cover processing requests
    """
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.environ.get('ENVIRONMENT', 'dev')
        self.nlcd_client = get_nlcd_integration(self.environment)
        
        # AWS clients
        self.sns_client = boto3.client('sns') if PROCESSING_CONFIG['nlcd']['sns_topic_arn'] else None
        self.sqs_client = boto3.client('sqs') if PROCESSING_CONFIG['nlcd']['sqs_queue_url'] else None
        
        logger.info(f"Initialized UnifiedNLCDProcessor for environment: {self.environment}")
    
    def process_request(self, event: Dict, context: Any = None) -> Dict:
        """
        Main entry point for all NLCD processing requests
        
        Args:
            event: Lambda event dictionary
            context: Lambda context object
            
        Returns:
            Processing result dictionary
        """
        
        try:
            request_type = self.determine_request_type(event)
            logger.info(f"Processing NLCD request type: {request_type}")
            
            if request_type == 'single_parcel':
                return self.process_single_parcel(event)
            elif request_type == 'batch_parcels':
                return self.process_batch_parcels(event)
            elif request_type == 'state_bulk':
                return self.process_state_bulk(event)
            elif request_type == 'enrichment_trigger':
                return self.process_enrichment_trigger(event)
            else:
                raise ValueError(f"Unknown request type: {request_type}")
                
        except Exception as e:
            logger.error(f"Error processing NLCD request: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'request_type': request_type if 'request_type' in locals() else 'unknown'
            }
    
    def determine_request_type(self, event: Dict) -> str:
        """
        Determine the type of processing request based on event structure
        
        Args:
            event: Lambda event dictionary
            
        Returns:
            Request type string
        """
        
        # Direct single parcel request
        if 'parcel_id' in event and isinstance(event['parcel_id'], str):
            return 'single_parcel'
        
        # Batch parcel request
        if 'parcel_ids' in event and isinstance(event['parcel_ids'], list):
            return 'batch_parcels'
        
        # State bulk processing request
        if 'state_code' in event or event.get('action') == 'state_bulk_nlcd':
            return 'state_bulk'
        
        # SQS message with enrichment request
        if 'Records' in event:
            for record in event['Records']:
                if record.get('eventSource') == 'aws:sqs':
                    try:
                        body = json.loads(record['body'])
                        if body.get('action') == 'nlcd_enrichment':
                            return 'enrichment_trigger'
                    except json.JSONDecodeError:
                        pass
        
        # Enrichment trigger from other Lambda functions
        if event.get('action') == 'nlcd_enrichment' or 'enrich_nlcd' in event:
            return 'enrichment_trigger'
        
        # Default to single parcel if unclear
        return 'single_parcel'
    
    def process_single_parcel(self, event: Dict) -> Dict:
        """
        Handle single parcel processing (API calls)
        
        Args:
            event: Event containing parcel_id and optional parameters
            
        Returns:
            Processing result dictionary
        """
        
        parcel_id = event.get('parcel_id')
        year = event.get('year', event.get('nlcd_year', 2021))
        force_refresh = event.get('force_refresh', False)
        
        if not parcel_id:
            return {
                'success': False,
                'error': 'Missing parcel_id parameter'
            }
        
        logger.info(f"Processing single parcel: {parcel_id}, year: {year}")
        
        try:
            result = self.nlcd_client.process_parcel_land_cover(
                parcel_id=parcel_id,
                year=year,
                force_refresh=force_refresh
            )
            
            # Add detailed data if requested
            if event.get('include_details', True):
                detailed_data = self.nlcd_client.get_parcel_land_cover_json(parcel_id)
                result['detailed_data'] = detailed_data
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing single parcel {parcel_id}: {str(e)}")
            return {
                'success': False,
                'parcel_id': parcel_id,
                'error': str(e)
            }
    
    def process_batch_parcels(self, event: Dict) -> Dict:
        """
        Handle batch processing (SQS messages, bulk requests)
        
        Args:
            event: Event containing parcel_ids and optional parameters
            
        Returns:
            Batch processing results
        """
        
        parcel_ids = event.get('parcel_ids', [])
        year = event.get('year', event.get('nlcd_year', 2021))
        batch_size = event.get('batch_size', PROCESSING_CONFIG['nlcd']['batch_size'])
        
        if not parcel_ids:
            return {
                'success': False,
                'error': 'Missing parcel_ids parameter'
            }
        
        logger.info(f"Processing batch of {len(parcel_ids)} parcels, year: {year}")
        
        results = []
        successful = 0
        failed = 0
        
        # Process in smaller batches if needed
        for i in range(0, len(parcel_ids), batch_size):
            batch = parcel_ids[i:i + batch_size]
            
            for parcel_id in batch:
                try:
                    result = self.nlcd_client.process_parcel_land_cover(
                        parcel_id=parcel_id,
                        year=year,
                        force_refresh=False  # Don't force refresh in batch
                    )
                    
                    results.append(result)
                    
                    if result['success']:
                        successful += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    logger.error(f"Error processing parcel {parcel_id}: {str(e)}")
                    results.append({
                        'success': False,
                        'parcel_id': parcel_id,
                        'error': str(e)
                    })
                    failed += 1
        
        return {
            'success': True,
            'batch_summary': {
                'total_parcels': len(parcel_ids),
                'successful': successful,
                'failed': failed,
                'year': year
            },
            'results': results
        }
    
    def process_state_bulk(self, event: Dict) -> Dict:
        """
        Handle state-level bulk processing by creating batch jobs
        
        Args:
            event: Event containing state_code and processing parameters
            
        Returns:
            Bulk processing job creation result
        """
        
        state_code = event.get('state_code')
        batch_size = event.get('batch_size', PROCESSING_CONFIG['nlcd']['batch_size'])
        year = event.get('year', 2021)
        
        if not state_code:
            return {
                'success': False,
                'error': 'Missing state_code parameter'
            }
        
        logger.info(f"Creating bulk processing jobs for state: {state_code}")
        
        try:
            # Get parcels for the state
            parcels = self.get_parcels_for_state(state_code)
            
            if not parcels:
                return {
                    'success': False,
                    'error': f'No parcels found for state {state_code}'
                }
            
            # Create batch jobs
            job_results = self.create_batch_jobs_for_state(parcels, state_code, batch_size, year)
            
            return {
                'success': True,
                'state_code': state_code,
                'total_parcels': len(parcels),
                'batch_jobs_created': len(job_results),
                'job_details': job_results
            }
            
        except Exception as e:
            logger.error(f"Error creating bulk jobs for state {state_code}: {str(e)}")
            return {
                'success': False,
                'state_code': state_code,
                'error': str(e)
            }
    
    def process_enrichment_trigger(self, event: Dict) -> Dict:
        """
        Handle enrichment triggers from other Lambda functions
        
        Args:
            event: Event containing enrichment request
            
        Returns:
            Enrichment processing result
        """
        
        # Extract parcel IDs from various event formats
        parcel_ids = []
        
        if 'Records' in event:
            # SQS message format
            for record in event['Records']:
                try:
                    body = json.loads(record['body'])
                    if 'parcel_ids' in body:
                        parcel_ids.extend(body['parcel_ids'])
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse SQS message body: {record.get('body', '')}")
        
        elif 'parcel_ids' in event:
            # Direct format
            parcel_ids = event['parcel_ids']
        
        elif 'parcel_id' in event:
            # Single parcel format
            parcel_ids = [event['parcel_id']]
        
        if not parcel_ids:
            return {
                'success': False,
                'error': 'No parcel IDs found in enrichment trigger'
            }
        
        # Process as batch
        batch_event = {
            'parcel_ids': parcel_ids,
            'year': event.get('year', event.get('nlcd_year', 2021)),
            'batch_size': event.get('batch_size', PROCESSING_CONFIG['nlcd']['batch_size'])
        }
        
        return self.process_batch_parcels(batch_event)
    
    def get_parcels_for_state(self, state_code: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Get parcels for a specific state from Snowflake
        
        Args:
            state_code: Two-letter state code
            limit: Optional limit for testing
            
        Returns:
            List of parcel dictionaries
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                query = """
                    SELECT 
                        PARCEL_ID,
                        STATE,
                        COUNTY,
                        LATITUDE,
                        LONGITUDE,
                        ACRES
                    FROM TEDDY_DATA.RAW.PARCEL_PROFILES 
                    WHERE STATE = %s
                    AND LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                results = sf.execute_query(query, {'1': state_code})
                return results
                
        except Exception as e:
            logger.error(f"Error getting parcels for state {state_code}: {e}")
            return []
    
    def create_batch_jobs_for_state(self, parcels: List[Dict], state_code: str, 
                                  batch_size: int, year: int) -> List[Dict]:
        """
        Create batch processing jobs for a state's parcels
        
        Args:
            parcels: List of parcel dictionaries
            state_code: State code
            batch_size: Size of each batch
            year: NLCD year
            
        Returns:
            List of created job details
        """
        
        job_results = []
        
        # Split parcels into batches
        for i in range(0, len(parcels), batch_size):
            batch = parcels[i:i + batch_size]
            batch_parcel_ids = [p['PARCEL_ID'] for p in batch]
            
            # Create batch job message
            job_message = {
                'action': 'nlcd_enrichment',
                'parcel_ids': batch_parcel_ids,
                'state_code': state_code,
                'year': year,
                'batch_number': (i // batch_size) + 1,
                'total_batches': (len(parcels) + batch_size - 1) // batch_size,
                'created_at': datetime.now().isoformat()
            }
            
            # Send to SQS queue for processing
            if self.sqs_client and PROCESSING_CONFIG['nlcd']['sqs_queue_url']:
                try:
                    response = self.sqs_client.send_message(
                        QueueUrl=PROCESSING_CONFIG['nlcd']['sqs_queue_url'],
                        MessageBody=json.dumps(job_message),
                        MessageAttributes={
                            'state_code': {
                                'StringValue': state_code,
                                'DataType': 'String'
                            },
                            'batch_size': {
                                'StringValue': str(len(batch_parcel_ids)),
                                'DataType': 'Number'
                            }
                        }
                    )
                    
                    job_results.append({
                        'batch_number': job_message['batch_number'],
                        'parcel_count': len(batch_parcel_ids),
                        'message_id': response['MessageId'],
                        'status': 'queued'
                    })
                    
                except Exception as e:
                    logger.error(f"Error sending batch job to SQS: {str(e)}")
                    job_results.append({
                        'batch_number': job_message['batch_number'],
                        'parcel_count': len(batch_parcel_ids),
                        'status': 'failed',
                        'error': str(e)
                    })
            else:
                # Process directly if no SQS queue configured
                try:
                    batch_result = self.process_batch_parcels({
                        'parcel_ids': batch_parcel_ids,
                        'year': year
                    })
                    
                    job_results.append({
                        'batch_number': job_message['batch_number'],
                        'parcel_count': len(batch_parcel_ids),
                        'status': 'completed',
                        'successful': batch_result['batch_summary']['successful'],
                        'failed': batch_result['batch_summary']['failed']
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing batch directly: {str(e)}")
                    job_results.append({
                        'batch_number': job_message['batch_number'],
                        'parcel_count': len(batch_parcel_ids),
                        'status': 'failed',
                        'error': str(e)
                    })
        
        return job_results
    
    def should_auto_enrich(self, source: str) -> bool:
        """
        Check if auto-enrichment is enabled for a specific source
        
        Args:
            source: Source type ('s3_ingestion', 'api_ingestion', 'bulk_ingestion')
            
        Returns:
            True if auto-enrichment is enabled
        """
        
        if not PROCESSING_CONFIG['nlcd']['enabled']:
            return False
        
        return PROCESSING_CONFIG['nlcd']['auto_enrich'].get(source, False)
    
    def trigger_enrichment(self, parcel_ids: List[str], source: str, 
                          additional_params: Dict = None) -> Dict:
        """
        Trigger NLCD enrichment for a list of parcels
        
        Args:
            parcel_ids: List of parcel IDs to enrich
            source: Source of the enrichment request
            additional_params: Additional parameters for processing
            
        Returns:
            Trigger result dictionary
        """
        
        if not self.should_auto_enrich(source):
            return {
                'success': False,
                'message': f'Auto-enrichment not enabled for source: {source}'
            }
        
        message = {
            'action': 'nlcd_enrichment',
            'parcel_ids': parcel_ids,
            'source': source,
            'timestamp': datetime.now().isoformat()
        }
        
        if additional_params:
            message.update(additional_params)
        
        # Send to SNS topic if configured
        if self.sns_client and PROCESSING_CONFIG['nlcd']['sns_topic_arn']:
            try:
                response = self.sns_client.publish(
                    TopicArn=PROCESSING_CONFIG['nlcd']['sns_topic_arn'],
                    Message=json.dumps(message),
                    Subject=f'NLCD Enrichment Request - {source}',
                    MessageAttributes={
                        'source': {
                            'StringValue': source,
                            'DataType': 'String'
                        },
                        'parcel_count': {
                            'StringValue': str(len(parcel_ids)),
                            'DataType': 'Number'
                        }
                    }
                )
                
                return {
                    'success': True,
                    'message_id': response['MessageId'],
                    'parcel_count': len(parcel_ids),
                    'source': source
                }
                
            except Exception as e:
                logger.error(f"Error publishing to SNS: {str(e)}")
                return {
                    'success': False,
                    'error': str(e)
                }
        
        # Send to SQS queue if configured
        elif self.sqs_client and PROCESSING_CONFIG['nlcd']['sqs_queue_url']:
            try:
                response = self.sqs_client.send_message(
                    QueueUrl=PROCESSING_CONFIG['nlcd']['sqs_queue_url'],
                    MessageBody=json.dumps(message)
                )
                
                return {
                    'success': True,
                    'message_id': response['MessageId'],
                    'parcel_count': len(parcel_ids),
                    'source': source
                }
                
            except Exception as e:
                logger.error(f"Error sending to SQS: {str(e)}")
                return {
                    'success': False,
                    'error': str(e)
                }
        
        else:
            # Process directly if no messaging configured
            try:
                result = self.process_batch_parcels({
                    'parcel_ids': parcel_ids,
                    'year': additional_params.get('year', 2021)
                })
                
                return {
                    'success': True,
                    'processed_directly': True,
                    'batch_result': result
                }
                
            except Exception as e:
                logger.error(f"Error processing directly: {str(e)}")
                return {
                    'success': False,
                    'error': str(e)
                }

# Convenience functions for integration with existing Lambda functions

def enrich_parcel_with_nlcd(parcel_id: str, year: int = 2021, 
                           environment: str = None) -> Dict:
    """
    Convenience function to enrich a single parcel with NLCD data
    
    Args:
        parcel_id: Parcel ID to enrich
        year: NLCD year
        environment: Environment (dev/prod)
        
    Returns:
        Enrichment result
    """
    
    processor = UnifiedNLCDProcessor(environment)
    return processor.process_single_parcel({
        'parcel_id': parcel_id,
        'year': year
    })

def enrich_parcels_with_nlcd_batch(parcel_ids: List[str], year: int = 2021,
                                  environment: str = None) -> Dict:
    """
    Convenience function to enrich multiple parcels with NLCD data
    
    Args:
        parcel_ids: List of parcel IDs to enrich
        year: NLCD year
        environment: Environment (dev/prod)
        
    Returns:
        Batch enrichment result
    """
    
    processor = UnifiedNLCDProcessor(environment)
    return processor.process_batch_parcels({
        'parcel_ids': parcel_ids,
        'year': year
    })

def trigger_nlcd_enrichment(parcel_ids: List[str], source: str,
                           environment: str = None, **kwargs) -> Dict:
    """
    Convenience function to trigger NLCD enrichment
    
    Args:
        parcel_ids: List of parcel IDs to enrich
        source: Source of the request
        environment: Environment (dev/prod)
        **kwargs: Additional parameters
        
    Returns:
        Trigger result
    """
    
    processor = UnifiedNLCDProcessor(environment)
    return processor.trigger_enrichment(parcel_ids, source, kwargs)

def should_enrich_with_nlcd(event: Dict, source: str = None) -> bool:
    """
    Check if NLCD enrichment should be performed based on event and configuration
    
    Args:
        event: Lambda event
        source: Source type
        
    Returns:
        True if enrichment should be performed
    """
    
    # Check explicit flag in event
    if event.get('enrich_nlcd', False):
        return True
    
    # Check configuration if source provided
    if source:
        processor = UnifiedNLCDProcessor()
        return processor.should_auto_enrich(source)
    
    return False

# Example usage for testing
if __name__ == "__main__":
    # Test the unified processor
    processor = UnifiedNLCDProcessor()
    
    # Test single parcel
    test_event = {
        'parcel_id': 'test_parcel_001',
        'year': 2021
    }
    
    result = processor.process_request(test_event)
    print(f"Single parcel result: {json.dumps(result, indent=2)}")
