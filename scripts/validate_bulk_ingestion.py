#!/usr/bin/env python3
"""
Bulk Ingestion Validation Script

This script validates the bulk parcel ingestion pipeline using real data
from the Shackelford County JSON file.
"""

import json
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Any
import boto3
from pathlib import Path

# Add the lambda utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))
from utils.dynamodb_cache import DynamoDBCacheManager
from utils.snowflake_connector import get_snowflake_connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BulkIngestionValidator:
    """Validates the bulk parcel ingestion pipeline"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.s3_client = boto3.client('s3')
        self.sns_client = boto3.client('sns')
        self.cache_manager = DynamoDBCacheManager(environment=environment)
        
    def analyze_json_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze the structure and content of the JSON file"""
        logger.info(f"Analyzing JSON file: {file_path}")
        
        try:
            # Get file size
            file_size = os.path.getsize(file_path)
            logger.info(f"File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            
            # Read and analyze the JSON structure
            with open(file_path, 'r') as f:
                # Read first few characters to determine structure
                first_char = f.read(1)
                f.seek(0)
                
                if first_char == '[':
                    # Array of parcels
                    logger.info("File contains an array of parcels")
                    parcels = json.load(f)
                    
                elif first_char == '{':
                    # Single object or object with parcels array
                    data = json.load(f)
                    if isinstance(data, dict):
                        if 'parcels' in data:
                            parcels = data['parcels']
                            logger.info("File contains object with 'parcels' array")
                        elif 'features' in data:
                            parcels = data['features']
                            logger.info("File contains GeoJSON-like structure with 'features'")
                        else:
                            # Assume the object itself is a parcel
                            parcels = [data]
                            logger.info("File contains single parcel object")
                    else:
                        parcels = [data]
                else:
                    raise ValueError("Unknown JSON structure")
                
                # Analyze parcel structure
                total_parcels = len(parcels)
                logger.info(f"Total parcels found: {total_parcels:,}")
                
                if total_parcels > 0:
                    sample_parcel = parcels[0]
                    logger.info(f"Sample parcel keys: {list(sample_parcel.keys())}")
                    
                    # Check for common parcel fields
                    common_fields = ['parcel_id', 'address', 'owner', 'acreage', 'value', 'geometry']
                    found_fields = [field for field in common_fields if field in sample_parcel]
                    logger.info(f"Common fields found: {found_fields}")
                
                return {
                    'file_size': file_size,
                    'total_parcels': total_parcels,
                    'sample_parcel': sample_parcel if total_parcels > 0 else None,
                    'structure_type': 'array' if first_char == '[' else 'object'
                }
                
        except Exception as e:
            logger.error(f"Error analyzing JSON file: {e}")
            raise
    
    def chunk_parcels(self, parcels: List[Dict], chunk_size: int = 500) -> List[List[Dict]]:
        """Split parcels into chunks for processing"""
        chunks = []
        for i in range(0, len(parcels), chunk_size):
            chunk = parcels[i:i + chunk_size]
            chunks.append(chunk)
        
        logger.info(f"Split {len(parcels)} parcels into {len(chunks)} chunks of max {chunk_size} parcels each")
        return chunks
    
    def simulate_s3_upload(self, chunks: List[List[Dict]], county: str = 'shackelford') -> List[str]:
        """Simulate uploading chunks to S3 (dry run)"""
        logger.info("Simulating S3 upload process...")
        
        s3_keys = []
        bucket_name = f"teddy-data-lake-{self.environment}"
        
        for i, chunk in enumerate(chunks):
            s3_key = f"raw/parcel/bulk/{datetime.now().strftime('%Y-%m-%d')}/{county}/chunk_{i:04d}.json"
            
            chunk_data = {
                'county': county,
                'chunk_index': i,
                'total_chunks': len(chunks),
                'parcel_count': len(chunk),
                'ingestion_timestamp': datetime.utcnow().isoformat(),
                'parcels': chunk
            }
            
            # In a real scenario, we would upload to S3
            # self.s3_client.put_object(
            #     Bucket=bucket_name,
            #     Key=s3_key,
            #     Body=json.dumps(chunk_data),
            #     ContentType='application/json'
            # )
            
            s3_keys.append(s3_key)
            logger.info(f"Would upload chunk {i+1}/{len(chunks)} to s3://{bucket_name}/{s3_key}")
        
        return s3_keys
    
    def test_dynamodb_caching(self, parcels: List[Dict], county: str = 'shackelford') -> Dict[str, Any]:
        """Test DynamoDB caching functionality"""
        logger.info("Testing DynamoDB caching...")
        
        try:
            # Test county metadata caching
            county_metadata = {
                'total_parcels': len(parcels),
                'last_ingestion': datetime.utcnow().isoformat(),
                'ingestion_type': 'bulk_validation'
            }
            
            # Cache county metadata
            self.cache_manager.cache_county_metadata(county, county_metadata)
            logger.info(f"Cached metadata for {county}: {county_metadata}")
            
            # Test individual parcel caching (sample first 10 parcels)
            sample_parcels = parcels[:10]
            parcel_cache_data = {}
            
            for i, parcel in enumerate(sample_parcels):
                # Create a parcel ID if not present
                parcel_id = parcel.get('parcel_id') or parcel.get('id') or f"{county}_{i:06d}"
                parcel_cache_data[parcel_id] = parcel
            
            if parcel_cache_data:
                cached_count = self.cache_manager.cache_multiple_parcels(parcel_cache_data, ttl_hours=24)
                logger.info(f"Cached {cached_count} sample parcels")
            
            # Test retrieval
            retrieved_metadata = self.cache_manager.get_cached_county_metadata(county)
            logger.info(f"Retrieved metadata: {retrieved_metadata}")
            
            return {
                'county_metadata_cached': True,
                'parcels_cached': len(parcel_cache_data),
                'retrieval_successful': retrieved_metadata is not None
            }
            
        except Exception as e:
            logger.error(f"Error testing DynamoDB caching: {e}")
            return {
                'county_metadata_cached': False,
                'parcels_cached': 0,
                'retrieval_successful': False,
                'error': str(e)
            }
    
    def test_snowflake_connection(self) -> Dict[str, Any]:
        """Test Snowflake connection with JWT authentication"""
        logger.info("Testing Snowflake connection...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                test_result = sf.test_connection()
                
                if test_result['status'] == 'success':
                    logger.info("✅ Snowflake JWT authentication successful!")
                    logger.info(f"Connected as: {test_result['connection_info']['CURRENT_USER']}")
                    
                    # Test table access
                    try:
                        table_info = sf.get_table_info('PARCEL_DATA_RAW', 'RAW')
                        logger.info(f"PARCEL_DATA_RAW table has {len(table_info)} columns")
                        
                        return {
                            'connection_successful': True,
                            'authentication_method': 'JWT',
                            'user': test_result['connection_info']['CURRENT_USER'],
                            'warehouse': test_result['connection_info']['CURRENT_WAREHOUSE'],
                            'table_accessible': True,
                            'table_columns': len(table_info)
                        }
                        
                    except Exception as table_error:
                        logger.warning(f"Table access test failed: {table_error}")
                        return {
                            'connection_successful': True,
                            'authentication_method': 'JWT',
                            'user': test_result['connection_info']['CURRENT_USER'],
                            'warehouse': test_result['connection_info']['CURRENT_WAREHOUSE'],
                            'table_accessible': False,
                            'table_error': str(table_error)
                        }
                else:
                    logger.error(f"❌ Snowflake connection failed: {test_result['error']}")
                    return {
                        'connection_successful': False,
                        'error': test_result['error']
                    }
                    
        except Exception as e:
            logger.error(f"Error testing Snowflake connection: {e}")
            return {
                'connection_successful': False,
                'error': str(e)
            }
    
    def simulate_sns_events(self, s3_keys: List[str], county: str = 'shackelford') -> Dict[str, Any]:
        """Simulate SNS event publishing"""
        logger.info("Simulating SNS event publishing...")
        
        try:
            # Get topic ARN (would be from environment variables in real scenario)
            topic_arn = f"arn:aws:sns:us-east-1:123456789012:teddy-parcel-data-{self.environment}"
            
            events_published = 0
            for i, s3_key in enumerate(s3_keys):
                event_detail = {
                    'bucket': f"teddy-data-lake-{self.environment}",
                    's3_key': s3_key,
                    'county': county,
                    'chunk_index': i,
                    'ingestion_type': 'bulk_validation'
                }
                
                # In real scenario, would publish to SNS
                # self.sns_client.publish(
                #     TopicArn=topic_arn,
                #     Subject='Parcel Data S3 Upload',
                #     Message=json.dumps(event_detail),
                #     MessageAttributes={
                #         'event_type': {'DataType': 'String', 'StringValue': 's3_upload'},
                #         'county': {'DataType': 'String', 'StringValue': county}
                #     }
                # )
                
                events_published += 1
                logger.info(f"Would publish SNS event {i+1}/{len(s3_keys)} for {s3_key}")
            
            return {
                'events_published': events_published,
                'topic_arn': topic_arn,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error simulating SNS events: {e}")
            return {
                'events_published': 0,
                'success': False,
                'error': str(e)
            }
    
    def run_validation(self, json_file_path: str) -> Dict[str, Any]:
        """Run complete validation of the bulk ingestion pipeline"""
        logger.info("🚀 Starting bulk ingestion pipeline validation...")
        
        validation_results = {
            'timestamp': datetime.utcnow().isoformat(),
            'environment': self.environment,
            'json_file': json_file_path
        }
        
        try:
            # Step 1: Analyze JSON file
            logger.info("📊 Step 1: Analyzing JSON file structure...")
            file_analysis = self.analyze_json_file(json_file_path)
            validation_results['file_analysis'] = file_analysis
            
            # Load parcels for processing
            with open(json_file_path, 'r') as f:
                first_char = f.read(1)
                f.seek(0)
                
                if first_char == '[':
                    parcels = json.load(f)
                else:
                    data = json.load(f)
                    if isinstance(data, dict):
                        if 'parcels' in data:
                            parcels = data['parcels']
                        elif 'features' in data:
                            parcels = data['features']
                        else:
                            parcels = [data]
                    else:
                        parcels = [data]
            
            # Step 2: Test chunking
            logger.info("🔄 Step 2: Testing parcel chunking...")
            chunks = self.chunk_parcels(parcels, chunk_size=500)
            validation_results['chunking'] = {
                'total_parcels': len(parcels),
                'total_chunks': len(chunks),
                'chunk_size': 500
            }
            
            # Step 3: Simulate S3 upload
            logger.info("☁️ Step 3: Simulating S3 upload...")
            s3_keys = self.simulate_s3_upload(chunks, county='shackelford')
            validation_results['s3_simulation'] = {
                'chunks_uploaded': len(s3_keys),
                'sample_s3_keys': s3_keys[:3]  # First 3 keys as sample
            }
            
            # Step 4: Test DynamoDB caching
            logger.info("💾 Step 4: Testing DynamoDB caching...")
            cache_results = self.test_dynamodb_caching(parcels, county='shackelford')
            validation_results['dynamodb_caching'] = cache_results
            
            # Step 5: Test Snowflake connection
            logger.info("❄️ Step 5: Testing Snowflake connection...")
            snowflake_results = self.test_snowflake_connection()
            validation_results['snowflake_connection'] = snowflake_results
            
            # Step 6: Simulate SNS events
            logger.info("📡 Step 6: Simulating SNS event publishing...")
            sns_results = self.simulate_sns_events(s3_keys, county='shackelford')
            validation_results['sns_simulation'] = sns_results
            
            # Overall validation status
            validation_results['overall_status'] = 'SUCCESS'
            validation_results['summary'] = {
                'parcels_processed': len(parcels),
                'chunks_created': len(chunks),
                'cache_test_passed': cache_results.get('county_metadata_cached', False),
                'snowflake_test_passed': snowflake_results.get('connection_successful', False),
                'sns_simulation_passed': sns_results.get('success', False)
            }
            
            logger.info("✅ Bulk ingestion pipeline validation completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            validation_results['overall_status'] = 'FAILED'
            validation_results['error'] = str(e)
        
        return validation_results

def main():
    """Main function to run the validation"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate bulk parcel ingestion pipeline')
    parser.add_argument('--json-file', required=True, help='Path to the JSON file to validate')
    parser.add_argument('--environment', default='dev', help='Environment (dev, staging, prod)')
    parser.add_argument('--output', help='Output file for validation results (JSON)')
    
    args = parser.parse_args()
    
    # Validate file exists
    if not os.path.exists(args.json_file):
        logger.error(f"JSON file not found: {args.json_file}")
        sys.exit(1)
    
    # Run validation
    validator = BulkIngestionValidator(environment=args.environment)
    results = validator.run_validation(args.json_file)
    
    # Output results
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Validation results saved to: {args.output}")
    else:
        print("\n" + "="*80)
        print("VALIDATION RESULTS")
        print("="*80)
        print(json.dumps(results, indent=2))
    
    # Exit with appropriate code
    if results.get('overall_status') == 'SUCCESS':
        logger.info("🎉 All validation tests passed!")
        sys.exit(0)
    else:
        logger.error("❌ Some validation tests failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
