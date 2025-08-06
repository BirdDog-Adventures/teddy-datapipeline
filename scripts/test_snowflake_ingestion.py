#!/usr/bin/env python3
"""
Simple Snowflake Connection and Data Ingestion Test Script

This script tests the Snowflake connection using AWS Secrets Manager credentials
and performs a simple data ingestion to verify everything is working correctly.

Usage:
    python test_snowflake_ingestion.py [--environment dev|prod] [--create-table] [--sample-size N]
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any
import uuid

# Add the lambda directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

try:
    from utils.snowflake_connector import get_snowflake_connector
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Make sure you're running this from the teddy-datapipeline directory")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowflakeIngestionTester:
    """Test Snowflake connectivity and data ingestion"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.test_table = 'TEST_DATA_INGESTION'
        self.test_schema = 'RAW'
        
        logger.info(f"Initializing Snowflake Ingestion Tester for environment: {environment}")
    
    def test_connection(self) -> Dict[str, Any]:
        """Test basic Snowflake connection"""
        
        logger.info("🔗 Testing Snowflake connection...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                result = sf.test_connection()
                
                if result['status'] == 'success':
                    logger.info("✅ Snowflake connection successful!")
                    logger.info(f"   User: {result['connection_info']['CURRENT_USER']}")
                    logger.info(f"   Warehouse: {result['connection_info']['CURRENT_WAREHOUSE']}")
                    logger.info(f"   Database: {result['connection_info']['CURRENT_DATABASE']}")
                    logger.info(f"   Schema: {result['connection_info']['CURRENT_SCHEMA']}")
                    logger.info(f"   Connection Time: {result['connection_info']['CONNECTION_TIME']}")
                else:
                    logger.error("❌ Snowflake connection failed!")
                    logger.error(f"   Error: {result['error']}")
                
                return result
                
        except Exception as e:
            logger.error(f"❌ Connection test failed with exception: {e}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    
    def create_test_table(self) -> bool:
        """Create a test table for data ingestion"""
        
        logger.info(f"🏗️  Creating test table {self.test_schema}.{self.test_table}...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # First, try to drop table if exists (ignore errors if it doesn't exist)
                try:
                    drop_query = f"DROP TABLE IF EXISTS {self.test_schema}.{self.test_table}"
                    sf.execute_non_query(drop_query)
                    logger.info(f"   Dropped existing table if present")
                except Exception as drop_error:
                    logger.info(f"   No existing table to drop (this is normal): {drop_error}")
                
                # Create test table with OR REPLACE to handle any remaining issues
                create_query = f"""
                CREATE OR REPLACE TABLE {self.test_schema}.{self.test_table} (
                    ID VARCHAR(50) PRIMARY KEY,
                    TEST_NAME VARCHAR(100) NOT NULL,
                    TEST_VALUE NUMBER(10,2),
                    TEST_DATE TIMESTAMP_NTZ,
                    TEST_JSON VARIANT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    ENVIRONMENT VARCHAR(10)
                )
                """
                
                sf.execute_non_query(create_query)
                logger.info(f"✅ Test table {self.test_schema}.{self.test_table} created successfully!")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to create test table: {e}")
            logger.info(f"   This might be due to insufficient permissions on the {self.test_schema} schema")
            logger.info(f"   Try running the test without --create-table to test connection only")
            return False
    
    def generate_sample_data(self, sample_size: int = 10) -> List[Dict[str, Any]]:
        """Generate sample test data"""
        
        logger.info(f"📊 Generating {sample_size} sample records...")
        
        sample_data = []
        base_time = datetime.now()
        
        for i in range(sample_size):
            record = {
                'ID': str(uuid.uuid4()),
                'TEST_NAME': f'Test Record {i+1}',
                'TEST_VALUE': round((i + 1) * 10.5, 2),
                'TEST_DATE': (base_time - timedelta(days=i)).isoformat(),
                'TEST_JSON': json.dumps({
                    'record_number': i + 1,
                    'test_type': 'ingestion_test',
                    'metadata': {
                        'generated_at': base_time.isoformat(),
                        'test_run_id': str(uuid.uuid4())[:8]
                    }
                }),
                'ENVIRONMENT': self.environment
            }
            sample_data.append(record)
        
        logger.info(f"✅ Generated {len(sample_data)} sample records")
        return sample_data
    
    def ingest_data(self, data: List[Dict[str, Any]]) -> bool:
        """Ingest data into the test table"""
        
        logger.info(f"📥 Ingesting {len(data)} records into {self.test_schema}.{self.test_table}...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Use bulk insert method
                affected_rows = sf.bulk_insert(
                    table_name=self.test_table,
                    data=data,
                    schema=self.test_schema
                )
                
                logger.info(f"✅ Successfully ingested {affected_rows} records!")
                return True
                
        except Exception as e:
            logger.error(f"❌ Data ingestion failed: {e}")
            return False
    
    def verify_data(self, expected_count: int) -> Dict[str, Any]:
        """Verify the ingested data"""
        
        logger.info(f"🔍 Verifying ingested data...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Count total records
                count_query = f"SELECT COUNT(*) as TOTAL_COUNT FROM {self.test_schema}.{self.test_table}"
                count_result = sf.execute_query(count_query)
                total_count = count_result[0]['TOTAL_COUNT'] if count_result else 0
                
                # Get sample records
                sample_query = f"""
                SELECT 
                    ID,
                    TEST_NAME,
                    TEST_VALUE,
                    TEST_DATE,
                    ENVIRONMENT,
                    CREATED_AT
                FROM {self.test_schema}.{self.test_table}
                ORDER BY CREATED_AT DESC
                LIMIT 5
                """
                sample_records = sf.execute_query(sample_query)
                
                # Get data statistics
                stats_query = f"""
                SELECT 
                    COUNT(*) as RECORD_COUNT,
                    AVG(TEST_VALUE) as AVG_VALUE,
                    MIN(TEST_DATE) as MIN_DATE,
                    MAX(TEST_DATE) as MAX_DATE,
                    COUNT(DISTINCT ENVIRONMENT) as UNIQUE_ENVIRONMENTS
                FROM {self.test_schema}.{self.test_table}
                """
                stats_result = sf.execute_query(stats_query)
                stats = stats_result[0] if stats_result else {}
                
                verification_result = {
                    'total_count': total_count,
                    'expected_count': expected_count,
                    'count_matches': total_count == expected_count,
                    'sample_records': sample_records,
                    'statistics': stats
                }
                
                if verification_result['count_matches']:
                    logger.info(f"✅ Data verification successful!")
                    logger.info(f"   Total records: {total_count}")
                    logger.info(f"   Average test value: {stats.get('AVG_VALUE', 'N/A')}")
                    logger.info(f"   Date range: {stats.get('MIN_DATE', 'N/A')} to {stats.get('MAX_DATE', 'N/A')}")
                else:
                    logger.warning(f"⚠️  Record count mismatch: expected {expected_count}, found {total_count}")
                
                return verification_result
                
        except Exception as e:
            logger.error(f"❌ Data verification failed: {e}")
            return {
                'total_count': 0,
                'expected_count': expected_count,
                'count_matches': False,
                'error': str(e)
            }
    
    def cleanup_test_data(self) -> bool:
        """Clean up test data and table"""
        
        logger.info(f"🧹 Cleaning up test table {self.test_schema}.{self.test_table}...")
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                drop_query = f"DROP TABLE IF EXISTS {self.test_schema}.{self.test_table}"
                sf.execute_non_query(drop_query)
                logger.info("✅ Test table cleaned up successfully!")
                return True
                
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            return False
    
    def run_full_test(self, sample_size: int = 10, create_table: bool = True, cleanup: bool = True) -> Dict[str, Any]:
        """Run the complete ingestion test"""
        
        logger.info("🚀 Starting Snowflake Ingestion Test Suite")
        logger.info("=" * 60)
        
        test_results = {
            'connection_test': None,
            'table_creation': None,
            'data_generation': None,
            'data_ingestion': None,
            'data_verification': None,
            'cleanup': None,
            'overall_success': False
        }
        
        try:
            # Step 1: Test connection
            test_results['connection_test'] = self.test_connection()
            if test_results['connection_test']['status'] != 'success':
                logger.error("❌ Connection test failed, aborting test suite")
                return test_results
            
            # Step 2: Create test table (if requested)
            if create_table:
                test_results['table_creation'] = self.create_test_table()
                if not test_results['table_creation']:
                    logger.error("❌ Table creation failed, aborting test suite")
                    return test_results
            
            # Step 3: Generate sample data
            sample_data = self.generate_sample_data(sample_size)
            test_results['data_generation'] = len(sample_data) == sample_size
            
            # Step 4: Ingest data
            test_results['data_ingestion'] = self.ingest_data(sample_data)
            if not test_results['data_ingestion']:
                logger.error("❌ Data ingestion failed, aborting test suite")
                return test_results
            
            # Step 5: Verify data
            test_results['data_verification'] = self.verify_data(sample_size)
            
            # Step 6: Cleanup (if requested)
            if cleanup:
                test_results['cleanup'] = self.cleanup_test_data()
            
            # Determine overall success
            test_results['overall_success'] = (
                test_results['connection_test']['status'] == 'success' and
                (not create_table or test_results['table_creation']) and
                test_results['data_generation'] and
                test_results['data_ingestion'] and
                test_results['data_verification']['count_matches'] and
                (not cleanup or test_results['cleanup'])
            )
            
            # Print summary
            self.print_test_summary(test_results)
            
            return test_results
            
        except Exception as e:
            logger.error(f"❌ Test suite failed with exception: {e}")
            test_results['error'] = str(e)
            return test_results
    
    def print_test_summary(self, results: Dict[str, Any]):
        """Print a summary of test results"""
        
        logger.info("\n" + "=" * 60)
        logger.info("🎯 SNOWFLAKE INGESTION TEST SUMMARY")
        logger.info("=" * 60)
        
        # Connection test
        conn_status = "✅ PASSED" if results['connection_test']['status'] == 'success' else "❌ FAILED"
        logger.info(f"Connection Test: {conn_status}")
        
        # Table creation
        if results['table_creation'] is not None:
            table_status = "✅ PASSED" if results['table_creation'] else "❌ FAILED"
            logger.info(f"Table Creation: {table_status}")
        
        # Data generation
        data_gen_status = "✅ PASSED" if results['data_generation'] else "❌ FAILED"
        logger.info(f"Data Generation: {data_gen_status}")
        
        # Data ingestion
        ingestion_status = "✅ PASSED" if results['data_ingestion'] else "❌ FAILED"
        logger.info(f"Data Ingestion: {ingestion_status}")
        
        # Data verification
        if results['data_verification']:
            verification_status = "✅ PASSED" if results['data_verification']['count_matches'] else "❌ FAILED"
            logger.info(f"Data Verification: {verification_status}")
            if results['data_verification']['count_matches']:
                logger.info(f"   Records ingested: {results['data_verification']['total_count']}")
        
        # Cleanup
        if results['cleanup'] is not None:
            cleanup_status = "✅ PASSED" if results['cleanup'] else "❌ FAILED"
            logger.info(f"Cleanup: {cleanup_status}")
        
        # Overall result
        overall_status = "✅ SUCCESS" if results['overall_success'] else "❌ FAILED"
        logger.info(f"\nOverall Test Result: {overall_status}")
        
        if results['overall_success']:
            logger.info("🎉 All tests passed! Snowflake connection and ingestion working correctly.")
        else:
            logger.info("⚠️  Some tests failed. Check the logs above for details.")
        
        logger.info("=" * 60)

def main():
    """Main function to run Snowflake ingestion tests"""
    
    parser = argparse.ArgumentParser(description='Test Snowflake connectivity and data ingestion')
    parser.add_argument('--environment', choices=['dev', 'prod'], default='dev',
                       help='Environment to test (default: dev)')
    parser.add_argument('--create-table', action='store_true',
                       help='Create test table (will drop existing table)')
    parser.add_argument('--sample-size', type=int, default=10,
                       help='Number of sample records to generate (default: 10)')
    parser.add_argument('--no-cleanup', action='store_true',
                       help='Skip cleanup (leave test table and data)')
    parser.add_argument('--output-file', type=str,
                       help='File to save test results JSON')
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = SnowflakeIngestionTester(args.environment)
    
    # Run tests
    results = tester.run_full_test(
        sample_size=args.sample_size,
        create_table=args.create_table,
        cleanup=not args.no_cleanup
    )
    
    # Save results if requested
    if args.output_file:
        with open(args.output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Test results saved to {args.output_file}")
    
    # Exit with appropriate code
    sys.exit(0 if results['overall_success'] else 1)

if __name__ == "__main__":
    main()
