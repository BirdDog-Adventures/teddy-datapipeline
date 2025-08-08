#!/usr/bin/env python3
"""
NLCD Client Test Script

This script tests the NLCD client functionality before integrating it into the main pipeline.
It validates:
1. Database schema setup
2. NLCD API connectivity
3. Data processing and storage
4. Error handling
5. Performance metrics

Usage:
    python test_nlcd_client.py [--environment dev|prod] [--test-parcel PARCEL_ID]
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional

# Add the lambda directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

try:
    from utils.nlcd_client import NLCDSnowflakeIntegration, get_nlcd_integration
    from utils.snowflake_connector import get_snowflake_connector
    from utils.unified_nlcd_processor import UnifiedNLCDProcessor
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

class NLCDClientTester:
    """Comprehensive tester for NLCD client functionality"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.test_results = {}
        self.start_time = None
        
        logger.info(f"Initializing NLCD Client Tester for environment: {environment}")
        
    def run_all_tests(self, test_parcel_id: Optional[str] = None) -> Dict:
        """Run all NLCD client tests"""
        
        self.start_time = time.time()
        logger.info("🚀 Starting NLCD Client Test Suite")
        
        tests = [
            ('Database Schema', self.test_database_schema),
            ('NLCD API Connectivity', self.test_nlcd_api_connectivity),
            ('Client Initialization', self.test_client_initialization),
            ('Single Parcel Processing', lambda: self.test_single_parcel_processing(test_parcel_id)),
            ('Batch Processing', self.test_batch_processing),
            ('Error Handling', self.test_error_handling),
            ('Data Validation', self.test_data_validation),
            ('Performance Metrics', self.test_performance_metrics),
            ('Unified Processor', self.test_unified_processor)
        ]
        
        for test_name, test_func in tests:
            logger.info(f"\n📋 Running test: {test_name}")
            try:
                result = test_func()
                self.test_results[test_name] = {
                    'status': 'PASSED' if result else 'FAILED',
                    'details': result if isinstance(result, dict) else {'success': result}
                }
                status_emoji = "✅" if result else "❌"
                logger.info(f"{status_emoji} {test_name}: {'PASSED' if result else 'FAILED'}")
                
            except Exception as e:
                logger.error(f"❌ {test_name}: ERROR - {str(e)}")
                self.test_results[test_name] = {
                    'status': 'ERROR',
                    'details': {'error': str(e)}
                }
        
        # Generate summary
        self.generate_test_summary()
        return self.test_results
    
    def test_database_schema(self) -> bool:
        """Test if the database schema is properly set up"""
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Test if RAW schema exists
                result = sf.execute_query("SHOW SCHEMAS LIKE 'RAW' IN DATABASE TEDDY_DATA")
                if not result:
                    logger.error("RAW schema not found in TEDDY_DATA database")
                    return False
                
                # Test if NLCD tables exist
                required_tables = [
                    'LAND_COVER_CLASSES',
                    'LAND_COVER',
                    'LAND_COVER_SUMMARY',
                    'PROCESSING_STATUS'
                ]
                
                for table in required_tables:
                    result = sf.execute_query(f"SHOW TABLES LIKE '{table}' IN SCHEMA TEDDY_DATA.RAW")
                    if not result:
                        logger.error(f"Required table {table} not found in RAW schema")
                        return False
                
                # Test if LAND_COVER_CLASSES has data
                result = sf.execute_query("SELECT COUNT(*) as count FROM TEDDY_DATA.RAW.LAND_COVER_CLASSES")
                if not result or result[0]['COUNT'] == 0:
                    logger.warning("LAND_COVER_CLASSES table is empty - run setup_land_cover_schema.sql first")
                    return False
                
                logger.info(f"✅ Database schema validation passed - {result[0]['COUNT']} land cover classes found")
                return True
                
        except Exception as e:
            logger.error(f"Database schema test failed: {e}")
            return False
    
    def test_nlcd_api_connectivity(self) -> bool:
        """Test connectivity to NLCD API"""
        
        try:
            # Test with a known location (Austin, TX)
            test_lat, test_lon = 30.2672, -97.7431
            
            # Import the NLCD API client directly to test API
            from utils.nlcd_client import NLCDAPIClient
            
            nlcd_client = NLCDAPIClient()
            
            # Test point-based query
            result = nlcd_client.get_land_cover_for_point(test_lat, test_lon, year=2021)
            
            if result and 'nlcd_code' in result:
                logger.info(f"✅ NLCD API connectivity test passed - got NLCD code: {result['nlcd_code']}")
                return True
            else:
                logger.error("NLCD API returned no data or invalid format")
                return False
                
        except Exception as e:
            logger.error(f"NLCD API connectivity test failed: {e}")
            return False
    
    def test_client_initialization(self) -> bool:
        """Test NLCD client initialization"""
        
        try:
            # Test getting NLCD integration
            nlcd_integration = get_nlcd_integration(self.environment)
            
            if not nlcd_integration:
                logger.error("Failed to initialize NLCD integration")
                return False
            
            # Test if it has required methods
            required_methods = [
                'process_parcel_land_cover',
                'get_parcel_land_cover_json',
                'bulk_process_parcels'
            ]
            
            for method in required_methods:
                if not hasattr(nlcd_integration, method):
                    logger.error(f"NLCD integration missing required method: {method}")
                    return False
            
            logger.info("✅ NLCD client initialization test passed")
            return True
            
        except Exception as e:
            logger.error(f"Client initialization test failed: {e}")
            return False
    
    def test_single_parcel_processing(self, test_parcel_id: Optional[str] = None) -> Dict:
        """Test processing a single parcel"""
        
        if not test_parcel_id:
            # Try to get a test parcel from the database
            test_parcel_id = self.get_test_parcel_id()
            
        if not test_parcel_id:
            logger.warning("No test parcel ID provided and none found in database")
            return {'success': False, 'reason': 'No test parcel available'}
        
        try:
            nlcd_integration = get_nlcd_integration(self.environment)
            
            start_time = time.time()
            result = nlcd_integration.process_parcel_land_cover(
                parcel_id=test_parcel_id,
                year=2021,
                force_refresh=True
            )
            processing_time = time.time() - start_time
            
            if result and result.get('success'):
                logger.info(f"✅ Single parcel processing test passed in {processing_time:.2f}s")
                
                # Get detailed data
                detailed_data = nlcd_integration.get_parcel_land_cover_json(test_parcel_id)
                
                return {
                    'success': True,
                    'parcel_id': test_parcel_id,
                    'processing_time': processing_time,
                    'result': result,
                    'detailed_data': detailed_data
                }
            else:
                logger.error(f"Single parcel processing failed: {result}")
                return {'success': False, 'result': result}
                
        except Exception as e:
            logger.error(f"Single parcel processing test failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_batch_processing(self) -> Dict:
        """Test batch processing functionality"""
        
        try:
            # Get a few test parcels
            test_parcel_ids = self.get_test_parcel_ids(limit=3)
            
            if not test_parcel_ids:
                logger.warning("No test parcels found for batch processing test")
                return {'success': False, 'reason': 'No test parcels available'}
            
            nlcd_integration = get_nlcd_integration(self.environment)
            
            start_time = time.time()
            results = nlcd_integration.bulk_process_parcels(
                parcel_ids=test_parcel_ids,
                year=2021,
                batch_size=2
            )
            processing_time = time.time() - start_time
            
            if results and results.get('success'):
                logger.info(f"✅ Batch processing test passed in {processing_time:.2f}s")
                return {
                    'success': True,
                    'parcel_count': len(test_parcel_ids),
                    'processing_time': processing_time,
                    'results': results
                }
            else:
                logger.error(f"Batch processing failed: {results}")
                return {'success': False, 'results': results}
                
        except Exception as e:
            logger.error(f"Batch processing test failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_error_handling(self) -> bool:
        """Test error handling with invalid inputs"""
        
        try:
            nlcd_integration = get_nlcd_integration(self.environment)
            
            # Test with invalid parcel ID
            result = nlcd_integration.process_parcel_land_cover(
                parcel_id="INVALID_PARCEL_ID_12345",
                year=2021
            )
            
            # Should handle gracefully and return success=False
            if result and not result.get('success'):
                logger.info("✅ Error handling test passed - invalid parcel handled gracefully")
                return True
            else:
                logger.error("Error handling test failed - should have returned success=False")
                return False
                
        except Exception as e:
            logger.error(f"Error handling test failed with exception: {e}")
            return False
    
    def test_data_validation(self) -> bool:
        """Test data validation and integrity"""
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Check for any data in LAND_COVER
                result = sf.execute_query("""
                    SELECT COUNT(*) as count,
                           COUNT(DISTINCT PARCEL_ID) as unique_parcels,
                           COUNT(DISTINCT NLCD_CODE) as unique_codes
                    FROM TEDDY_DATA.RAW.LAND_COVER
                """)
                
                if result and result[0]['COUNT'] > 0:
                    stats = result[0]
                    logger.info(f"✅ Data validation passed - {stats['COUNT']} records, "
                              f"{stats['UNIQUE_PARCELS']} parcels, {stats['UNIQUE_CODES']} NLCD codes")
                    
                    # Validate data integrity
                    integrity_check = sf.execute_query("""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN COVERAGE_PERCENTAGE BETWEEN 0 AND 100 THEN 1 END) as valid_percentages,
                            COUNT(CASE WHEN NLCD_CODE IN (SELECT NLCD_CODE FROM TEDDY_DATA.RAW.LAND_COVER_CLASSES) THEN 1 END) as valid_codes
                        FROM TEDDY_DATA.RAW.LAND_COVER
                    """)
                    
                    if integrity_check:
                        check = integrity_check[0]
                        if (check['TOTAL_RECORDS'] == check['VALID_PERCENTAGES'] and 
                            check['TOTAL_RECORDS'] == check['VALID_CODES']):
                            logger.info("✅ Data integrity validation passed")
                            return True
                        else:
                            logger.warning(f"Data integrity issues found: {check}")
                            return False
                else:
                    logger.info("No NLCD data found yet - this is expected for first run")
                    return True
                    
        except Exception as e:
            logger.error(f"Data validation test failed: {e}")
            return False
    
    def test_performance_metrics(self) -> Dict:
        """Test performance metrics and timing"""
        
        try:
            # Test single parcel processing time
            test_parcel_id = self.get_test_parcel_id()
            if not test_parcel_id:
                return {'success': False, 'reason': 'No test parcel available'}
            
            nlcd_integration = get_nlcd_integration(self.environment)
            
            # Time multiple operations
            times = []
            for i in range(3):
                start_time = time.time()
                result = nlcd_integration.process_parcel_land_cover(
                    parcel_id=test_parcel_id,
                    year=2021,
                    force_refresh=False  # Use cache after first run
                )
                times.append(time.time() - start_time)
                
                if not result or not result.get('success'):
                    break
            
            if times:
                avg_time = sum(times) / len(times)
                min_time = min(times)
                max_time = max(times)
                
                logger.info(f"✅ Performance test completed - Avg: {avg_time:.2f}s, "
                          f"Min: {min_time:.2f}s, Max: {max_time:.2f}s")
                
                return {
                    'success': True,
                    'average_time': avg_time,
                    'min_time': min_time,
                    'max_time': max_time,
                    'runs': len(times)
                }
            else:
                return {'success': False, 'reason': 'No successful runs'}
                
        except Exception as e:
            logger.error(f"Performance metrics test failed: {e}")
            return {'success': False, 'error': str(e)}
    
    def test_unified_processor(self) -> bool:
        """Test the unified NLCD processor"""
        
        try:
            processor = UnifiedNLCDProcessor(self.environment)
            
            # Test single parcel processing
            test_parcel_id = self.get_test_parcel_id()
            if not test_parcel_id:
                logger.warning("No test parcel for unified processor test")
                return False
            
            event = {
                'parcel_id': test_parcel_id,
                'year': 2021
            }
            
            result = processor.process_request(event)
            
            if result and result.get('success'):
                logger.info("✅ Unified processor test passed")
                return True
            else:
                logger.error(f"Unified processor test failed: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Unified processor test failed: {e}")
            return False
    
    def get_test_parcel_id(self) -> Optional[str]:
        """Get a test parcel ID from the database"""
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                result = sf.execute_query("""
                    SELECT PARCEL_ID 
                    FROM TEDDY_DATA.CURATED.PARCEL_PROFILES 
                    WHERE LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL 
                    AND STATE_CODE = 'TX'
                    LIMIT 1
                """)
                
                if result:
                    return result[0]['PARCEL_ID']
                    
        except Exception as e:
            logger.error(f"Error getting test parcel ID: {e}")
            
        return None
    
    def get_test_parcel_ids(self, limit: int = 5) -> List[str]:
        """Get multiple test parcel IDs from the database"""
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                result = sf.execute_query(f"""
                    SELECT PARCEL_ID 
                    FROM TEDDY_DATA.CURATED.PARCEL_PROFILES 
                    WHERE LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL 
                    AND STATE_CODE = 'TX'
                    LIMIT {limit}
                """)
                
                if result:
                    return [row['PARCEL_ID'] for row in result]
                    
        except Exception as e:
            logger.error(f"Error getting test parcel IDs: {e}")
            
        return []
    
    def generate_test_summary(self):
        """Generate and display test summary"""
        
        total_time = time.time() - self.start_time if self.start_time else 0
        
        passed = sum(1 for result in self.test_results.values() if result['status'] == 'PASSED')
        failed = sum(1 for result in self.test_results.values() if result['status'] == 'FAILED')
        errors = sum(1 for result in self.test_results.values() if result['status'] == 'ERROR')
        total = len(self.test_results)
        
        logger.info("\n" + "="*60)
        logger.info("🎯 NLCD CLIENT TEST SUMMARY")
        logger.info("="*60)
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Total Tests: {total}")
        logger.info(f"✅ Passed: {passed}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"🚨 Errors: {errors}")
        logger.info(f"⏱️  Total Time: {total_time:.2f}s")
        logger.info(f"Success Rate: {(passed/total)*100:.1f}%")
        
        # Detailed results
        logger.info("\n📊 DETAILED RESULTS:")
        for test_name, result in self.test_results.items():
            status_emoji = {"PASSED": "✅", "FAILED": "❌", "ERROR": "🚨"}[result['status']]
            logger.info(f"{status_emoji} {test_name}: {result['status']}")
            
            if result['status'] != 'PASSED' and 'error' in result['details']:
                logger.info(f"   Error: {result['details']['error']}")
        
        # Recommendations
        logger.info("\n💡 RECOMMENDATIONS:")
        if failed > 0 or errors > 0:
            logger.info("❗ Some tests failed. Check the following:")
            logger.info("   1. Run setup_land_cover_schema.sql to create database schema")
            logger.info("   2. Verify Snowflake connection credentials")
            logger.info("   3. Check NLCD API connectivity")
            logger.info("   4. Ensure test parcels exist in PARCEL_PROFILES table")
        else:
            logger.info("🎉 All tests passed! NLCD client is ready for pipeline integration.")
        
        logger.info("="*60)

def main():
    """Main function to run NLCD client tests"""
    
    parser = argparse.ArgumentParser(description='Test NLCD Client functionality')
    parser.add_argument('--environment', choices=['dev', 'prod'], default='dev',
                       help='Environment to test (default: dev)')
    parser.add_argument('--test-parcel', type=str,
                       help='Specific parcel ID to test with')
    parser.add_argument('--output-file', type=str,
                       help='File to save test results JSON')
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = NLCDClientTester(args.environment)
    
    # Run tests
    results = tester.run_all_tests(args.test_parcel)
    
    # Save results if requested
    if args.output_file:
        with open(args.output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Test results saved to {args.output_file}")
    
    # Exit with appropriate code
    failed_tests = sum(1 for result in results.values() 
                      if result['status'] in ['FAILED', 'ERROR'])
    sys.exit(failed_tests)

if __name__ == "__main__":
    main()
