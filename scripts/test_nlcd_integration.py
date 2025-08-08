#!/usr/bin/env python3
"""
Test Script for NLCD Integration

This script tests the NLCD integration components including:
- Database schema setup
- NLCD API client functionality
- Snowflake integration
- Lambda function processing
- Bulk processing capabilities

Usage:
    python test_nlcd_integration.py --test-all
    python test_nlcd_integration.py --test-schema
    python test_nlcd_integration.py --test-api
    python test_nlcd_integration.py --test-integration
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add the lambda utils to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from utils.nlcd_client import get_nlcd_integration, NLCDAPIClient, NLCDCodeMapping
from utils.snowflake_connector import get_snowflake_connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NLCDIntegrationTester:
    """Test suite for NLCD integration components"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.test_results = []
        
    def log_test_result(self, test_name: str, success: bool, message: str = "", details: Any = None):
        """Log test result"""
        result = {
            'test_name': test_name,
            'success': success,
            'message': message,
            'details': details,
            'timestamp': datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} - {test_name}: {message}")
        
        if details and not success:
            logger.error(f"Details: {details}")
    
    def test_snowflake_connection(self) -> bool:
        """Test Snowflake connection"""
        try:
            with get_snowflake_connector(self.environment) as sf:
                result = sf.test_connection()
                
                if result['status'] == 'success':
                    self.log_test_result(
                        "Snowflake Connection",
                        True,
                        f"Connected as {result['connection_info']['CURRENT_USER']}",
                        result
                    )
                    return True
                else:
                    self.log_test_result(
                        "Snowflake Connection",
                        False,
                        "Connection failed",
                        result
                    )
                    return False
                    
        except Exception as e:
            self.log_test_result(
                "Snowflake Connection",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_land_cover_schema(self) -> bool:
        """Test that the LAND_COVER schema exists and has required tables"""
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Check if schema exists
                schema_query = """
                    SELECT SCHEMA_NAME 
                    FROM INFORMATION_SCHEMA.SCHEMATA 
                    WHERE SCHEMA_NAME = 'LAND_COVER' 
                    AND CATALOG_NAME = 'TEDDY_DATA'
                """
                
                schema_result = sf.execute_query(schema_query)
                
                if not schema_result:
                    self.log_test_result(
                        "Land Cover Schema",
                        False,
                        "LAND_COVER schema not found"
                    )
                    return False
                
                # Check required tables
                required_tables = [
                    'LAND_COVER_CLASSES',
                    'LAND_COVER',
                    'LAND_COVER_SUMMARY',
                    'PROCESSING_STATUS'
                ]
                
                tables_query = """
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA = 'LAND_COVER' 
                    AND TABLE_CATALOG = 'TEDDY_DATA'
                """
                
                tables_result = sf.execute_query(tables_query)
                existing_tables = [row['TABLE_NAME'] for row in tables_result]
                
                missing_tables = [table for table in required_tables if table not in existing_tables]
                
                if missing_tables:
                    self.log_test_result(
                        "Land Cover Schema",
                        False,
                        f"Missing tables: {missing_tables}",
                        {'existing_tables': existing_tables, 'missing_tables': missing_tables}
                    )
                    return False
                
                # Check land cover classes data
                classes_query = "SELECT COUNT(*) as count FROM TEDDY_DATA.LAND_COVER.LAND_COVER_CLASSES"
                classes_result = sf.execute_query(classes_query)
                classes_count = classes_result[0]['COUNT'] if classes_result else 0
                
                if classes_count < 15:  # Should have at least 15 NLCD classes
                    self.log_test_result(
                        "Land Cover Schema",
                        False,
                        f"Insufficient land cover classes: {classes_count}",
                        {'classes_count': classes_count}
                    )
                    return False
                
                self.log_test_result(
                    "Land Cover Schema",
                    True,
                    f"Schema valid with {len(existing_tables)} tables and {classes_count} land cover classes",
                    {'tables': existing_tables, 'classes_count': classes_count}
                )
                return True
                
        except Exception as e:
            self.log_test_result(
                "Land Cover Schema",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_nlcd_code_mapping(self) -> bool:
        """Test NLCD code mapping functionality"""
        try:
            # Test known NLCD codes
            test_codes = [11, 21, 41, 81, 82]  # Water, Developed, Forest, Pasture, Crops
            
            for code in test_codes:
                info = NLCDCodeMapping.get_land_cover_info(code)
                
                if info['name'].startswith('Unknown'):
                    self.log_test_result(
                        "NLCD Code Mapping",
                        False,
                        f"Unknown code mapping for {code}",
                        {'code': code, 'info': info}
                    )
                    return False
            
            # Test agricultural classification
            if not NLCDCodeMapping.is_agricultural(81):  # Pasture/Hay
                self.log_test_result(
                    "NLCD Code Mapping",
                    False,
                    "Agricultural classification failed for code 81"
                )
                return False
            
            if not NLCDCodeMapping.is_agricultural(82):  # Cultivated Crops
                self.log_test_result(
                    "NLCD Code Mapping",
                    False,
                    "Agricultural classification failed for code 82"
                )
                return False
            
            # Test natural classification
            if not NLCDCodeMapping.is_natural(41):  # Deciduous Forest
                self.log_test_result(
                    "NLCD Code Mapping",
                    False,
                    "Natural classification failed for code 41"
                )
                return False
            
            self.log_test_result(
                "NLCD Code Mapping",
                True,
                f"All {len(test_codes)} test codes mapped correctly"
            )
            return True
            
        except Exception as e:
            self.log_test_result(
                "NLCD Code Mapping",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_nlcd_api_client(self) -> bool:
        """Test NLCD API client functionality"""
        try:
            client = NLCDAPIClient(rate_limit_delay=0.5)  # Faster for testing
            
            # Test point sampling with known coordinates (Texas agricultural area)
            test_lon, test_lat = -95.0526, 34.4535  # Austin, TX area
            
            result = client.get_feature_info(test_lon, test_lat, year=2021)
            
            if not result.get('success'):
                self.log_test_result(
                    "NLCD API Client",
                    False,
                    "Point sampling failed",
                    result
                )
                return False
            
            # Validate response structure
            required_fields = ['nlcd_code', 'land_cover_name', 'land_cover_category']
            missing_fields = [field for field in required_fields if field not in result]
            
            if missing_fields:
                self.log_test_result(
                    "NLCD API Client",
                    False,
                    f"Missing response fields: {missing_fields}",
                    result
                )
                return False
            
            self.log_test_result(
                "NLCD API Client",
                True,
                f"Point sampling successful: {result['land_cover_name']} (code {result['nlcd_code']})",
                result
            )
            return True
            
        except Exception as e:
            self.log_test_result(
                "NLCD API Client",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_snowflake_integration(self) -> bool:
        """Test NLCD Snowflake integration"""
        try:
            integration = get_nlcd_integration(self.environment)
            
            # Test with a sample parcel (create test data if needed)
            test_parcel_id = f"test_nlcd_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # First, insert a test parcel into parcel_profiles
            with get_snowflake_connector(self.environment) as sf:
                # Check if we have any existing parcels to use for testing
                existing_parcels = sf.execute_query("""
                    SELECT PARCEL_ID, LATITUDE, LONGITUDE, ACRES 
                    FROM TEDDY_DATA.RAW.PARCEL_PROFILES 
                    WHERE LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL 
                    LIMIT 1
                """)
                
                if existing_parcels:
                    # Use existing parcel for testing
                    test_parcel = existing_parcels[0]
                    test_parcel_id = test_parcel['PARCEL_ID']
                    
                    # Process the parcel
                    result = integration.process_parcel_land_cover(
                        parcel_id=test_parcel_id,
                        year=2021,
                        force_refresh=True  # Force refresh for testing
                    )
                    
                    if not result['success']:
                        self.log_test_result(
                            "Snowflake Integration",
                            False,
                            f"Parcel processing failed: {result.get('error', 'Unknown error')}",
                            result
                        )
                        return False
                    
                    # Verify data was stored
                    stored_data = integration.get_parcel_land_cover_json(test_parcel_id)
                    
                    if not stored_data:
                        self.log_test_result(
                            "Snowflake Integration",
                            False,
                            "No data found after processing",
                            result
                        )
                        return False
                    
                    # Validate stored data structure
                    if 'summary' not in stored_data or 'classes' not in stored_data:
                        self.log_test_result(
                            "Snowflake Integration",
                            False,
                            "Invalid stored data structure",
                            stored_data
                        )
                        return False
                    
                    self.log_test_result(
                        "Snowflake Integration",
                        True,
                        f"Successfully processed and stored data for parcel {test_parcel_id}",
                        {
                            'parcel_id': test_parcel_id,
                            'processing_method': result.get('processing_method'),
                            'dominant_class': stored_data['summary']['dominant_class'] if stored_data.get('summary') else None
                        }
                    )
                    return True
                    
                else:
                    self.log_test_result(
                        "Snowflake Integration",
                        False,
                        "No test parcels available in database"
                    )
                    return False
                    
        except Exception as e:
            self.log_test_result(
                "Snowflake Integration",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_database_functions(self) -> bool:
        """Test database stored procedures and functions"""
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Test the JSON function with a sample parcel
                parcels = sf.execute_query("""
                    SELECT PARCEL_ID 
                    FROM TEDDY_DATA.LAND_COVER.LAND_COVER_SUMMARY 
                    LIMIT 1
                """)
                
                if not parcels:
                    self.log_test_result(
                        "Database Functions",
                        True,
                        "No processed parcels available for function testing (this is OK for new setup)"
                    )
                    return True
                
                test_parcel_id = parcels[0]['PARCEL_ID']
                
                # Test the JSON function
                json_result = sf.execute_query("""
                    SELECT TEDDY_DATA.LAND_COVER.FN_GET_LAND_COVER_JSON(%s) as json_data
                """, {'1': test_parcel_id})
                
                if not json_result or not json_result[0]['JSON_DATA']:
                    self.log_test_result(
                        "Database Functions",
                        False,
                        "JSON function returned no data"
                    )
                    return False
                
                # Validate JSON structure
                json_data = json_result[0]['JSON_DATA']
                if 'parcel_id' not in json_data or 'summary' not in json_data:
                    self.log_test_result(
                        "Database Functions",
                        False,
                        "Invalid JSON function output structure",
                        json_data
                    )
                    return False
                
                self.log_test_result(
                    "Database Functions",
                    True,
                    f"Database functions working correctly for parcel {test_parcel_id}"
                )
                return True
                
        except Exception as e:
            self.log_test_result(
                "Database Functions",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def test_bulk_processor_dry_run(self) -> bool:
        """Test bulk processor with dry run"""
        try:
            # Import the bulk processor
            sys.path.append(os.path.dirname(__file__))
            from bulk_nlcd_processor import BulkNLCDProcessor
            
            processor = BulkNLCDProcessor(self.environment)
            
            # Test with a small limit and dry run
            result = processor.process_state(
                state_code='TX',
                year=2021,
                batch_size=10,
                dry_run=True,
                limit=50  # Small limit for testing
            )
            
            if not result['success']:
                self.log_test_result(
                    "Bulk Processor Dry Run",
                    False,
                    f"Dry run failed: {result.get('error', 'Unknown error')}",
                    result
                )
                return False
            
            if not result.get('dry_run'):
                self.log_test_result(
                    "Bulk Processor Dry Run",
                    False,
                    "Dry run flag not set correctly"
                )
                return False
            
            self.log_test_result(
                "Bulk Processor Dry Run",
                True,
                f"Dry run successful - would process {result['total_parcels']} parcels",
                result
            )
            return True
            
        except Exception as e:
            self.log_test_result(
                "Bulk Processor Dry Run",
                False,
                f"Exception: {str(e)}",
                str(e)
            )
            return False
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all tests and return summary"""
        logger.info("Starting NLCD Integration Test Suite")
        logger.info("=" * 60)
        
        tests = [
            self.test_snowflake_connection,
            self.test_land_cover_schema,
            self.test_nlcd_code_mapping,
            self.test_nlcd_api_client,
            self.test_snowflake_integration,
            self.test_database_functions,
            self.test_bulk_processor_dry_run
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Test {test.__name__} crashed: {e}")
                failed += 1
        
        # Generate summary
        summary = {
            'total_tests': len(tests),
            'passed': passed,
            'failed': failed,
            'success_rate': (passed / len(tests)) * 100,
            'test_results': self.test_results,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info("=" * 60)
        logger.info("TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Tests: {summary['total_tests']}")
        logger.info(f"Passed: {summary['passed']}")
        logger.info(f"Failed: {summary['failed']}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        
        if failed > 0:
            logger.info("\nFailed Tests:")
            for result in self.test_results:
                if not result['success']:
                    logger.info(f"  - {result['test_name']}: {result['message']}")
        
        return summary

def main():
    """Main function for command-line usage"""
    
    parser = argparse.ArgumentParser(description='NLCD Integration Test Suite')
    parser.add_argument('--environment', default='dev', choices=['dev', 'prod'],
                       help='Environment to test (default: dev)')
    parser.add_argument('--test-all', action='store_true',
                       help='Run all tests')
    parser.add_argument('--test-schema', action='store_true',
                       help='Test database schema only')
    parser.add_argument('--test-api', action='store_true',
                       help='Test NLCD API client only')
    parser.add_argument('--test-integration', action='store_true',
                       help='Test Snowflake integration only')
    parser.add_argument('--output-json', 
                       help='Output results to JSON file')
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = NLCDIntegrationTester(args.environment)
    
    # Run specific tests or all tests
    if args.test_schema:
        tester.test_snowflake_connection()
        tester.test_land_cover_schema()
    elif args.test_api:
        tester.test_nlcd_code_mapping()
        tester.test_nlcd_api_client()
    elif args.test_integration:
        tester.test_snowflake_integration()
        tester.test_database_functions()
    else:
        # Run all tests by default
        summary = tester.run_all_tests()
        
        # Output to JSON if requested
        if args.output_json:
            with open(args.output_json, 'w') as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Results saved to {args.output_json}")
        
        # Exit with error code if tests failed
        if summary['failed'] > 0:
            sys.exit(1)

if __name__ == "__main__":
    main()
