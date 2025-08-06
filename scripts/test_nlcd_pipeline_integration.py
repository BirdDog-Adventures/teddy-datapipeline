#!/usr/bin/env python3
"""
NLCD Pipeline Integration Test

This script tests the complete NLCD pipeline integration to ensure
all components work correctly with the fixed NLCD client.

Tests:
1. Direct NLCD client functionality
2. NLCD parcel processor lambda
3. Unified NLCD processor
4. End-to-end pipeline flow
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any

# Add the lambda directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambda'))

# Import our components
from utils.nlcd_client import get_nlcd_integration, NLCDAPIClient
from utils.unified_nlcd_processor import UnifiedNLCDProcessor, enrich_parcel_with_nlcd
import nlcd_parcel_processor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NLCDPipelineIntegrationTester:
    """Comprehensive NLCD pipeline integration tester"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.test_coordinates = {
            'lon': -95.0526,
            'lat': 34.4535
        }
        self.test_parcel_id = 'test_nlcd_pipeline_001'
        self.results = {}
        
        logger.info(f"🚀 Initializing NLCD Pipeline Integration Tester for environment: {environment}")
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all integration tests"""
        
        logger.info("=" * 60)
        logger.info("🧪 NLCD PIPELINE INTEGRATION TEST SUITE")
        logger.info("=" * 60)
        
        tests = [
            ('Direct NLCD API Client', self.test_direct_nlcd_api),
            ('NLCD Integration Client', self.test_nlcd_integration_client),
            ('NLCD Parcel Processor Lambda', self.test_nlcd_parcel_processor),
            ('Unified NLCD Processor', self.test_unified_nlcd_processor),
            ('End-to-End Pipeline Flow', self.test_end_to_end_pipeline)
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            logger.info(f"\n📋 Running test: {test_name}")
            try:
                result = test_func()
                if result.get('success', False):
                    logger.info(f"✅ {test_name}: PASSED")
                    passed += 1
                else:
                    logger.error(f"❌ {test_name}: FAILED - {result.get('error', 'Unknown error')}")
                    failed += 1
                
                self.results[test_name] = result
                
            except Exception as e:
                logger.error(f"❌ {test_name}: ERROR - {str(e)}", exc_info=True)
                failed += 1
                self.results[test_name] = {
                    'success': False,
                    'error': str(e)
                }
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 INTEGRATION TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Environment: {self.environment}")
        logger.info(f"Total Tests: {len(tests)}")
        logger.info(f"✅ Passed: {passed}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"Success Rate: {(passed / len(tests) * 100):.1f}%")
        
        if failed == 0:
            logger.info("🎉 ALL TESTS PASSED - NLCD Pipeline is fully functional!")
        else:
            logger.warning("⚠️  Some tests failed - check the logs above for details")
        
        return {
            'summary': {
                'total_tests': len(tests),
                'passed': passed,
                'failed': failed,
                'success_rate': passed / len(tests) * 100
            },
            'results': self.results
        }
    
    def test_direct_nlcd_api(self) -> Dict[str, Any]:
        """Test direct NLCD API client functionality"""
        
        try:
            api_client = NLCDAPIClient()
            
            # Test get_feature_info
            result = api_client.get_feature_info(
                self.test_coordinates['lon'],
                self.test_coordinates['lat'],
                year=2021
            )
            
            if not result.get('success'):
                return {
                    'success': False,
                    'error': 'NLCD API call failed',
                    'details': result
                }
            
            # Verify we got valid NLCD data
            if 'nlcd_code' not in result:
                return {
                    'success': False,
                    'error': 'No NLCD code in response',
                    'details': result
                }
            
            nlcd_code = result['nlcd_code']
            land_cover_name = result.get('land_cover_name', 'Unknown')
            
            logger.info(f"   🎯 NLCD Code: {nlcd_code}")
            logger.info(f"   🎯 Land Cover: {land_cover_name}")
            logger.info(f"   🎯 Category: {result.get('land_cover_category', 'Unknown')}")
            
            return {
                'success': True,
                'nlcd_code': nlcd_code,
                'land_cover_name': land_cover_name,
                'full_result': result
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Direct API test failed: {str(e)}'
            }
    
    def test_nlcd_integration_client(self) -> Dict[str, Any]:
        """Test NLCD integration client with Snowflake"""
        
        try:
            integration = get_nlcd_integration(self.environment)
            
            # Test processing a parcel (this will use point sampling since we don't have geometry)
            result = integration.process_parcel_land_cover(
                parcel_id=self.test_parcel_id,
                year=2021,
                force_refresh=True  # Force refresh for testing
            )
            
            if not result.get('success'):
                return {
                    'success': False,
                    'error': 'Integration client processing failed',
                    'details': result
                }
            
            logger.info(f"   📊 Processing Method: {result.get('processing_method', 'Unknown')}")
            logger.info(f"   📊 Data Source: {result.get('source', 'Unknown')}")
            
            # Try to get detailed JSON data
            try:
                json_data = integration.get_parcel_land_cover_json(self.test_parcel_id)
                logger.info(f"   📊 JSON Data Available: {json_data is not None}")
            except Exception as e:
                logger.warning(f"   ⚠️  JSON data retrieval failed: {str(e)}")
                json_data = None
            
            return {
                'success': True,
                'processing_result': result,
                'json_data_available': json_data is not None
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Integration client test failed: {str(e)}'
            }
    
    def test_nlcd_parcel_processor(self) -> Dict[str, Any]:
        """Test NLCD parcel processor lambda function"""
        
        try:
            # Test single parcel event
            test_event = {
                'parcel_id': self.test_parcel_id,
                'year': 2021,
                'force_refresh': True
            }
            
            # Mock context
            class MockContext:
                def __init__(self):
                    self.function_name = "test-nlcd-parcel-processor"
                    self.aws_request_id = "test-request-id"
            
            context = MockContext()
            
            # Call the lambda handler
            result = nlcd_parcel_processor.lambda_handler(test_event, context)
            
            if result.get('statusCode') != 200:
                return {
                    'success': False,
                    'error': f'Lambda returned status {result.get("statusCode")}',
                    'details': result
                }
            
            # Parse the response body
            try:
                body = json.loads(result['body'])
                if not body.get('success'):
                    return {
                        'success': False,
                        'error': 'Lambda processing failed',
                        'details': body
                    }
                
                logger.info(f"   🔧 Lambda Status: {result['statusCode']}")
                logger.info(f"   🔧 Processing Success: {body.get('success')}")
                logger.info(f"   🔧 Parcel ID: {body.get('parcel_id')}")
                
                return {
                    'success': True,
                    'lambda_response': result,
                    'processing_result': body
                }
                
            except json.JSONDecodeError as e:
                return {
                    'success': False,
                    'error': f'Failed to parse lambda response: {str(e)}',
                    'raw_response': result
                }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Lambda processor test failed: {str(e)}'
            }
    
    def test_unified_nlcd_processor(self) -> Dict[str, Any]:
        """Test unified NLCD processor"""
        
        try:
            processor = UnifiedNLCDProcessor(self.environment)
            
            # Test single parcel processing
            test_event = {
                'parcel_id': self.test_parcel_id,
                'year': 2021,
                'force_refresh': True
            }
            
            result = processor.process_request(test_event)
            
            if not result.get('success'):
                return {
                    'success': False,
                    'error': 'Unified processor failed',
                    'details': result
                }
            
            logger.info(f"   🔄 Unified Processor Success: {result.get('success')}")
            logger.info(f"   🔄 Processing Method: {result.get('processing_method', 'Unknown')}")
            
            # Test convenience function
            try:
                convenience_result = enrich_parcel_with_nlcd(
                    self.test_parcel_id,
                    year=2021,
                    environment=self.environment
                )
                
                convenience_success = convenience_result.get('success', False)
                logger.info(f"   🔄 Convenience Function Success: {convenience_success}")
                
            except Exception as e:
                logger.warning(f"   ⚠️  Convenience function failed: {str(e)}")
                convenience_success = False
            
            return {
                'success': True,
                'unified_result': result,
                'convenience_function_success': convenience_success
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Unified processor test failed: {str(e)}'
            }
    
    def test_end_to_end_pipeline(self) -> Dict[str, Any]:
        """Test complete end-to-end pipeline flow"""
        
        try:
            logger.info("   🌊 Testing complete pipeline flow...")
            
            # Step 1: Direct API call
            api_client = NLCDAPIClient()
            api_result = api_client.get_feature_info(
                self.test_coordinates['lon'],
                self.test_coordinates['lat'],
                year=2021
            )
            
            if not api_result.get('success'):
                return {
                    'success': False,
                    'error': 'Pipeline step 1 (API) failed',
                    'step': 1
                }
            
            # Step 2: Integration processing
            integration = get_nlcd_integration(self.environment)
            integration_result = integration.process_parcel_land_cover(
                parcel_id=self.test_parcel_id,
                year=2021,
                force_refresh=True
            )
            
            if not integration_result.get('success'):
                return {
                    'success': False,
                    'error': 'Pipeline step 2 (Integration) failed',
                    'step': 2
                }
            
            # Step 3: Lambda processing
            test_event = {
                'parcel_id': self.test_parcel_id,
                'year': 2021,
                'force_refresh': False  # Should use cached data from step 2
            }
            
            class MockContext:
                def __init__(self):
                    self.function_name = "test-pipeline"
                    self.aws_request_id = "test-pipeline-request"
            
            lambda_result = nlcd_parcel_processor.lambda_handler(test_event, MockContext())
            
            if lambda_result.get('statusCode') != 200:
                return {
                    'success': False,
                    'error': 'Pipeline step 3 (Lambda) failed',
                    'step': 3
                }
            
            # Step 4: Unified processor
            processor = UnifiedNLCDProcessor(self.environment)
            unified_result = processor.process_request(test_event)
            
            if not unified_result.get('success'):
                return {
                    'success': False,
                    'error': 'Pipeline step 4 (Unified) failed',
                    'step': 4
                }
            
            logger.info("   🌊 All pipeline steps completed successfully!")
            logger.info(f"   📊 API NLCD Code: {api_result.get('nlcd_code')}")
            logger.info(f"   📊 Integration Source: {integration_result.get('source')}")
            logger.info(f"   📊 Lambda Status: {lambda_result.get('statusCode')}")
            logger.info(f"   📊 Unified Success: {unified_result.get('success')}")
            
            return {
                'success': True,
                'pipeline_steps': {
                    'api': api_result,
                    'integration': integration_result,
                    'lambda': lambda_result,
                    'unified': unified_result
                }
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'End-to-end pipeline test failed: {str(e)}'
            }

def main():
    """Main test runner"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='NLCD Pipeline Integration Test')
    parser.add_argument('--environment', '-e', default='dev', 
                       help='Environment to test (dev/prod)')
    parser.add_argument('--output', '-o', 
                       help='Output file for test results (JSON)')
    
    args = parser.parse_args()
    
    # Run the tests
    tester = NLCDPipelineIntegrationTester(args.environment)
    results = tester.run_all_tests()
    
    # Save results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"📄 Test results saved to: {args.output}")
    
    # Exit with appropriate code
    if results['summary']['failed'] == 0:
        logger.info("🎉 All tests passed! NLCD pipeline is ready for production.")
        sys.exit(0)
    else:
        logger.error("❌ Some tests failed. Please review the results above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
