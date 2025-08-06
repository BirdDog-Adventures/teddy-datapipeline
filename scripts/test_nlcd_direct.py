#!/usr/bin/env python3
"""
Direct NLCD API Test

Test the NLCD client directly with specific coordinates to see what's happening.
"""

import sys
import os
import json
import logging

# Add the lambda directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from utils.nlcd_client import NLCDAPIClient

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_nlcd_direct():
    """Test NLCD API directly with the problematic coordinates"""
    
    # The coordinates that were reported as not working
    lon, lat = -95.0526, 34.4535
    
    logger.info(f"🔍 Testing NLCD API directly with coordinates: {lon}, {lat}")
    
    # Create client
    client = NLCDAPIClient(rate_limit_delay=0.5)
    
    try:
        # Test the get_feature_info method (used by the integration)
        logger.info("📋 Testing get_feature_info method...")
        result = client.get_feature_info(lon, lat, year=2021)
        
        logger.info(f"✅ get_feature_info result:")
        logger.info(f"   Success: {result.get('success')}")
        logger.info(f"   Full result: {json.dumps(result, indent=2)}")
        
        if result.get('success'):
            logger.info(f"🎯 NLCD Code: {result.get('nlcd_code')}")
            logger.info(f"🎯 Land Cover: {result.get('land_cover_name')}")
            logger.info(f"🎯 Category: {result.get('land_cover_category')}")
            logger.info(f"🎯 Is Agricultural: {result.get('is_agricultural')}")
            logger.info(f"🎯 Is Natural: {result.get('is_natural')}")
        else:
            logger.error(f"❌ API call failed: {result.get('error')}")
            logger.error(f"❌ Raw response: {result.get('raw_response')}")
        
        # Also test the compatibility method
        logger.info("\n📋 Testing get_land_cover_for_point method...")
        result2 = client.get_land_cover_for_point(lat, lon, year=2021)
        
        logger.info(f"✅ get_land_cover_for_point result:")
        logger.info(f"   Success: {result2.get('success')}")
        logger.info(f"   Full result: {json.dumps(result2, indent=2)}")
        
        return result.get('success', False)
        
    except Exception as e:
        logger.error(f"❌ Exception during API test: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return False

def test_different_years():
    """Test different NLCD years"""
    
    lon, lat = -95.0526, 34.4535
    years = [2021, 2019, 2016]
    
    logger.info(f"\n🔍 Testing different NLCD years for coordinates: {lon}, {lat}")
    
    client = NLCDAPIClient(rate_limit_delay=0.5)
    
    for year in years:
        logger.info(f"\n📋 Testing year {year}...")
        try:
            result = client.get_feature_info(lon, lat, year=year)
            
            if result.get('success'):
                logger.info(f"   ✅ Year {year}: {result.get('land_cover_name')} (code {result.get('nlcd_code')})")
            else:
                logger.info(f"   ❌ Year {year}: Failed - {result.get('error')}")
                
        except Exception as e:
            logger.error(f"   ❌ Year {year}: Exception - {e}")

def main():
    """Main test function"""
    
    logger.info("🚀 Starting Direct NLCD API Test")
    logger.info("=" * 60)
    
    # Test 1: Direct API call
    success = test_nlcd_direct()
    
    # Test 2: Different years
    test_different_years()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST SUMMARY")
    logger.info("=" * 60)
    
    if success:
        logger.info("✅ NLCD API is working correctly!")
        logger.info("💡 The issue is likely in the integration layer or test script logic.")
        logger.info("💡 Check the test_nlcd_integration.py script for processing issues.")
    else:
        logger.info("❌ NLCD API call failed.")
        logger.info("💡 Check network connectivity and API endpoint availability.")

if __name__ == "__main__":
    main()
