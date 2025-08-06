#!/usr/bin/env python3
"""
Debug NLCD API Issues

This script helps diagnose issues with NLCD API calls by testing different
endpoints, parameters, and response formats.
"""

import json
import logging
import requests
import sys
import os
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NLCDAPIDebugger:
    """Debug NLCD API issues"""
    
    def __init__(self):
        self.wms_base_url = "https://www.mrlc.gov/geoserver/mrlc_display/wms"
        self.wfs_base_url = "https://www.mrlc.gov/geoserver/mrlc_display/wfs"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Teddy-DataPipeline-Debug/1.0'
        })
    
    def test_wms_capabilities(self) -> Dict[str, Any]:
        """Test WMS GetCapabilities to see available layers"""
        logger.info("🔍 Testing WMS GetCapabilities...")
        
        params = {
            'service': 'WMS',
            'version': '1.1.1',
            'request': 'GetCapabilities'
        }
        
        try:
            response = self.session.get(self.wms_base_url, params=params, timeout=30)
            response.raise_for_status()
            
            # Look for NLCD layers in the response
            content = response.text
            nlcd_layers = []
            
            # Simple text search for NLCD layers
            lines = content.split('\n')
            for line in lines:
                if 'NLCD' in line and ('2021' in line or '2019' in line or '2016' in line):
                    nlcd_layers.append(line.strip())
            
            logger.info(f"✅ WMS Capabilities successful. Found {len(nlcd_layers)} NLCD layers")
            for layer in nlcd_layers[:5]:  # Show first 5
                logger.info(f"   - {layer}")
            
            return {
                'success': True,
                'layers_found': len(nlcd_layers),
                'sample_layers': nlcd_layers[:10],
                'full_response_length': len(content)
            }
            
        except Exception as e:
            logger.error(f"❌ WMS Capabilities failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def test_different_layer_names(self, lon: float, lat: float) -> Dict[str, Any]:
        """Test different NLCD layer name formats"""
        logger.info(f"🔍 Testing different layer names for coordinates {lon}, {lat}...")
        
        # Different possible layer name formats
        layer_formats = [
            'NLCD_2021_Land_Cover_L48',
            'NLCD_2019_Land_Cover_L48',
            'NLCD_2016_Land_Cover_L48',
            'mrlc_display:NLCD_2021_Land_Cover_L48',
            'mrlc_display:NLCD_2019_Land_Cover_L48',
            'NLCD_2021_Land_Cover',
            'NLCD_2019_Land_Cover',
            'nlcd_2021_land_cover_l48',
            'nlcd_2019_land_cover_l48'
        ]
        
        results = {}
        
        for layer_name in layer_formats:
            logger.info(f"   Testing layer: {layer_name}")
            
            buffer = 0.001
            bbox = (lon - buffer, lat - buffer, lon + buffer, lat + buffer)
            
            params = {
                'service': 'WMS',
                'version': '1.1.1',
                'request': 'GetFeatureInfo',
                'layers': layer_name,
                'query_layers': layer_name,
                'styles': '',
                'bbox': ','.join(map(str, bbox)),
                'width': 101,
                'height': 101,
                'srs': 'EPSG:4326',
                'format': 'image/png',
                'info_format': 'application/json',
                'x': 50,
                'y': 50
            }
            
            try:
                response = self.session.get(self.wms_base_url, params=params, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        results[layer_name] = {
                            'success': True,
                            'status_code': response.status_code,
                            'content_type': response.headers.get('content-type'),
                            'has_features': len(data.get('features', [])) > 0,
                            'response_data': data
                        }
                        logger.info(f"      ✅ Success - {len(data.get('features', []))} features")
                    except json.JSONDecodeError:
                        results[layer_name] = {
                            'success': True,
                            'status_code': response.status_code,
                            'content_type': response.headers.get('content-type'),
                            'response_text': response.text[:200] + '...' if len(response.text) > 200 else response.text
                        }
                        logger.info(f"      ⚠️  Non-JSON response: {response.headers.get('content-type')}")
                else:
                    results[layer_name] = {
                        'success': False,
                        'status_code': response.status_code,
                        'error': response.text[:200]
                    }
                    logger.info(f"      ❌ HTTP {response.status_code}")
                    
            except Exception as e:
                results[layer_name] = {
                    'success': False,
                    'error': str(e)
                }
                logger.info(f"      ❌ Exception: {e}")
        
        return results
    
    def test_different_info_formats(self, lon: float, lat: float) -> Dict[str, Any]:
        """Test different info_format options"""
        logger.info(f"🔍 Testing different info formats for coordinates {lon}, {lat}...")
        
        info_formats = [
            'application/json',
            'text/plain',
            'text/html',
            'application/vnd.ogc.gml',
            'text/xml'
        ]
        
        results = {}
        layer_name = 'NLCD_2021_Land_Cover_L48'  # Use most likely layer name
        
        buffer = 0.001
        bbox = (lon - buffer, lat - buffer, lon + buffer, lat + buffer)
        
        for info_format in info_formats:
            logger.info(f"   Testing format: {info_format}")
            
            params = {
                'service': 'WMS',
                'version': '1.1.1',
                'request': 'GetFeatureInfo',
                'layers': layer_name,
                'query_layers': layer_name,
                'styles': '',
                'bbox': ','.join(map(str, bbox)),
                'width': 101,
                'height': 101,
                'srs': 'EPSG:4326',
                'format': 'image/png',
                'info_format': info_format,
                'x': 50,
                'y': 50
            }
            
            try:
                response = self.session.get(self.wms_base_url, params=params, timeout=30)
                
                results[info_format] = {
                    'success': response.status_code == 200,
                    'status_code': response.status_code,
                    'content_type': response.headers.get('content-type'),
                    'content_length': len(response.text),
                    'response_preview': response.text[:300] + '...' if len(response.text) > 300 else response.text
                }
                
                if response.status_code == 200:
                    logger.info(f"      ✅ Success - {len(response.text)} chars")
                else:
                    logger.info(f"      ❌ HTTP {response.status_code}")
                    
            except Exception as e:
                results[info_format] = {
                    'success': False,
                    'error': str(e)
                }
                logger.info(f"      ❌ Exception: {e}")
        
        return results
    
    def test_coordinate_variations(self, base_lon: float, base_lat: float) -> Dict[str, Any]:
        """Test slight variations in coordinates to see if location matters"""
        logger.info(f"🔍 Testing coordinate variations around {base_lon}, {base_lat}...")
        
        # Test points in a small grid around the base coordinates
        offsets = [-0.01, -0.005, 0, 0.005, 0.01]
        results = {}
        
        layer_name = 'NLCD_2021_Land_Cover_L48'
        
        for lon_offset in offsets:
            for lat_offset in offsets:
                test_lon = base_lon + lon_offset
                test_lat = base_lat + lat_offset
                
                coord_key = f"{test_lon:.4f},{test_lat:.4f}"
                logger.info(f"   Testing: {coord_key}")
                
                buffer = 0.001
                bbox = (test_lon - buffer, test_lat - buffer, test_lon + buffer, test_lat + buffer)
                
                params = {
                    'service': 'WMS',
                    'version': '1.1.1',
                    'request': 'GetFeatureInfo',
                    'layers': layer_name,
                    'query_layers': layer_name,
                    'styles': '',
                    'bbox': ','.join(map(str, bbox)),
                    'width': 101,
                    'height': 101,
                    'srs': 'EPSG:4326',
                    'format': 'image/png',
                    'info_format': 'application/json',
                    'x': 50,
                    'y': 50
                }
                
                try:
                    response = self.session.get(self.wms_base_url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            features = data.get('features', [])
                            
                            results[coord_key] = {
                                'success': True,
                                'has_features': len(features) > 0,
                                'feature_count': len(features),
                                'response_data': data
                            }
                            
                            if features:
                                logger.info(f"      ✅ Found {len(features)} features")
                            else:
                                logger.info(f"      ⚠️  No features found")
                                
                        except json.JSONDecodeError:
                            results[coord_key] = {
                                'success': True,
                                'json_error': True,
                                'response_text': response.text[:100]
                            }
                            logger.info(f"      ⚠️  JSON decode error")
                    else:
                        results[coord_key] = {
                            'success': False,
                            'status_code': response.status_code
                        }
                        logger.info(f"      ❌ HTTP {response.status_code}")
                        
                except Exception as e:
                    results[coord_key] = {
                        'success': False,
                        'error': str(e)
                    }
                    logger.info(f"      ❌ Exception: {e}")
        
        return results
    
    def test_direct_url_access(self, lon: float, lat: float) -> Dict[str, Any]:
        """Test direct URL construction to see the exact request being made"""
        logger.info(f"🔍 Testing direct URL construction for {lon}, {lat}...")
        
        buffer = 0.001
        bbox = (lon - buffer, lat - buffer, lon + buffer, lat + buffer)
        
        params = {
            'service': 'WMS',
            'version': '1.1.1',
            'request': 'GetFeatureInfo',
            'layers': 'NLCD_2021_Land_Cover_L48',
            'query_layers': 'NLCD_2021_Land_Cover_L48',
            'styles': '',
            'bbox': ','.join(map(str, bbox)),
            'width': 101,
            'height': 101,
            'srs': 'EPSG:4326',
            'format': 'image/png',
            'info_format': 'application/json',
            'x': 50,
            'y': 50
        }
        
        # Construct the full URL
        param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{self.wms_base_url}?{param_string}"
        
        logger.info(f"Full URL: {full_url}")
        
        try:
            response = self.session.get(full_url, timeout=30)
            
            result = {
                'url': full_url,
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'content_length': len(response.text),
                'response_preview': response.text[:500] + '...' if len(response.text) > 500 else response.text
            }
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    result['json_data'] = data
                    result['feature_count'] = len(data.get('features', []))
                    logger.info(f"✅ Success - {result['feature_count']} features found")
                except json.JSONDecodeError:
                    result['json_error'] = True
                    logger.info(f"⚠️  Response is not JSON")
            else:
                logger.info(f"❌ HTTP {response.status_code}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Request failed: {e}")
            return {
                'url': full_url,
                'error': str(e)
            }
    
    def run_full_diagnosis(self, lon: float = -95.0526, lat: float = 34.4535) -> Dict[str, Any]:
        """Run complete diagnosis of NLCD API issues"""
        logger.info("🚀 Starting NLCD API Diagnosis")
        logger.info("=" * 60)
        
        results = {
            'coordinates': {'lon': lon, 'lat': lat},
            'tests': {}
        }
        
        # Test 1: WMS Capabilities
        logger.info("\n📋 Test 1: WMS Capabilities")
        results['tests']['capabilities'] = self.test_wms_capabilities()
        
        # Test 2: Different layer names
        logger.info("\n📋 Test 2: Different Layer Names")
        results['tests']['layer_names'] = self.test_different_layer_names(lon, lat)
        
        # Test 3: Different info formats
        logger.info("\n📋 Test 3: Different Info Formats")
        results['tests']['info_formats'] = self.test_different_info_formats(lon, lat)
        
        # Test 4: Coordinate variations
        logger.info("\n📋 Test 4: Coordinate Variations")
        results['tests']['coordinate_variations'] = self.test_coordinate_variations(lon, lat)
        
        # Test 5: Direct URL access
        logger.info("\n📋 Test 5: Direct URL Access")
        results['tests']['direct_url'] = self.test_direct_url_access(lon, lat)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 DIAGNOSIS SUMMARY")
        logger.info("=" * 60)
        
        # Find working configurations
        working_layers = []
        for layer, result in results['tests']['layer_names'].items():
            if result.get('success') and result.get('has_features'):
                working_layers.append(layer)
        
        working_formats = []
        for format_name, result in results['tests']['info_formats'].items():
            if result.get('success') and result.get('content_length', 0) > 50:
                working_formats.append(format_name)
        
        working_coords = []
        for coord, result in results['tests']['coordinate_variations'].items():
            if result.get('success') and result.get('has_features'):
                working_coords.append(coord)
        
        logger.info(f"✅ Working layer names: {working_layers}")
        logger.info(f"✅ Working info formats: {working_formats}")
        logger.info(f"✅ Working coordinates: {working_coords}")
        
        if not working_layers and not working_coords:
            logger.info("❌ No working configurations found!")
            logger.info("💡 Recommendations:")
            logger.info("   1. Check if NLCD service is operational")
            logger.info("   2. Try different coordinates (maybe outside coverage area)")
            logger.info("   3. Check NLCD documentation for current layer names")
            logger.info("   4. Consider using alternative NLCD data sources")
        
        return results

def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Debug NLCD API Issues')
    parser.add_argument('--lon', type=float, default=-95.0526,
                       help='Longitude to test (default: -95.0526)')
    parser.add_argument('--lat', type=float, default=34.4535,
                       help='Latitude to test (default: 34.4535)')
    parser.add_argument('--output-json', 
                       help='Output results to JSON file')
    
    args = parser.parse_args()
    
    # Run diagnosis
    debugger = NLCDAPIDebugger()
    results = debugger.run_full_diagnosis(args.lon, args.lat)
    
    # Save results if requested
    if args.output_json:
        with open(args.output_json, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {args.output_json}")

if __name__ == "__main__":
    main()
