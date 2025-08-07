"""
NLCD (National Land Cover Database) Client for Teddy Data Pipeline

This client provides access to NLCD land cover data through web services
and integrates with the existing Snowflake infrastructure for data storage.

Adapted from the original NLCD client to work with the teddy-datapipeline project.
"""

import json
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
import requests
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from .snowflake_connector import get_snowflake_connector

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class NLCDLandCoverData:
    """Container for NLCD land cover response"""
    parcel_id: str
    year: int
    land_cover_classes: Dict[int, Dict[str, Any]]
    dominant_class: Dict[str, Any]
    total_area: float
    processing_method: str
    raw_response: Dict

class NLCDCodeMapping:
    """NLCD land cover code mappings"""
    
    LAND_COVER_CODES = {
        11: {"name": "Open Water", "category": "Water", "color": "#476BA0"},
        12: {"name": "Perennial Ice/Snow", "category": "Ice/Snow", "color": "#D1DDF9"},
        21: {"name": "Developed, Open Space", "category": "Developed", "color": "#DECACA"},
        22: {"name": "Developed, Low Intensity", "category": "Developed", "color": "#D99482"},
        23: {"name": "Developed, Medium Intensity", "category": "Developed", "color": "#EB0000"},
        24: {"name": "Developed, High Intensity", "category": "Developed", "color": "#AB0000"},
        31: {"name": "Barren Land (Rock/Sand/Clay)", "category": "Barren", "color": "#B3AC9F"},
        41: {"name": "Deciduous Forest", "category": "Forest", "color": "#68AB5F"},
        42: {"name": "Evergreen Forest", "category": "Forest", "color": "#1C5F2C"},
        43: {"name": "Mixed Forest", "category": "Forest", "color": "#B5C58F"},
        51: {"name": "Dwarf Scrub", "category": "Shrubland", "color": "#AF963C"},
        52: {"name": "Shrub/Scrub", "category": "Shrubland", "color": "#CCB879"},
        71: {"name": "Grassland/Herbaceous", "category": "Herbaceous", "color": "#DFDFC2"},
        72: {"name": "Sedge/Herbaceous", "category": "Herbaceous", "color": "#D1D182"},
        73: {"name": "Lichens", "category": "Herbaceous", "color": "#A3CC51"},
        74: {"name": "Moss", "category": "Herbaceous", "color": "#82BA9E"},
        81: {"name": "Pasture/Hay", "category": "Planted/Cultivated", "color": "#DCD939"},
        82: {"name": "Cultivated Crops", "category": "Planted/Cultivated", "color": "#AB6C28"},
        90: {"name": "Woody Wetlands", "category": "Wetlands", "color": "#B8D9EB"},
        95: {"name": "Emergent Herbaceous Wetlands", "category": "Wetlands", "color": "#6C9FB8"}
    }
    
    @classmethod
    def get_land_cover_info(cls, code: int) -> Dict[str, str]:
        """Get land cover name and category for NLCD code"""
        return cls.LAND_COVER_CODES.get(code, {"name": f"Unknown_{code}", "category": "Unknown", "color": "#000000"})
    
    @classmethod
    def is_agricultural(cls, code: int) -> bool:
        """Check if NLCD code represents agricultural land use"""
        agricultural_codes = {81, 82}  # Pasture/Hay, Cultivated Crops
        return code in agricultural_codes
    
    @classmethod
    def is_natural(cls, code: int) -> bool:
        """Check if NLCD code represents natural land cover"""
        natural_categories = {"Forest", "Shrubland", "Herbaceous", "Wetlands", "Water"}
        land_cover_info = cls.get_land_cover_info(code)
        return land_cover_info["category"] in natural_categories

class NLCDAPIClient:
    """Client for NLCD Web Services"""
    
    def __init__(self, rate_limit_delay: float = 1.0):
        # NLCD Web Services endpoints
        self.wms_base_url = "https://www.mrlc.gov/geoserver/mrlc_display/wms"
        self.wfs_base_url = "https://www.mrlc.gov/geoserver/mrlc_display/wfs"
        self.rest_base_url = "https://www.mrlc.gov/api"
        
        self.session = requests.Session()
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        
        # Set user agent
        self.session.headers.update({
            'User-Agent': 'Teddy-DataPipeline/1.0'
        })
    
    def _rate_limit(self):
        """Implement rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _make_request(self, url: str, params: Dict) -> requests.Response:
        """Make rate-limited API request"""
        self._rate_limit()
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_feature_info(self, lon: float, lat: float, year: int = 2021) -> Dict:
        """
        Get land cover information for a specific point using GetFeatureInfo
        
        Args:
            lon: Longitude
            lat: Latitude  
            year: NLCD year
            
        Returns:
            Dictionary with feature information
        """
        
        # Create a small bbox around the point
        buffer = 0.001  # Small buffer for point query
        bbox = (lon - buffer, lat - buffer, lon + buffer, lat + buffer)
        
        params = {
            'service': 'WMS',
            'version': '1.1.1',
            'request': 'GetFeatureInfo',
            'layers': f'NLCD_{year}_Land_Cover_L48',
            'query_layers': f'NLCD_{year}_Land_Cover_L48',
            'styles': '',
            'bbox': ','.join(map(str, bbox)),
            'width': 101,
            'height': 101,
            'srs': 'EPSG:4326',
            'format': 'image/png',
            'info_format': 'application/json',
            'x': 50,  # Center of 101x101 image
            'y': 50
        }
        
        try:
            response = self._make_request(self.wms_base_url, params)
            
            # Try to parse as JSON
            try:
                data = response.json()
                return self._process_feature_info_response(data, lon, lat, year)
            except json.JSONDecodeError:
                # If not JSON, return raw text
                return {
                    'success': True,
                    'lon': lon,
                    'lat': lat,
                    'year': year,
                    'raw_response': response.text
                }
            
        except Exception as e:
            logger.error(f"Failed to get feature info: {e}")
            raise
    
    def get_land_cover_wfs(self, geometry: Dict, year: int = 2021) -> NLCDLandCoverData:
        """
        Get land cover data via WFS (Web Feature Service)
        
        Args:
            geometry: GeoJSON geometry (Polygon)
            year: NLCD year
            
        Returns:
            NLCDLandCoverData object with land cover data
        """
        
        # Convert geometry to WKT format for CQL filter
        if geometry['type'] == 'Polygon':
            coords = geometry['coordinates'][0]
            coord_pairs = [f"{coord[0]} {coord[1]}" for coord in coords]
            wkt_geometry = f"POLYGON(({', '.join(coord_pairs)}))"
        else:
            raise ValueError("Only Polygon geometries are supported")
        
        params = {
            'service': 'WFS',
            'version': '1.0.0',
            'request': 'GetFeature',
            'typeName': f'NLCD_{year}_Land_Cover_L48',
            'outputFormat': 'application/json',
            'CQL_FILTER': f"INTERSECTS(the_geom, {wkt_geometry})"
        }
        
        try:
            response = self._make_request(self.wfs_base_url, params)
            data = response.json()
            
            # Process the response
            return self._process_wfs_response(data, geometry, year)
            
        except Exception as e:
            logger.error(f"Failed to get WFS data: {e}")
            raise
    
    def _process_wfs_response(self, data: Dict, geometry: Dict, year: int) -> NLCDLandCoverData:
        """Process WFS API response into structured data"""
        
        features = data.get('features', [])
        
        if not features:
            logger.warning("No NLCD data found for geometry")
            return NLCDLandCoverData(
                parcel_id="unknown",
                year=year,
                land_cover_classes={},
                dominant_class={"code": 0, "name": "No Data", "percentage": 0},
                total_area=0.0,
                processing_method="wfs",
                raw_response=data
            )
        
        # Calculate land cover statistics
        land_cover_classes = {}
        total_area = 0.0
        
        for feature in features:
            properties = feature.get('properties', {})
            
            # Extract land cover code (field name may vary)
            nlcd_code = None
            for field in ['NLCD_Land_Cover_Class', 'landcover', 'class', 'value']:
                if field in properties:
                    nlcd_code = properties[field]
                    break
            
            if nlcd_code is None:
                continue
                
            area = properties.get('area', properties.get('Shape_Area', 1.0))
            
            if nlcd_code not in land_cover_classes:
                land_cover_info = NLCDCodeMapping.get_land_cover_info(nlcd_code)
                land_cover_classes[nlcd_code] = {
                    "name": land_cover_info["name"],
                    "category": land_cover_info["category"],
                    "color": land_cover_info["color"],
                    "area": 0.0,
                    "count": 0
                }
            
            land_cover_classes[nlcd_code]["area"] += area
            land_cover_classes[nlcd_code]["count"] += 1
            total_area += area
        
        # Calculate percentages and find dominant class
        dominant_class = {"code": 0, "name": "No Data", "percentage": 0.0}
        
        for code, data in land_cover_classes.items():
            percentage = (data["area"] / total_area * 100) if total_area > 0 else 0
            data["percentage"] = percentage
            
            if percentage > dominant_class["percentage"]:
                dominant_class = {
                    "code": code,
                    "name": data["name"],
                    "category": data["category"],
                    "percentage": percentage
                }
        
        return NLCDLandCoverData(
            parcel_id="unknown",  # Will be set by caller
            year=year,
            land_cover_classes=land_cover_classes,
            dominant_class=dominant_class,
            total_area=total_area,
            processing_method="wfs",
            raw_response=data
        )
    
    def _process_feature_info_response(self, data: Dict, lon: float, lat: float, year: int) -> Dict:
        """Process GetFeatureInfo response"""
        
        # Extract land cover information from response
        features = data.get('features', [])
        
        if features:
            feature = features[0]
            properties = feature.get('properties', {})
            
            # Extract land cover code
            nlcd_code = None
            for field in ['NLCD_Land_Cover_Class', 'landcover', 'class', 'value', 'pixel_value', 'PALETTE_INDEX']:
                if field in properties:
                    nlcd_code = properties[field]
                    break
            
            if nlcd_code:
                land_cover_info = NLCDCodeMapping.get_land_cover_info(nlcd_code)
                
                return {
                    'success': True,
                    'lon': lon,
                    'lat': lat,
                    'year': year,
                    'nlcd_code': nlcd_code,
                    'land_cover_name': land_cover_info["name"],
                    'land_cover_category': land_cover_info["category"],
                    'color': land_cover_info["color"],
                    'is_agricultural': NLCDCodeMapping.is_agricultural(nlcd_code),
                    'is_natural': NLCDCodeMapping.is_natural(nlcd_code),
                    'properties': properties
                }
        
        return {
            'success': False,
            'lon': lon,
            'lat': lat,
            'year': year,
            'error': 'No land cover data found',
            'raw_response': data
        }
    
    def get_land_cover_for_point(self, lat: float, lon: float, year: int = 2021) -> Dict:
        """
        Get land cover information for a specific point (for test compatibility)
        
        Args:
            lat: Latitude
            lon: Longitude  
            year: NLCD year
            
        Returns:
            Dictionary with land cover information
        """
        return self.get_feature_info(lon, lat, year)

class NLCDSnowflakeIntegration:
    """Integration layer between NLCD API and Snowflake database"""
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.environ.get('ENVIRONMENT', 'dev')
        self.api_client = NLCDAPIClient()
    
    def bulk_process_parcels(self, parcel_ids: List[str], year: int = 2021, force_refresh: bool = False) -> Dict:
        """
        Process land cover data for multiple parcels in bulk
        
        Args:
            parcel_ids: List of parcel identifiers
            year: NLCD year to process
            force_refresh: Force refresh even if recent data exists
            
        Returns:
            Dictionary with bulk processing results
        """
        results = {
            'total_parcels': len(parcel_ids),
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'results': [],
            'errors': []
        }
        
        logger.info(f"Starting bulk processing of {len(parcel_ids)} parcels")
        
        for i, parcel_id in enumerate(parcel_ids):
            try:
                logger.info(f"Processing parcel {i+1}/{len(parcel_ids)}: {parcel_id}")
                
                result = self.process_parcel_land_cover(parcel_id, year, force_refresh)
                
                if result['success']:
                    if result.get('source') == 'cache':
                        results['skipped'] += 1
                    else:
                        results['successful'] += 1
                else:
                    results['failed'] += 1
                    results['errors'].append({
                        'parcel_id': parcel_id,
                        'error': result.get('error', 'Unknown error')
                    })
                
                results['results'].append(result)
                
                # Add small delay to avoid overwhelming the API
                if i < len(parcel_ids) - 1:  # Don't sleep after the last parcel
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error processing parcel {parcel_id}: {e}")
                results['failed'] += 1
                results['errors'].append({
                    'parcel_id': parcel_id,
                    'error': str(e)
                })
        
        logger.info(f"Bulk processing complete: {results['successful']} successful, {results['failed']} failed, {results['skipped']} skipped")
        
        return results
        
    def get_parcel_geometry(self, parcel_id: str) -> Optional[Dict]:
        """
        Get parcel geometry from the parcel_profile table
        
        Args:
            parcel_id: Unique parcel identifier
            
        Returns:
            Dictionary with geometry and coordinates, or None if not found
        """
        try:
            with get_snowflake_connector(self.environment) as sf:
                query = """
                    SELECT 
                        PARCEL_ID,
                        LATITUDE,
                        LONGITUDE,
                        ACRES
                    FROM TEDDY_DATA.CURATED.PARCEL_PROFILES 
                    WHERE PARCEL_ID = %s
                """
                
                results = sf.execute_query(query, {'1': parcel_id})
                
                if not results:
                    logger.warning(f"No parcel found with ID: {parcel_id}")
                    return None
                
                parcel = results[0]
                
                # Return parcel data with geometry
                return {
                    'parcel_id': parcel['PARCEL_ID'],
                    'latitude': float(parcel['LATITUDE']) if parcel['LATITUDE'] else None,
                    'longitude': float(parcel['LONGITUDE']) if parcel['LONGITUDE'] else None,
                    'geometry': None,  # No geometry data available in current schema
                    'acres': float(parcel['ACRES']) if parcel['ACRES'] else None
                }
                
        except Exception as e:
            logger.error(f"Error getting parcel geometry: {e}")
            return None
    
    def check_existing_land_cover(self, parcel_id: str, max_age_days: int = 30) -> Optional[Dict]:
        """
        Check if land cover data already exists for a parcel
        
        Args:
            parcel_id: Unique parcel identifier
            max_age_days: Maximum age of data to consider valid
            
        Returns:
            Existing land cover summary or None
        """
        try:
            with get_snowflake_connector(self.environment) as sf:
                cutoff_date = datetime.now() - timedelta(days=max_age_days)
                
                query = """
                    SELECT 
                        PARCEL_ID,
                        NLCD_YEAR,
                        DOMINANT_CLASS_NAME,
                        DOMINANT_CLASS_PERCENTAGE,
                        IS_AGRICULTURAL,
                        IS_NATURAL,
                        IS_DEVELOPED,
                        AGRICULTURAL_PERCENTAGE,
                        NATURAL_PERCENTAGE,
                        DEVELOPED_PERCENTAGE,
                        LAST_PROCESSED_AT
                    FROM TEDDY_DATA.LAND_COVER.PARCEL_LAND_COVER_SUMMARY 
                    WHERE PARCEL_ID = %s 
                    AND LAST_PROCESSED_AT > %s
                """
                
                results = sf.execute_query(query, {'1': parcel_id, '2': cutoff_date.isoformat()})
                
                if results:
                    return results[0]
                
                return None
                
        except Exception as e:
            logger.error(f"Error checking existing land cover: {e}")
            return None
    
    def process_parcel_land_cover(self, parcel_id: str, year: int = 2021, force_refresh: bool = False) -> Dict:
        """
        Process land cover data for a single parcel
        
        Args:
            parcel_id: Unique parcel identifier
            year: NLCD year to process
            force_refresh: Force refresh even if recent data exists
            
        Returns:
            Dictionary with processing results
        """
        try:
            # Check for existing data unless force refresh
            if not force_refresh:
                existing = self.check_existing_land_cover(parcel_id)
                if existing:
                    logger.info(f"Using existing land cover data for parcel {parcel_id}")
                    return {
                        'success': True,
                        'parcel_id': parcel_id,
                        'source': 'cache',
                        'data': existing
                    }
            
            # Get parcel geometry
            parcel_data = self.get_parcel_geometry(parcel_id)
            if not parcel_data:
                return {
                    'success': False,
                    'parcel_id': parcel_id,
                    'error': 'Parcel not found'
                }
            
            # Determine processing method based on parcel size and available data
            processing_method = 'point_sample'  # Default to point sampling
            nlcd_data = None
            
            # Try polygon analysis for larger parcels if geometry is available
            if parcel_data.get('geometry') and parcel_data.get('acres', 0) >= 1.0:
                try:
                    nlcd_data = self.api_client.get_land_cover_wfs(parcel_data['geometry'], year)
                    nlcd_data.parcel_id = parcel_id
                    processing_method = 'polygon'
                    logger.info(f"Successfully processed parcel {parcel_id} using polygon method")
                except Exception as wfs_error:
                    logger.warning(f"WFS failed for {parcel_id}, falling back to point sampling: {wfs_error}")
            
            # Fall back to point sampling if WFS failed or for small parcels
            if nlcd_data is None:
                if parcel_data['latitude'] and parcel_data['longitude']:
                    point_data = self.api_client.get_feature_info(
                        parcel_data['longitude'], 
                        parcel_data['latitude'], 
                        year
                    )
                    
                    if point_data.get('success'):
                        # Convert point data to NLCDLandCoverData format
                        nlcd_data = self._create_nlcd_data_from_point(point_data, parcel_id, year)
                        processing_method = 'point_sample'
                        logger.info(f"Successfully processed parcel {parcel_id} using point sampling")
                    else:
                        return {
                            'success': False,
                            'parcel_id': parcel_id,
                            'error': 'No land cover data available'
                        }
                else:
                    return {
                        'success': False,
                        'parcel_id': parcel_id,
                        'error': 'No coordinates available for parcel'
                    }
            
            # Store results in Snowflake
            self._store_land_cover_data(nlcd_data, processing_method, parcel_data.get('acres'))
            
            # Get the stored summary
            summary = self.check_existing_land_cover(parcel_id, max_age_days=1)
            
            return {
                'success': True,
                'parcel_id': parcel_id,
                'source': 'api',
                'processing_method': processing_method,
                'data': summary
            }
            
        except Exception as e:
            logger.error(f"Error processing parcel land cover for {parcel_id}: {e}")
            return {
                'success': False,
                'parcel_id': parcel_id,
                'error': str(e)
            }
    
    def _create_nlcd_data_from_point(self, point_data: Dict, parcel_id: str, year: int) -> NLCDLandCoverData:
        """Create NLCDLandCoverData from point sampling result"""
        
        nlcd_code = point_data['nlcd_code']
        land_cover_classes = {
            nlcd_code: {
                "name": point_data['land_cover_name'],
                "category": point_data['land_cover_category'],
                "color": point_data['color'],
                "area": 1.0,  # Normalized area for point sample
                "count": 1,
                "percentage": 100.0
            }
        }
        
        dominant_class = {
            "code": nlcd_code,
            "name": point_data['land_cover_name'],
            "category": point_data['land_cover_category'],
            "percentage": 100.0
        }
        
        return NLCDLandCoverData(
            parcel_id=parcel_id,
            year=year,
            land_cover_classes=land_cover_classes,
            dominant_class=dominant_class,
            total_area=1.0,
            processing_method="point_sample",
            raw_response=point_data
        )
    
    def _store_land_cover_data(self, nlcd_data: NLCDLandCoverData, processing_method: str, parcel_acres: Optional[float] = None):
        """Store NLCD data in Snowflake database"""
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Clear existing data for this parcel
                sf.execute_non_query(
                    "DELETE FROM TEDDY_DATA.LAND_COVER.PARCEL_LAND_COVER WHERE PARCEL_ID = %s",
                    {'1': nlcd_data.parcel_id}
                )
                
                # Insert detailed land cover data
                for nlcd_code, class_data in nlcd_data.land_cover_classes.items():
                    if class_data["percentage"] > 0.1:  # Only store significant coverage
                        
                        # Calculate area in square feet if parcel acres is available
                        coverage_area_sqft = None
                        if parcel_acres:
                            coverage_area_sqft = parcel_acres * 43560 * (class_data["percentage"] / 100)
                        
                        insert_data = {
                            'PARCEL_ID': nlcd_data.parcel_id,
                            'NLCD_CODE': nlcd_code,
                            'NLCD_YEAR': nlcd_data.year,
                            'COVERAGE_PERCENTAGE': round(class_data["percentage"], 2),
                            'COVERAGE_AREA_SQFT': coverage_area_sqft,
                            'IS_DOMINANT_CLASS': nlcd_code == nlcd_data.dominant_class["code"],
                            'PROCESSING_METHOD': processing_method,
                            'DATA_SOURCE': 'nlcd_api',
                            'CONFIDENCE_SCORE': 1.0,
                            'API_RESPONSE_JSON': json.dumps(nlcd_data.raw_response)
                        }
                        
                        sf.execute_non_query("""
                            INSERT INTO TEDDY_DATA.LAND_COVER.PARCEL_LAND_COVER (
                                PARCEL_ID, NLCD_CODE, NLCD_YEAR, COVERAGE_PERCENTAGE, 
                                COVERAGE_AREA_SQFT, IS_DOMINANT_CLASS, PROCESSING_METHOD, 
                                DATA_SOURCE, CONFIDENCE_SCORE, API_RESPONSE_JSON
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
                        """, {
                            '1': insert_data['PARCEL_ID'],
                            '2': insert_data['NLCD_CODE'],
                            '3': insert_data['NLCD_YEAR'],
                            '4': insert_data['COVERAGE_PERCENTAGE'],
                            '5': insert_data['COVERAGE_AREA_SQFT'],
                            '6': insert_data['IS_DOMINANT_CLASS'],
                            '7': insert_data['PROCESSING_METHOD'],
                            '8': insert_data['DATA_SOURCE'],
                            '9': insert_data['CONFIDENCE_SCORE'],
                            '10': insert_data['API_RESPONSE_JSON']
                        })
                
                # Update summary table using stored procedure
                sf.execute_query(
                    "CALL TEDDY_DATA.LAND_COVER.SP_UPDATE_PARCEL_SUMMARY(%s)",
                    {'1': nlcd_data.parcel_id}
                )
                
                logger.info(f"Successfully stored land cover data for parcel {nlcd_data.parcel_id}")
                
        except Exception as e:
            logger.error(f"Error storing land cover data: {e}")
            raise
    
    def get_parcel_land_cover_json(self, parcel_id: str) -> Optional[Dict]:
        """
        Get formatted land cover data for a parcel
        
        Args:
            parcel_id: Unique parcel identifier
            
        Returns:
            Formatted land cover data or None
        """
        try:
            with get_snowflake_connector(self.environment) as sf:
                results = sf.execute_query(
                    "SELECT TEDDY_DATA.LAND_COVER.FN_GET_PARCEL_LAND_COVER_JSON(%s) as LAND_COVER_DATA",
                    {'1': parcel_id}
                )
                
                if results and results[0]['LAND_COVER_DATA']:
                    return results[0]['LAND_COVER_DATA']
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting parcel land cover JSON: {e}")
            return None

# Create alias for backward compatibility with test scripts
NLCDClient = NLCDSnowflakeIntegration

# Convenience function for Lambda usage
def get_nlcd_integration(environment: str = None) -> NLCDSnowflakeIntegration:
    """
    Get an NLCD Snowflake integration instance
    """
    return NLCDSnowflakeIntegration(environment=environment)

# Example usage for testing
if __name__ == "__main__":
    # Test the integration
    integration = get_nlcd_integration()
    
    # Test with a sample parcel ID
    test_parcel_id = "test_parcel_001"
    
    try:
        result = integration.process_parcel_land_cover(test_parcel_id, year=2021)
        print(f"Processing result: {result}")
        
        if result['success']:
            # Get formatted JSON data
            json_data = integration.get_parcel_land_cover_json(test_parcel_id)
            print(f"Land cover JSON: {json.dumps(json_data, indent=2)}")
            
    except Exception as e:
        print(f"Error: {e}")
