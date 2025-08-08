"""
Topography Client Integration for Teddy Data Pipeline

This module provides USGS elevation/topography data integration for the Teddy pipeline,
supporting both individual parcel processing and bulk operations.

Based on the bulk_topography_client.py script with Lambda-optimized implementation.
"""

import json
import logging
import os
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.utils.snowflake_jwt_connector import get_snowflake_connector

# Configure logging
logger = logging.getLogger(__name__)

class TopographyIntegration:
    """
    Integration class for USGS elevation/topography data processing
    """
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.sf_connector = get_snowflake_connector(environment)
        
        # USGS API endpoints
        self.bulk_point_url = "https://epqs.nationalmap.gov/v1/json"
        self.elevation_point_url = "https://nationalmap.gov/epqs/pqs.php"
        
        # Rate limiting and retry configuration
        self.delay = 0.2
        self.timeout = 30
        self.max_retries = 3
        
        # Circuit breaker pattern
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        self.circuit_breaker_cooldown = 30
        self.circuit_breaker_open_time = None
        
        logger.info(f"Initialized TopographyIntegration for environment: {environment}")
    
    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is currently open"""
        if self.circuit_breaker_open_time is None:
            return False
        
        if time.time() - self.circuit_breaker_open_time > self.circuit_breaker_cooldown:
            logger.info("Circuit breaker cooldown period elapsed, closing circuit")
            self.circuit_breaker_open_time = None
            self.consecutive_failures = 0
            return False
        
        return True
    
    def _record_success(self):
        """Record successful API call"""
        self.consecutive_failures = 0
        if self.circuit_breaker_open_time is not None:
            logger.info("Circuit breaker closed after successful request")
            self.circuit_breaker_open_time = None
    
    def _record_failure(self):
        """Record failed API call and potentially open circuit breaker"""
        self.consecutive_failures += 1
        
        if self.consecutive_failures >= self.max_consecutive_failures and self.circuit_breaker_open_time is None:
            self.circuit_breaker_open_time = time.time()
            logger.error(f"Circuit breaker opened after {self.consecutive_failures} consecutive failures")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with exponential backoff retry logic"""
        if self._is_circuit_breaker_open():
            remaining_time = self.circuit_breaker_cooldown - (time.time() - self.circuit_breaker_open_time)
            raise Exception(f"Circuit breaker is open. Service unavailable for {remaining_time:.0f} more seconds")
        
        for attempt in range(self.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                self._record_success()
                return result
                
            except Exception as e:
                if attempt == self.max_retries:
                    self._record_failure()
                    raise e
                
                # Exponential backoff
                wait_time = self.delay * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time}s")
                time.sleep(wait_time)
        
        self._record_failure()
        raise Exception(f"Max retries ({self.max_retries}) exceeded")
    
    def get_elevation_data(self, latitude: float, longitude: float, 
                          units: str = 'Meters') -> Dict[str, Any]:
        """
        Get elevation data for a single point from USGS API
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            units: Units for elevation (Meters or Feet)
            
        Returns:
            Elevation data dictionary
        """
        
        def _make_request():
            params = {
                'x': longitude,
                'y': latitude,
                'units': units,
                'output': 'json'
            }
            
            logger.debug(f"Requesting elevation data for {latitude}, {longitude}")
            response = requests.get(self.bulk_point_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            return response.json()
        
        try:
            data = self._retry_with_backoff(_make_request)
            
            # Extract elevation information
            elevation_query = data.get('USGS_Elevation_Point_Query_Service', {})
            elevation_query_list = elevation_query.get('Elevation_Query', [])
            
            if elevation_query_list:
                elevation_info = elevation_query_list[0]
                elevation_value = elevation_info.get('Elevation')
                
                # Convert elevation to float if it's a string
                if isinstance(elevation_value, str):
                    try:
                        elevation_value = float(elevation_value)
                    except (ValueError, TypeError):
                        elevation_value = None
                
                return {
                    'success': True,
                    'latitude': latitude,
                    'longitude': longitude,
                    'elevation': elevation_value,
                    'units': elevation_info.get('Units', units),
                    'data_source': elevation_info.get('Data_Source', 'USGS 3DEP'),
                    'resolution': elevation_info.get('Resolution', 'Unknown'),
                    'query_time': datetime.now().isoformat()
                }
            else:
                return {
                    'success': False,
                    'latitude': latitude,
                    'longitude': longitude,
                    'error': 'No elevation data returned from USGS API'
                }
                
        except Exception as e:
            logger.error(f"Error getting elevation data for {latitude}, {longitude}: {str(e)}")
            return {
                'success': False,
                'latitude': latitude,
                'longitude': longitude,
                'error': str(e)
            }
    
    def get_parcel_coordinates(self, parcel_id: str) -> Optional[Tuple[float, float]]:
        """
        Get coordinates for a parcel from Snowflake
        
        Args:
            parcel_id: Parcel identifier
            
        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        
        try:
            with self.sf_connector as sf:
                query = """
                    SELECT LATITUDE, LONGITUDE 
                    FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW 
                    WHERE PARCEL_ID = %s 
                    AND LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL 
                    LIMIT 1
                """
                
                results = sf.execute_query(query, {'1': parcel_id})
                
                if results and len(results) > 0:
                    row = results[0]
                    lat = row.get('LATITUDE')
                    lon = row.get('LONGITUDE')
                    
                    if lat is not None and lon is not None:
                        return (float(lat), float(lon))
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting coordinates for parcel {parcel_id}: {str(e)}")
            return None
    
    def check_existing_topography(self, parcel_id: str, max_age_days: int = 30) -> Optional[Dict[str, Any]]:
        """
        Check if topography data already exists for a parcel
        
        Args:
            parcel_id: Parcel identifier
            max_age_days: Maximum age of existing data in days
            
        Returns:
            Existing topography data or None if not found/too old
        """
        
        try:
            with self.sf_connector as sf:
                query = """
                    SELECT 
                        PARCEL_ID,
                        MEAN_ELEVATION_FT,
                        MIN_ELEVATION_FT,
                        MAX_ELEVATION_FT,
                        ELEVATION_VARIANCE_FT,
                        SLOPE_PERCENT,
                        TERRAIN_ANALYSIS,
                        DATA_SOURCE,
                        RESOLUTION,
                        COLLECTION_METHOD,
                        COLLECTION_DATE,
                        LATITUDE,
                        LONGITUDE,
                        COUNTY_ID,
                        STATE_CODE,
                        ELEVATION_UNITS,
                        CREATED_AT,
                        UPDATED_AT
                    FROM TEDDY_DATA.RAW.TOPOGRAPHY 
                    WHERE PARCEL_ID = %s 
                    AND UPDATED_AT >= %s
                    ORDER BY UPDATED_AT DESC 
                    LIMIT 1
                """
                
                cutoff_date = datetime.now() - timedelta(days=max_age_days)
                results = sf.execute_query(query, {'1': parcel_id, '2': cutoff_date})
                
                if results and len(results) > 0:
                    row = results[0]
                    return {
                        'parcel_id': row.get('PARCEL_ID'),
                        'mean_elevation_ft': row.get('MEAN_ELEVATION_FT'),
                        'min_elevation_ft': row.get('MIN_ELEVATION_FT'),
                        'max_elevation_ft': row.get('MAX_ELEVATION_FT'),
                        'elevation_variance_ft': row.get('ELEVATION_VARIANCE_FT'),
                        'slope_percent': row.get('SLOPE_PERCENT'),
                        'terrain_analysis': row.get('TERRAIN_ANALYSIS'),
                        'data_source': row.get('DATA_SOURCE'),
                        'resolution': row.get('RESOLUTION'),
                        'collection_method': row.get('COLLECTION_METHOD'),
                        'collection_date': row.get('COLLECTION_DATE'),
                        'latitude': row.get('LATITUDE'),
                        'longitude': row.get('LONGITUDE'),
                        'county_id': row.get('COUNTY_ID'),
                        'state_code': row.get('STATE_CODE'),
                        'elevation_units': row.get('ELEVATION_UNITS'),
                        'created_at': row.get('CREATED_AT'),
                        'updated_at': row.get('UPDATED_AT')
                    }
                
                return None
                
        except Exception as e:
            logger.error(f"Error checking existing topography for parcel {parcel_id}: {str(e)}")
            return None
    
    def store_topography_data(self, parcel_id: str, elevation_data: Dict[str, Any]) -> bool:
        """
        Store topography data in Snowflake with enhanced schema
        
        Args:
            parcel_id: Parcel identifier
            elevation_data: Elevation data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        
        try:
            with self.sf_connector as sf:
                # First, delete any existing data for this parcel
                delete_query = "DELETE FROM TEDDY_DATA.RAW.TOPOGRAPHY WHERE PARCEL_ID = %s"
                sf.execute_query(delete_query, {'1': parcel_id})
                
                # Convert elevation from meters to feet if needed
                elevation_meters = elevation_data.get('elevation')
                elevation_feet = None
                if elevation_meters is not None:
                    if elevation_data.get('units', 'Meters').lower() in ['meters', 'metre', 'm']:
                        elevation_feet = elevation_meters * 3.28084  # Convert meters to feet
                    else:
                        elevation_feet = elevation_meters
                
                # Insert new data with enhanced schema
                insert_query = """
                    INSERT INTO TEDDY_DATA.RAW.TOPOGRAPHY (
                        PARCEL_ID,
                        MEAN_ELEVATION_FT,
                        MIN_ELEVATION_FT,
                        MAX_ELEVATION_FT,
                        ELEVATION_VARIANCE_FT,
                        SLOPE_PERCENT,
                        TERRAIN_ANALYSIS,
                        DATA_SOURCE,
                        RESOLUTION,
                        COLLECTION_METHOD,
                        COLLECTION_DATE,
                        LATITUDE,
                        LONGITUDE,
                        COUNTY_ID,
                        STATE_CODE,
                        ELEVATION_UNITS,
                        PROCESSING_METHOD,
                        CONFIDENCE_SCORE,
                        API_RESPONSE_JSON,
                        CREATED_AT,
                        UPDATED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                current_time = datetime.now()
                current_date = current_time.date()
                
                # Basic terrain analysis
                terrain_desc = f"Single point elevation measurement at {elevation_feet:.2f} ft" if elevation_feet else "No elevation data"
                
                params = {
                    '1': parcel_id,
                    '2': elevation_feet,  # MEAN_ELEVATION_FT
                    '3': elevation_feet,  # MIN_ELEVATION_FT (same as mean for single point)
                    '4': elevation_feet,  # MAX_ELEVATION_FT (same as mean for single point)
                    '5': 0.0,  # ELEVATION_VARIANCE_FT (0 for single point)
                    '6': None,  # SLOPE_PERCENT (unknown for single point)
                    '7': terrain_desc,  # TERRAIN_ANALYSIS
                    '8': elevation_data.get('data_source', 'USGS 3DEP'),  # DATA_SOURCE
                    '9': elevation_data.get('resolution', 'Unknown'),  # RESOLUTION
                    '10': 'USGS_API_SINGLE_POINT',  # COLLECTION_METHOD
                    '11': current_date,  # COLLECTION_DATE
                    '12': elevation_data.get('latitude'),  # LATITUDE
                    '13': elevation_data.get('longitude'),  # LONGITUDE
                    '14': None,  # COUNTY_ID (to be populated later)
                    '15': None,  # STATE_CODE (to be populated later)
                    '16': 'Feet',  # ELEVATION_UNITS
                    '17': 'USGS_API',  # PROCESSING_METHOD
                    '18': 1.0,  # CONFIDENCE_SCORE
                    '19': json.dumps(elevation_data),  # API_RESPONSE_JSON
                    '20': current_time,  # CREATED_AT
                    '21': current_time   # UPDATED_AT
                }
                
                sf.execute_query(insert_query, params)
                logger.info(f"Stored topography data for parcel {parcel_id} (elevation: {elevation_feet:.2f} ft)")
                return True
                
        except Exception as e:
            logger.error(f"Error storing topography data for parcel {parcel_id}: {str(e)}")
            return False
    
    def process_parcel_topography(self, parcel_id: str, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Process topography data for a single parcel
        
        Args:
            parcel_id: Parcel identifier
            force_refresh: Force refresh even if recent data exists
            
        Returns:
            Processing result dictionary
        """
        
        logger.info(f"Processing topography for parcel: {parcel_id}, force_refresh: {force_refresh}")
        
        try:
            # Check if we have recent data unless force_refresh is True
            if not force_refresh:
                existing_data = self.check_existing_topography(parcel_id)
                if existing_data:
                    logger.info(f"Found existing topography data for parcel {parcel_id}")
                    return {
                        'success': True,
                        'parcel_id': parcel_id,
                        'source': 'cache',
                        'processing_method': 'existing_data',
                        'data': existing_data
                    }
            
            # Get parcel coordinates
            coordinates = self.get_parcel_coordinates(parcel_id)
            if not coordinates:
                return {
                    'success': False,
                    'parcel_id': parcel_id,
                    'error': 'Could not find coordinates for parcel'
                }
            
            latitude, longitude = coordinates
            
            # Get elevation data from USGS API
            elevation_data = self.get_elevation_data(latitude, longitude)
            
            if not elevation_data.get('success'):
                return {
                    'success': False,
                    'parcel_id': parcel_id,
                    'error': elevation_data.get('error', 'Failed to get elevation data')
                }
            
            # Store the data
            if self.store_topography_data(parcel_id, elevation_data):
                return {
                    'success': True,
                    'parcel_id': parcel_id,
                    'source': 'api',
                    'processing_method': 'USGS_API',
                    'data': elevation_data
                }
            else:
                return {
                    'success': False,
                    'parcel_id': parcel_id,
                    'error': 'Failed to store topography data'
                }
                
        except Exception as e:
            logger.error(f"Error processing topography for parcel {parcel_id}: {str(e)}")
            return {
                'success': False,
                'parcel_id': parcel_id,
                'error': str(e)
            }
    
    def get_parcel_topography_json(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed topography data for a parcel in JSON format
        
        Args:
            parcel_id: Parcel identifier
            
        Returns:
            Detailed topography data or None if not found
        """
        
        try:
            with self.sf_connector as sf:
                # Use the Snowflake function to get topography JSON
                query = "SELECT TEDDY_DATA.RAW.FN_GET_TOPOGRAPHY_JSON(%s) as topography_json"
                results = sf.execute_query(query, {'1': parcel_id})
                
                if results and len(results) > 0 and results[0].get('TOPOGRAPHY_JSON'):
                    return json.loads(results[0]['TOPOGRAPHY_JSON'])
                
                return None
                
        except Exception as e:
            logger.error(f"Error getting topography JSON for parcel {parcel_id}: {str(e)}")
            return None

# Convenience function for Lambda integration
def get_topography_integration(environment: str = None) -> TopographyIntegration:
    """
    Factory function to create TopographyIntegration instance
    
    Args:
        environment: Environment (dev/prod), defaults to ENVIRONMENT env var
        
    Returns:
        TopographyIntegration instance
    """
    
    if environment is None:
        environment = os.environ.get('ENVIRONMENT', 'dev')
    
    return TopographyIntegration(environment)

# Example usage for testing
if __name__ == "__main__":
    # Test the topography integration
    topo_client = get_topography_integration('dev')
    
    # Test single point elevation
    test_lat, test_lon = 35.0135, -80.7434  # Charlotte, NC area
    elevation_result = topo_client.get_elevation_data(test_lat, test_lon)
    print(f"Elevation result: {json.dumps(elevation_result, indent=2)}")
    
    # Test parcel processing
    test_parcel_id = "test_parcel_001"
    processing_result = topo_client.process_parcel_topography(test_parcel_id)
    print(f"Processing result: {json.dumps(processing_result, indent=2)}")