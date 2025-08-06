#!/usr/bin/env python3
"""
Snowflake CDL Crop History Loader

This script loads crop history data for all parcels in the PARCEL_PROFILE table
using the USDA Cropland Data Layer (CDL) API and stores results in Snowflake.

Features:
- Integrates with existing Snowflake PARCEL_PROFILE table
- Efficient batch processing with rate limiting
- Comprehensive error handling and logging
- Resume capability for interrupted runs
- Progress tracking and reporting
"""

import os
import sys
import json
import time
import logging
import requests
import snowflake.connector
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cdl_crop_history_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SnowflakeCDLCropHistoryLoader:
    """Load crop history data from USDA CDL API into Snowflake"""
    
    def __init__(self):
        self.snowflake_config = self._get_snowflake_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BirdDog-CDL-Crop-History-Loader/1.0',
            'Accept': 'application/xml,application/json'
        })
        
        # CDL API configuration
        self.cdl_api_base = "https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLStat"
        self.rate_limit_delay = 0.5  # Seconds between API calls
        self.max_retries = 3
        
        # Thread safety
        self._lock = threading.Lock()
        self._api_call_count = 0
        self._last_api_call = 0
        
        # Comprehensive crop code mapping
        self.crop_codes = {
            1: "Corn", 2: "Cotton", 3: "Rice", 4: "Sorghum", 5: "Soybeans",
            6: "Sunflower", 10: "Peanuts", 11: "Tobacco", 12: "Sweet Corn",
            13: "Pop or Orn Corn", 14: "Mint", 21: "Barley", 22: "Durum Wheat",
            23: "Spring Wheat", 24: "Winter Wheat", 25: "Other Small Grains",
            26: "Dbl Crop WinWht/Soybeans", 27: "Rye", 28: "Oats", 29: "Millet",
            30: "Speltz", 31: "Canola", 32: "Flaxseed", 33: "Safflower",
            34: "Rape Seed", 35: "Mustard", 36: "Alfalfa", 37: "Other Hay/Non Alfalfa",
            38: "Camelina", 39: "Buckwheat", 41: "Sugarbeets", 42: "Dry Beans",
            43: "Potatoes", 44: "Other Crops", 45: "Sugarcane", 46: "Sweet Potatoes",
            47: "Misc Vegs & Fruits", 48: "Watermelons", 49: "Onions", 50: "Cucumbers",
            51: "Chick Peas", 52: "Lentils", 53: "Peas", 54: "Tomatoes",
            55: "Caneberries", 56: "Hops", 57: "Herbs", 58: "Clover/Wildflowers",
            59: "Sod/Grass Seed", 60: "Switchgrass", 61: "Fallow/Idle Cropland",
            63: "Forest", 64: "Shrubland", 65: "Barren", 81: "Clouds/No Data",
            82: "Developed", 83: "Water", 87: "Wetlands", 88: "Nonag/Undefined",
            92: "Aquaculture", 111: "Open Water", 112: "Perennial Ice/Snow",
            121: "Developed/Open Space", 122: "Developed/Low Intensity",
            123: "Developed/Med Intensity", 124: "Developed/High Intensity",
            131: "Barren", 141: "Deciduous Forest", 142: "Evergreen Forest",
            143: "Mixed Forest", 152: "Shrubland", 176: "Grassland/Pasture",
            190: "Woody Wetlands", 195: "Herbaceous Wetlands"
        }
    
    def _get_snowflake_config(self):
        """Get Snowflake configuration from environment variables"""
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'BIRDDOG_INGESTION_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'BIRDDOG_DATA'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'CURATED')
        }
    
    def get_snowflake_connection(self):
        """Get Snowflake connection with authentication"""
        try:
            # Try private key authentication first
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if private_key_path and os.path.exists(private_key_path):
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                from cryptography.hazmat.backends import default_backend
                
                with open(private_key_path, 'rb') as key_file:
                    private_key = load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend()
                    )
                
                private_key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                config = self.snowflake_config.copy()
                config['private_key'] = private_key_bytes
                
                return snowflake.connector.connect(**config)
            else:
                # Fall back to password authentication
                password = os.getenv('SNOWFLAKE_PASSWORD')
                if password:
                    config = self.snowflake_config.copy()
                    config['password'] = password
                    return snowflake.connector.connect(**config)
                else:
                    raise ValueError("No Snowflake authentication method available")
                    
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def setup_crop_history_table(self):
        """Create crop history table in Snowflake"""
        logger.info("Setting up crop history table in Snowflake...")
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS PARCEL_CROP_HISTORY (
                PARCEL_ID VARCHAR(255) NOT NULL,
                CROP_YEAR INTEGER NOT NULL,
                CDL_CROP_CODE INTEGER,
                CROP_NAME VARCHAR(255),
                CROP_CATEGORY VARCHAR(100),
                LATITUDE FLOAT,
                LONGITUDE FLOAT,
                COUNTY_ID VARCHAR(100),
                STATE_CODE VARCHAR(10),
                ACRES FLOAT,
                API_RESPONSE_RAW VARIANT,
                QUERY_SUCCESS BOOLEAN DEFAULT FALSE,
                QUERY_METHOD VARCHAR(50), -- 'point' or 'polygon'
                DATA_SOURCE VARCHAR(100) DEFAULT 'USDA_CDL_API',
                COLLECTION_DATE DATE DEFAULT CURRENT_DATE,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (PARCEL_ID, CROP_YEAR)
            )
            """
            
            cursor.execute(create_table_sql)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS IDX_CROP_HISTORY_PARCEL ON PARCEL_CROP_HISTORY(PARCEL_ID)",
                "CREATE INDEX IF NOT EXISTS IDX_CROP_HISTORY_YEAR ON PARCEL_CROP_HISTORY(CROP_YEAR)",
                "CREATE INDEX IF NOT EXISTS IDX_CROP_HISTORY_CROP_CODE ON PARCEL_CROP_HISTORY(CDL_CROP_CODE)",
                "CREATE INDEX IF NOT EXISTS IDX_CROP_HISTORY_STATE ON PARCEL_CROP_HISTORY(STATE_CODE)",
                "CREATE INDEX IF NOT EXISTS IDX_CROP_HISTORY_SUCCESS ON PARCEL_CROP_HISTORY(QUERY_SUCCESS)"
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Index creation warning: {e}")
            
            conn.commit()
            logger.info("✅ Crop history table setup complete")
            
        except Exception as e:
            logger.error(f"Error setting up crop history table: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_parcels_for_processing(self, limit: Optional[int] = None, state_filter: Optional[str] = None) -> List[Dict]:
        """Get parcels from PARCEL_PROFILE that need crop history data"""
        logger.info("Fetching parcels for crop history processing...")
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Base query to get parcels without crop history
            query = """
            SELECT 
                p.PARCEL_ID,
                p.LATITUDE,
                p.LONGITUDE,
                p.COUNTY_ID,
                p.STATE_CODE,
                p.ACRES,
                p.PARCEL_WKT
            FROM PARCEL_PROFILE p
            LEFT JOIN PARCEL_CROP_HISTORY ch ON p.PARCEL_ID = ch.PARCEL_ID
            WHERE p.LATITUDE IS NOT NULL 
              AND p.LONGITUDE IS NOT NULL
              AND ch.PARCEL_ID IS NULL  -- Only parcels without crop history
            """
            
            # Add state filter if specified
            if state_filter:
                query += f" AND p.STATE_CODE = '{state_filter}'"
            else:
                # Focus on major agricultural states
                query += " AND p.STATE_CODE IN ('TX', 'IA', 'IL', 'IN', 'KS', 'NE', 'MN', 'MO', 'OH', 'WI', 'OK', 'AR', 'MS', 'AL', 'GA', 'FL', 'SC', 'NC', 'VA', 'MD', 'DE', 'NJ', 'PA', 'NY', 'CT', 'RI', 'MA', 'VT', 'NH', 'ME')"
            
            # Order by acres descending (larger parcels first)
            query += " ORDER BY p.ACRES DESC"
            
            # Add limit if specified
            if limit:
                query += f" LIMIT {limit}"
            
            cursor.execute(query)
            
            parcels = []
            for row in cursor.fetchall():
                parcels.append({
                    'parcel_id': row[0],
                    'latitude': float(row[1]) if row[1] else None,
                    'longitude': float(row[2]) if row[2] else None,
                    'county_id': row[3],
                    'state_code': row[4],
                    'acres': float(row[5]) if row[5] else 0,
                    'parcel_wkt': row[6]
                })
            
            logger.info(f"Found {len(parcels)} parcels to process")
            return parcels
            
        except Exception as e:
            logger.error(f"Error fetching parcels: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _rate_limited_api_call(self, url: str, params: Dict) -> requests.Response:
        """Make rate-limited API call to CDL service"""
        with self._lock:
            # Ensure we don't exceed rate limits
            current_time = time.time()
            time_since_last_call = current_time - self._last_api_call
            
            if time_since_last_call < self.rate_limit_delay:
                sleep_time = self.rate_limit_delay - time_since_last_call
                time.sleep(sleep_time)
            
            self._api_call_count += 1
            self._last_api_call = time.time()
            
            # Log every 100 API calls
            if self._api_call_count % 100 == 0:
                logger.info(f"Made {self._api_call_count} API calls")
        
        return self.session.get(url, params=params, timeout=30)
    
    def query_cdl_for_parcel(self, parcel: Dict, year: int) -> Optional[Dict]:
        """Query CDL API for specific parcel and year"""
        try:
            params = {
                'year': year,
                'x': parcel['longitude'],
                'y': parcel['latitude'],
                'format': 'xml'  # CDL API works better with XML format
            }
            
            response = self._rate_limited_api_call(self.cdl_api_base, params)
            response.raise_for_status()
            
            # Parse XML response
            crop_data = self._parse_cdl_xml_response(response.text)
            
            if crop_data:
                return {
                    'parcel_id': parcel['parcel_id'],
                    'year': year,
                    'crop_code': crop_data['crop_code'],
                    'crop_name': crop_data['crop_name'],
                    'crop_category': crop_data['crop_category'],
                    'raw_response': response.text,
                    'query_success': True,
                    'query_method': 'point'
                }
            else:
                return {
                    'parcel_id': parcel['parcel_id'],
                    'year': year,
                    'crop_code': None,
                    'crop_name': None,
                    'crop_category': None,
                    'raw_response': response.text,
                    'query_success': False,
                    'query_method': 'point'
                }
                
        except Exception as e:
            logger.error(f"Error querying CDL for parcel {parcel['parcel_id']}, year {year}: {e}")
            return {
                'parcel_id': parcel['parcel_id'],
                'year': year,
                'crop_code': None,
                'crop_name': None,
                'crop_category': None,
                'raw_response': str(e),
                'query_success': False,
                'query_method': 'point'
            }
    
    def _parse_cdl_xml_response(self, xml_text: str) -> Optional[Dict]:
        """Parse CDL XML response to extract crop information"""
        try:
            # Try to parse as XML first
            root = ET.fromstring(xml_text)
            
            # Look for Result element
            result_elem = root.find('.//Result')
            if result_elem is not None and result_elem.text:
                crop_code = int(result_elem.text)
                crop_name = self.crop_codes.get(crop_code, f"Unknown ({crop_code})")
                crop_category = self._get_crop_category(crop_code)
                
                return {
                    'crop_code': crop_code,
                    'crop_name': crop_name,
                    'crop_category': crop_category
                }
            
            # If no Result element, try other common patterns
            for elem in root.iter():
                if elem.text and elem.text.isdigit():
                    crop_code = int(elem.text)
                    if crop_code in self.crop_codes:
                        crop_name = self.crop_codes[crop_code]
                        crop_category = self._get_crop_category(crop_code)
                        
                        return {
                            'crop_code': crop_code,
                            'crop_name': crop_name,
                            'crop_category': crop_category
                        }
            
            return None
            
        except ET.ParseError:
            # If XML parsing fails, try regex patterns
            import re
            
            patterns = [
                r'<Result>(\d+)</Result>',
                r'<category>(\d+)</category>',
                r'<value>(\d+)</value>',
                r'>(\d+)<'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, xml_text)
                if match:
                    crop_code = int(match.group(1))
                    if crop_code in self.crop_codes:
                        crop_name = self.crop_codes[crop_code]
                        crop_category = self._get_crop_category(crop_code)
                        
                        return {
                            'crop_code': crop_code,
                            'crop_name': crop_name,
                            'crop_category': crop_category
                        }
            
            return None
            
        except Exception as e:
            logger.error(f"Error parsing CDL XML response: {e}")
            return None
    
    def _get_crop_category(self, crop_code: int) -> str:
        """Categorize crop codes into major categories"""
        # Major crop categories
        if crop_code in [1, 12, 13]:  # Corn varieties
            return "Corn"
        elif crop_code in [5, 26]:  # Soybeans
            return "Soybeans"
        elif crop_code in [21, 22, 23, 24, 25]:  # Wheat varieties
            return "Wheat"
        elif crop_code == 2:  # Cotton
            return "Cotton"
        elif crop_code in [3]:  # Rice
            return "Rice"
        elif crop_code in [4]:  # Sorghum
            return "Sorghum"
        elif crop_code in [36, 37]:  # Hay/Alfalfa
            return "Hay"
        elif crop_code in [41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54]:  # Specialty crops
            return "Specialty Crops"
        elif crop_code == 61:  # Fallow
            return "Fallow"
        elif crop_code == 176:  # Grassland/Pasture
            return "Pasture"
        elif crop_code in [63, 141, 142, 143]:  # Forest
            return "Forest"
        elif crop_code in [82, 121, 122, 123, 124]:  # Developed
            return "Developed"
        elif crop_code in [83, 111]:  # Water
            return "Water"
        elif crop_code in [87, 190, 195]:  # Wetlands
            return "Wetlands"
        else:
            return "Other"
    
    def save_crop_history_batch(self, crop_history_records: List[Dict]):
        """Save batch of crop history records to Snowflake"""
        if not crop_history_records:
            return
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_sql = """
            MERGE INTO PARCEL_CROP_HISTORY AS target
            USING (SELECT 
                %s as PARCEL_ID, %s as CROP_YEAR, %s as CDL_CROP_CODE, %s as CROP_NAME,
                %s as CROP_CATEGORY, %s as LATITUDE, %s as LONGITUDE, %s as COUNTY_ID,
                %s as STATE_CODE, %s as ACRES, PARSE_JSON(%s) as API_RESPONSE_RAW,
                %s as QUERY_SUCCESS, %s as QUERY_METHOD
            ) AS source
            ON target.PARCEL_ID = source.PARCEL_ID AND target.CROP_YEAR = source.CROP_YEAR
            WHEN MATCHED THEN UPDATE SET
                CDL_CROP_CODE = source.CDL_CROP_CODE,
                CROP_NAME = source.CROP_NAME,
                CROP_CATEGORY = source.CROP_CATEGORY,
                QUERY_SUCCESS = source.QUERY_SUCCESS,
                UPDATED_AT = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                PARCEL_ID, CROP_YEAR, CDL_CROP_CODE, CROP_NAME, CROP_CATEGORY,
                LATITUDE, LONGITUDE, COUNTY_ID, STATE_CODE, ACRES,
                API_RESPONSE_RAW, QUERY_SUCCESS, QUERY_METHOD
            ) VALUES (
                source.PARCEL_ID, source.CROP_YEAR, source.CDL_CROP_CODE, source.CROP_NAME,
                source.CROP_CATEGORY, source.LATITUDE, source.LONGITUDE, source.COUNTY_ID,
                source.STATE_CODE, source.ACRES, source.API_RESPONSE_RAW,
                source.QUERY_SUCCESS, source.QUERY_METHOD
            )
            """
            
            for record in crop_history_records:
                cursor.execute(insert_sql, (
                    record['parcel_id'],
                    record['year'],
                    record['crop_code'],
                    record['crop_name'],
                    record['crop_category'],
                    record['latitude'],
                    record['longitude'],
                    record['county_id'],
                    record['state_code'],
                    record['acres'],
                    json.dumps({'error': str(record['raw_response'])}) if not isinstance(record['raw_response'], dict) else json.dumps(record['raw_response']),
                    record['query_success'],
                    record['query_method']
                ))
            
            conn.commit()
            logger.info(f"Saved {len(crop_history_records)} crop history records")
            
        except Exception as e:
            logger.error(f"Error saving crop history batch: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def process_parcel_crop_history(self, parcel: Dict, years: List[int]) -> List[Dict]:
        """Process crop history for a single parcel across multiple years"""
        crop_history_records = []
        
        for year in years:
            crop_data = self.query_cdl_for_parcel(parcel, year)
            if crop_data:
                # Add parcel information to the crop data
                crop_data.update({
                    'latitude': parcel['latitude'],
                    'longitude': parcel['longitude'],
                    'county_id': parcel['county_id'],
                    'state_code': parcel['state_code'],
                    'acres': parcel['acres']
                })
                crop_history_records.append(crop_data)
        
        return crop_history_records
    
    def run_crop_history_ingestion(self, 
                                 max_parcels: Optional[int] = None,
                                 years: Optional[List[int]] = None,
                                 state_filter: Optional[str] = None,
                                 batch_size: int = 100):
        """Run the complete crop history ingestion process"""
        
        if years is None:
            # Default to last 5 years
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        logger.info(f"Starting CDL crop history ingestion")
        logger.info(f"Years: {years}")
        logger.info(f"Max parcels: {max_parcels or 'unlimited'}")
        logger.info(f"State filter: {state_filter or 'all agricultural states'}")
        logger.info(f"Batch size: {batch_size}")
        
        # Setup database table
        self.setup_crop_history_table()
        
        # Get parcels to process
        parcels = self.get_parcels_for_processing(limit=max_parcels, state_filter=state_filter)
        
        if not parcels:
            logger.info("No parcels found for processing")
            return
        
        logger.info(f"Processing {len(parcels)} parcels for {len(years)} years each")
        
        # Process parcels in batches
        processed = 0
        successful = 0
        failed = 0
        batch_records = []
        
        start_time = datetime.now()
        
        for i, parcel in enumerate(parcels):
            try:
                logger.info(f"Processing parcel {i+1}/{len(parcels)}: {parcel['parcel_id']} ({parcel['acres']:.1f} acres)")
                
                # Process crop history for this parcel
                crop_records = self.process_parcel_crop_history(parcel, years)
                batch_records.extend(crop_records)
                
                # Count successful queries
                successful += sum(1 for record in crop_records if record['query_success'])
                failed += sum(1 for record in crop_records if not record['query_success'])
                
                processed += 1
                
                # Save batch when it reaches batch_size
                if len(batch_records) >= batch_size:
                    self.save_crop_history_batch(batch_records)
                    batch_records = []
                
                # Progress reporting
                if processed % 10 == 0:
                    elapsed = datetime.now() - start_time
                    rate = processed / elapsed.total_seconds() * 60  # parcels per minute
                    remaining = len(parcels) - processed
                    eta = timedelta(minutes=remaining / rate) if rate > 0 else timedelta(0)
                    
                    logger.info(f"Progress: {processed}/{len(parcels)} parcels ({processed/len(parcels)*100:.1f}%)")
                    logger.info(f"Rate: {rate:.1f} parcels/min, ETA: {eta}")
                    logger.info(f"Success rate: {successful}/{successful+failed} queries ({successful/(successful+failed)*100:.1f}%)")
                
            except Exception as e:
                logger.error(f"Error processing parcel {parcel['parcel_id']}: {e}")
                failed += len(years)  # Count all years as failed for this parcel
                continue
        
        # Save any remaining records
        if batch_records:
            self.save_crop_history_batch(batch_records)
        
        # Final summary
        elapsed = datetime.now() - start_time
        logger.info(f"Crop history ingestion completed!")
        logger.info(f"Processed: {processed} parcels")
        logger.info(f"Successful queries: {successful}")
        logger.info(f"Failed queries: {failed}")
        logger.info(f"Success rate: {successful/(successful+failed)*100:.1f}%")
        logger.info(f"Total time: {elapsed}")
        logger.info(f"Average rate: {processed/elapsed.total_seconds()*60:.1f} parcels/min")
        logger.info(f"Total API calls: {self._api_call_count}")
    
    def get_progress_report(self) -> Dict:
        """Get progress report of crop history ingestion"""
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT PARCEL_ID) as total_parcels_with_data,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN QUERY_SUCCESS THEN 1 END) as successful_queries,
                    COUNT(CASE WHEN NOT QUERY_SUCCESS THEN 1 END) as failed_queries,
                    MIN(CROP_YEAR) as earliest_year,
                    MAX(CROP_YEAR) as latest_year
                FROM PARCEL_CROP_HISTORY
            """)
            
            overall_stats = cursor.fetchone()
            
            # By year statistics
            cursor.execute("""
                SELECT 
                    CROP_YEAR,
                    COUNT(DISTINCT PARCEL_ID) as parcels,
                    COUNT(*) as total_queries,
                    COUNT(CASE WHEN QUERY_SUCCESS THEN 1 END) as successful,
                    COUNT(CASE WHEN NOT QUERY_SUCCESS THEN 1 END) as failed
                FROM PARCEL_CROP_HISTORY
                GROUP BY CROP_YEAR
                ORDER BY CROP_YEAR DESC
            """)
            
            year_stats = cursor.fetchall()
            
            # By state statistics
            cursor.execute("""
                SELECT 
                    STATE_CODE,
                    COUNT(DISTINCT PARCEL_ID) as parcels,
                    COUNT(*) as total_queries,
                    COUNT(CASE WHEN QUERY_SUCCESS THEN 1 END) as successful
                FROM PARCEL_CROP_HISTORY
                GROUP BY STATE_CODE
                ORDER BY parcels DESC
            """)
            
            state_stats = cursor.fetchall()
            
            # Top crops
            cursor.execute("""
                SELECT 
                    CROP_CATEGORY,
                    COUNT(*) as occurrences,
                    COUNT(DISTINCT PARCEL_ID) as parcels
                FROM PARCEL_CROP_HISTORY
                WHERE QUERY_SUCCESS = TRUE
                GROUP BY CROP_CATEGORY
                ORDER BY occurrences DESC
                LIMIT 10
            """)
            
            crop_stats = cursor.fetchall()
            
            report = {
                'overall': {
                    'total_parcels_with_data': overall_stats[0],
                    'total_records': overall_stats[1],
                    'successful_queries': overall_stats[2],
                    'failed_queries': overall_stats[3],
                    'success_rate': overall_stats[2] / (overall_stats[2] + overall_stats[3]) * 100 if (overall_stats[2] + overall_stats[3]) > 0 else 0,
                    'earliest_year': overall_stats[4],
                    'latest_year': overall_stats[5]
                },
                'by_year': [
                    {
                        'year': row[0],
                        'parcels': row[1],
                        'total_queries': row[2],
                        'successful': row[3],
                        'failed': row[4],
                        'success_rate': row[3] / row[2] * 100 if row[2] > 0 else 0
                    }
                    for row in year_stats
                ],
                'by_state': [
                    {
                        'state': row[0],
                        'parcels': row[1],
                        'total_queries': row[2],
                        'successful': row[3],
                        'success_rate': row[3] / row[2] * 100 if row[2] > 0 else 0
                    }
                    for row in state_stats
                ],
                'top_crops': [
                    {
                        'crop_category': row[0],
                        'occurrences': row[1],
                        'parcels': row[2]
                    }
                    for row in crop_stats
                ],
                'generated_at': datetime.now().isoformat()
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating progress report: {e}")
            raise
        finally:
            if conn:
                conn.close()


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Snowflake CDL Crop History Loader')
    parser.add_argument('--max-parcels', type=int, help='Maximum number of parcels to process')
    parser.add_argument('--years', nargs='+', type=int, help='Years to process (e.g., 2020 2021 2022)')
    parser.add_argument('--state', type=str, help='State code to filter (e.g., TX)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for database operations')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited parcels')
    parser.add_argument('--report', action='store_true', help='Generate progress report only')
    
    args = parser.parse_args()
    
    try:
        loader = SnowflakeCDLCropHistoryLoader()
        
        if args.report:
            logger.info("Generating progress report...")
            report = loader.get_progress_report()
            
            print("\n" + "="*60)
            print("CDL CROP HISTORY PROGRESS REPORT")
            print("="*60)
            print(f"Total Parcels with Data: {report['overall']['total_parcels_with_data']:,}")
            print(f"Total Records: {report['overall']['total_records']:,}")
            print(f"Success Rate: {report['overall']['success_rate']:.1f}%")
            print(f"Years Covered: {report['overall']['earliest_year']} - {report['overall']['latest_year']}")
            
            print(f"\nTop Crop Categories:")
            for crop in report['top_crops'][:5]:
                print(f"  {crop['crop_category']}: {crop['occurrences']:,} records ({crop['parcels']:,} parcels)")
            
            print(f"\nBy State (Top 10):")
            for state in report['by_state'][:10]:
                print(f"  {state['state']}: {state['parcels']:,} parcels ({state['success_rate']:.1f}% success)")
            
            # Save detailed report
            with open('cdl_crop_history_report.json', 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info("Detailed report saved to cdl_crop_history_report.json")
            
        else:
            # Set defaults for test mode
            if args.test:
                max_parcels = 10
                years = [2022, 2023]
                logger.info("Running in test mode with 10 parcels and years 2022-2023")
            else:
                max_parcels = args.max_parcels
                years = args.years
            
            # Run the ingestion
            loader.run_crop_history_ingestion(
                max_parcels=max_parcels,
                years=years,
                state_filter=args.state,
                batch_size=args.batch_size
            )
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
