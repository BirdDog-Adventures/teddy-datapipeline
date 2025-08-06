#!/usr/bin/env python3
"""
NASS QuickStats County Agricultural Data Loader

This script loads county-level agricultural statistics from the USDA NASS QuickStats API
for all counties represented in your PARCEL_PROFILE table and stores the data in Snowflake.

Features:
- Discovers all counties from your parcel database
- Loads comprehensive agricultural statistics for each county
- Stores data in Snowflake for AI model feature engineering
- Provides county-level agricultural context for all parcels
- 100% reliable data source (no API failures like CDL)
"""

import os
import sys
import json
import time
import logging
import requests
import snowflake.connector
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('nass_quickstats_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NASSQuickStatsCountyLoader:
    """Load county-level agricultural data from NASS QuickStats API into Snowflake"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('NASS_API_KEY')
        if not self.api_key:
            raise ValueError("NASS API key required. Set NASS_API_KEY environment variable or pass api_key parameter.")
        
        self.snowflake_config = self._get_snowflake_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BirdDog-NASS-County-Loader/1.0'
        })
        
        # NASS QuickStats API configuration
        self.base_url = "https://quickstats.nass.usda.gov/api"
        self.rate_limit_delay = 0.1  # 10 requests per second (well under 35,000/hour limit)
        self.max_retries = 3
        
        # Thread safety
        self._lock = threading.Lock()
        self._api_call_count = 0
        self._last_api_call = 0
        
        # Major agricultural commodities to collect
        self.commodities = [
            'CORN', 'SOYBEANS', 'WHEAT', 'COTTON', 'RICE', 'SORGHUM',
            'BARLEY', 'OATS', 'SUNFLOWER', 'PEANUTS', 'POTATOES',
            'SUGAR BEETS', 'SUGARCANE', 'TOBACCO', 'HAY', 'ALFALFA',
            'CATTLE', 'HOGS', 'SHEEP', 'CHICKENS', 'TURKEYS'
        ]
        
        # Key statistics to collect
        self.statistics = [
            'AREA PLANTED', 'AREA HARVESTED', 'PRODUCTION', 'YIELD',
            'PRICE RECEIVED', 'VALUE OF PRODUCTION', 'INVENTORY'
        ]
    
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
    
    def setup_county_agricultural_data_table(self):
        """Create county agricultural data table in Snowflake"""
        logger.info("Setting up county agricultural data table in Snowflake...")
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS COUNTY_AGRICULTURAL_DATA (
                COUNTY_NAME VARCHAR(255) NOT NULL,
                STATE_CODE VARCHAR(10) NOT NULL,
                COUNTY_CODE VARCHAR(10),
                DATA_YEAR INTEGER NOT NULL,
                COMMODITY VARCHAR(100) NOT NULL,
                STATISTIC_CATEGORY VARCHAR(100) NOT NULL,
                VALUE_NUMERIC FLOAT,
                VALUE_TEXT VARCHAR(255),
                UNIT_DESCRIPTION VARCHAR(100),
                DOMAIN_CATEGORY VARCHAR(100),
                AGG_LEVEL_DESC VARCHAR(50),
                FREQ_DESC VARCHAR(50),
                REFERENCE_PERIOD_DESC VARCHAR(100),
                SOURCE_DESC VARCHAR(100),
                NASS_RAW_DATA VARIANT,
                DATA_SOURCE VARCHAR(100) DEFAULT 'USDA_NASS_QUICKSTATS',
                COLLECTION_DATE DATE DEFAULT CURRENT_DATE,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (COUNTY_NAME, STATE_CODE, DATA_YEAR, COMMODITY, STATISTIC_CATEGORY)
            )
            """
            
            cursor.execute(create_table_sql)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS IDX_COUNTY_AG_STATE_COUNTY ON COUNTY_AGRICULTURAL_DATA(STATE_CODE, COUNTY_NAME)",
                "CREATE INDEX IF NOT EXISTS IDX_COUNTY_AG_YEAR ON COUNTY_AGRICULTURAL_DATA(DATA_YEAR)",
                "CREATE INDEX IF NOT EXISTS IDX_COUNTY_AG_COMMODITY ON COUNTY_AGRICULTURAL_DATA(COMMODITY)",
                "CREATE INDEX IF NOT EXISTS IDX_COUNTY_AG_STATISTIC ON COUNTY_AGRICULTURAL_DATA(STATISTIC_CATEGORY)"
            ]
            
            for index_sql in indexes:
                try:
                    cursor.execute(index_sql)
                except Exception as e:
                    logger.warning(f"Index creation warning: {e}")
            
            conn.commit()
            logger.info("✅ County agricultural data table setup complete")
            
        except Exception as e:
            logger.error(f"Error setting up county agricultural data table: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def get_counties_from_parcels(self) -> Set[Tuple[str, str]]:
        """Get unique counties from PARCEL_PROFILE table"""
        logger.info("Discovering counties from PARCEL_PROFILE table...")
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Get unique state/county combinations
            query = """
            SELECT DISTINCT 
                UPPER(STATE_CODE) as state_code,
                UPPER(COUNTY_ID) as county_name
            FROM PARCEL_PROFILE
            WHERE STATE_CODE IS NOT NULL 
              AND COUNTY_ID IS NOT NULL
              AND STATE_CODE != ''
              AND COUNTY_ID != ''
            ORDER BY state_code, county_name
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            counties = set()
            for row in results:
                state_code, county_name = row
                # Clean up county name (remove "COUNTY" suffix if present)
                clean_county = county_name.replace(' COUNTY', '').replace(' CO', '').strip()
                counties.add((state_code, clean_county))
            
            logger.info(f"Found {len(counties)} unique counties across {len(set(c[0] for c in counties))} states")
            return counties
            
        except Exception as e:
            logger.error(f"Error getting counties from parcels: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _rate_limited_api_call(self, url: str, params: Dict) -> requests.Response:
        """Make rate-limited API call to NASS QuickStats"""
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
    
    def get_county_agricultural_data(self, state_code: str, county_name: str, year: int, 
                                   commodity: str = None, statistic: str = None) -> List[Dict]:
        """Get agricultural data for specific county and year with flexible parameters"""
        try:
            # Base parameters that are always included
            params = {
                'key': self.api_key,
                'agg_level_desc': 'COUNTY',
                'state_alpha': state_code,
                'county_name': county_name,
                'year': year,
                'format': 'JSON'
            }
            
            # Add optional filters only if specified
            if commodity:
                params['commodity_desc'] = commodity
            if statistic:
                params['statisticcat_desc'] = statistic
            
            url = f"{self.base_url}/api_GET"
            response = self._rate_limited_api_call(url, params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' in data and data['data']:
                return data['data']
            else:
                return []
                
        except Exception as e:
            logger.debug(f"Error getting data for {state_code} {county_name} {year} {commodity} {statistic}: {e}")
            return []
    
    def get_all_county_data(self, state_code: str, county_name: str, year: int) -> List[Dict]:
        """Get all available agricultural data for a county and year"""
        try:
            # First, try to get all data without filters
            params = {
                'key': self.api_key,
                'agg_level_desc': 'COUNTY',
                'state_alpha': state_code,
                'county_name': county_name,
                'year': year,
                'format': 'JSON'
            }
            
            url = f"{self.base_url}/api_GET"
            response = self._rate_limited_api_call(url, params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' in data and data['data']:
                return data['data']
            else:
                return []
                
        except Exception as e:
            logger.debug(f"Error getting all data for {state_code} {county_name} {year}: {e}")
            return []
    
    def process_county_data(self, state_code: str, county_name: str, years: List[int]) -> List[Dict]:
        """Process all agricultural data for a single county using flexible approach"""
        logger.info(f"Processing {county_name} County, {state_code}")
        
        all_records = []
        successful_years = 0
        
        for year in years:
            # Try to get all data for this county/year without restrictive filters
            year_records = self.get_all_county_data(state_code, county_name, year)
            
            if year_records:
                successful_years += 1
                for record in year_records:
                    processed_record = self._process_nass_record(record)
                    if processed_record:
                        all_records.append(processed_record)
                        
                logger.debug(f"  {year}: {len(year_records)} records found")
            else:
                logger.debug(f"  {year}: No data found")
        
        success_rate = (successful_years / len(years) * 100) if years else 0
        logger.info(f"  ✓ {county_name} County, {state_code}: {len(all_records)} records from {successful_years}/{len(years)} years, {success_rate:.1f}% year success rate")
        
        return all_records
    
    def _process_nass_record(self, record: Dict) -> Optional[Dict]:
        """Process a single NASS record into our database format"""
        try:
            # Extract numeric value if possible
            value_text = record.get('Value', '').strip()
            value_numeric = None
            
            if value_text and value_text not in ['(D)', '(Z)', '(NA)', '']:
                # Remove commas and try to convert to float
                clean_value = value_text.replace(',', '').replace('$', '')
                try:
                    value_numeric = float(clean_value)
                except ValueError:
                    pass
            
            return {
                'county_name': record.get('county_name', '').upper(),
                'state_code': record.get('state_alpha', '').upper(),
                'county_code': record.get('county_code', ''),
                'data_year': int(record.get('year', 0)),
                'commodity': record.get('commodity_desc', '').upper(),
                'statistic_category': record.get('statisticcat_desc', '').upper(),
                'value_numeric': value_numeric,
                'value_text': value_text,
                'unit_description': record.get('unit_desc', ''),
                'domain_category': record.get('domaincat_desc', ''),
                'agg_level_desc': record.get('agg_level_desc', ''),
                'freq_desc': record.get('freq_desc', ''),
                'reference_period_desc': record.get('reference_period_desc', ''),
                'source_desc': record.get('source_desc', ''),
                'nass_raw_data': record
            }
            
        except Exception as e:
            logger.error(f"Error processing NASS record: {e}")
            return None
    
    def save_county_agricultural_data(self, records: List[Dict]):
        """Save county agricultural data to Snowflake"""
        if not records:
            return
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert using MERGE for upsert functionality
            merge_sql = """
            MERGE INTO COUNTY_AGRICULTURAL_DATA AS target
            USING (SELECT 
                %s as COUNTY_NAME, %s as STATE_CODE, %s as COUNTY_CODE, %s as DATA_YEAR,
                %s as COMMODITY, %s as STATISTIC_CATEGORY, %s as VALUE_NUMERIC, %s as VALUE_TEXT,
                %s as UNIT_DESCRIPTION, %s as DOMAIN_CATEGORY, %s as AGG_LEVEL_DESC,
                %s as FREQ_DESC, %s as REFERENCE_PERIOD_DESC, %s as SOURCE_DESC,
                PARSE_JSON(%s) as NASS_RAW_DATA
            ) AS source
            ON target.COUNTY_NAME = source.COUNTY_NAME 
               AND target.STATE_CODE = source.STATE_CODE
               AND target.DATA_YEAR = source.DATA_YEAR
               AND target.COMMODITY = source.COMMODITY
               AND target.STATISTIC_CATEGORY = source.STATISTIC_CATEGORY
            WHEN MATCHED THEN UPDATE SET
                VALUE_NUMERIC = source.VALUE_NUMERIC,
                VALUE_TEXT = source.VALUE_TEXT,
                UNIT_DESCRIPTION = source.UNIT_DESCRIPTION,
                UPDATED_AT = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN INSERT (
                COUNTY_NAME, STATE_CODE, COUNTY_CODE, DATA_YEAR, COMMODITY, STATISTIC_CATEGORY,
                VALUE_NUMERIC, VALUE_TEXT, UNIT_DESCRIPTION, DOMAIN_CATEGORY, AGG_LEVEL_DESC,
                FREQ_DESC, REFERENCE_PERIOD_DESC, SOURCE_DESC, NASS_RAW_DATA
            ) VALUES (
                source.COUNTY_NAME, source.STATE_CODE, source.COUNTY_CODE, source.DATA_YEAR,
                source.COMMODITY, source.STATISTIC_CATEGORY, source.VALUE_NUMERIC, source.VALUE_TEXT,
                source.UNIT_DESCRIPTION, source.DOMAIN_CATEGORY, source.AGG_LEVEL_DESC,
                source.FREQ_DESC, source.REFERENCE_PERIOD_DESC, source.SOURCE_DESC, source.NASS_RAW_DATA
            )
            """
            
            saved_count = 0
            for record in records:
                cursor.execute(merge_sql, (
                    record['county_name'],
                    record['state_code'],
                    record['county_code'],
                    record['data_year'],
                    record['commodity'],
                    record['statistic_category'],
                    record['value_numeric'],
                    record['value_text'],
                    record['unit_description'],
                    record['domain_category'],
                    record['agg_level_desc'],
                    record['freq_desc'],
                    record['reference_period_desc'],
                    record['source_desc'],
                    json.dumps(record['nass_raw_data'])
                ))
                saved_count += 1
            
            conn.commit()
            logger.info(f"Saved {saved_count} county agricultural records")
            
        except Exception as e:
            logger.error(f"Error saving county agricultural data: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def run_county_data_collection(self, 
                                 years: Optional[List[int]] = None,
                                 max_counties: Optional[int] = None,
                                 state_filter: Optional[str] = None,
                                 batch_size: int = 50):
        """Run the complete county agricultural data collection process"""
        
        if years is None:
            # Default to last 5 years
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year))
        
        logger.info(f"Starting NASS QuickStats county agricultural data collection")
        logger.info(f"Years: {years}")
        logger.info(f"Max counties: {max_counties or 'unlimited'}")
        logger.info(f"State filter: {state_filter or 'all states'}")
        logger.info(f"Batch size: {batch_size}")
        
        # Setup database table
        self.setup_county_agricultural_data_table()
        
        # Get counties to process
        all_counties = self.get_counties_from_parcels()
        
        # Apply filters
        if state_filter:
            all_counties = {(s, c) for s, c in all_counties if s == state_filter.upper()}
        
        counties_list = list(all_counties)
        if max_counties:
            counties_list = counties_list[:max_counties]
        
        if not counties_list:
            logger.info("No counties found for processing")
            return
        
        logger.info(f"Processing {len(counties_list)} counties for {len(years)} years each")
        
        # Process counties in batches
        processed = 0
        total_records = 0
        batch_records = []
        
        start_time = datetime.now()
        
        for i, (state_code, county_name) in enumerate(counties_list):
            try:
                # Process agricultural data for this county
                county_records = self.process_county_data(state_code, county_name, years)
                batch_records.extend(county_records)
                total_records += len(county_records)
                
                processed += 1
                
                # Save batch when it reaches batch_size
                if len(batch_records) >= batch_size:
                    self.save_county_agricultural_data(batch_records)
                    batch_records = []
                
                # Progress reporting
                if processed % 10 == 0:
                    elapsed = datetime.now() - start_time
                    rate = processed / elapsed.total_seconds() * 60  # counties per minute
                    remaining = len(counties_list) - processed
                    eta = timedelta(minutes=remaining / rate) if rate > 0 else timedelta(0)
                    
                    logger.info(f"Progress: {processed}/{len(counties_list)} counties ({processed/len(counties_list)*100:.1f}%)")
                    logger.info(f"Rate: {rate:.1f} counties/min, ETA: {eta}")
                    logger.info(f"Total records collected: {total_records:,}")
                
            except Exception as e:
                logger.error(f"Error processing county {county_name}, {state_code}: {e}")
                continue
        
        # Save any remaining records
        if batch_records:
            self.save_county_agricultural_data(batch_records)
        
        # Final summary
        elapsed = datetime.now() - start_time
        logger.info(f"County agricultural data collection completed!")
        logger.info(f"Processed: {processed} counties")
        logger.info(f"Total records: {total_records:,}")
        logger.info(f"Total time: {elapsed}")
        logger.info(f"Average rate: {processed/elapsed.total_seconds()*60:.1f} counties/min")
        logger.info(f"Total API calls: {self._api_call_count}")
    
    def get_progress_report(self) -> Dict:
        """Get progress report of county agricultural data collection"""
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Overall statistics
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT CONCAT(STATE_CODE, '-', COUNTY_NAME)) as total_counties,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN VALUE_NUMERIC IS NOT NULL THEN 1 END) as records_with_numeric_values,
                    MIN(DATA_YEAR) as earliest_year,
                    MAX(DATA_YEAR) as latest_year,
                    COUNT(DISTINCT COMMODITY) as unique_commodities,
                    COUNT(DISTINCT STATISTIC_CATEGORY) as unique_statistics
                FROM COUNTY_AGRICULTURAL_DATA
            """)
            
            overall_stats = cursor.fetchone()
            
            # By state statistics
            cursor.execute("""
                SELECT 
                    STATE_CODE,
                    COUNT(DISTINCT COUNTY_NAME) as counties,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN VALUE_NUMERIC IS NOT NULL THEN 1 END) as numeric_records
                FROM COUNTY_AGRICULTURAL_DATA
                GROUP BY STATE_CODE
                ORDER BY counties DESC
            """)
            
            state_stats = cursor.fetchall()
            
            # Top commodities
            cursor.execute("""
                SELECT 
                    COMMODITY,
                    COUNT(DISTINCT CONCAT(STATE_CODE, '-', COUNTY_NAME)) as counties,
                    COUNT(*) as total_records,
                    AVG(VALUE_NUMERIC) as avg_value
                FROM COUNTY_AGRICULTURAL_DATA
                WHERE VALUE_NUMERIC IS NOT NULL
                GROUP BY COMMODITY
                ORDER BY counties DESC
                LIMIT 20
            """)
            
            commodity_stats = cursor.fetchall()
            
            report = {
                'overall': {
                    'total_counties': overall_stats[0],
                    'total_records': overall_stats[1],
                    'records_with_numeric_values': overall_stats[2],
                    'numeric_data_percentage': (overall_stats[2] / overall_stats[1] * 100) if overall_stats[1] > 0 else 0,
                    'earliest_year': overall_stats[3],
                    'latest_year': overall_stats[4],
                    'unique_commodities': overall_stats[5],
                    'unique_statistics': overall_stats[6]
                },
                'by_state': [
                    {
                        'state': row[0],
                        'counties': row[1],
                        'total_records': row[2],
                        'numeric_records': row[3],
                        'numeric_percentage': (row[3] / row[2] * 100) if row[2] > 0 else 0
                    }
                    for row in state_stats
                ],
                'top_commodities': [
                    {
                        'commodity': row[0],
                        'counties': row[1],
                        'total_records': row[2],
                        'avg_value': float(row[3]) if row[3] else None
                    }
                    for row in commodity_stats
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
    
    parser = argparse.ArgumentParser(description='NASS QuickStats County Agricultural Data Loader')
    parser.add_argument('--api-key', type=str, help='NASS API key (or set NASS_API_KEY env var)')
    parser.add_argument('--max-counties', type=int, help='Maximum number of counties to process')
    parser.add_argument('--years', nargs='+', type=int, help='Years to process (e.g., 2020 2021 2022)')
    parser.add_argument('--state', type=str, help='State code to filter (e.g., TX)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for database operations')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited counties')
    parser.add_argument('--report', action='store_true', help='Generate progress report only')
    
    args = parser.parse_args()
    
    try:
        loader = NASSQuickStatsCountyLoader(api_key=args.api_key)
        
        if args.report:
            logger.info("Generating progress report...")
            report = loader.get_progress_report()
            
            print("\n" + "="*80)
            print("NASS QUICKSTATS COUNTY DATA PROGRESS REPORT")
            print("="*80)
            print(f"Total Counties: {report['overall']['total_counties']:,}")
            print(f"Total Records: {report['overall']['total_records']:,}")
            print(f"Records with Numeric Values: {report['overall']['numeric_data_percentage']:.1f}%")
            print(f"Years Covered: {report['overall']['earliest_year']} - {report['overall']['latest_year']}")
            print(f"Unique Commodities: {report['overall']['unique_commodities']}")
            print(f"Unique Statistics: {report['overall']['unique_statistics']}")
            
            print(f"\nTop Commodities by County Coverage:")
            for commodity in report['top_commodities'][:10]:
                print(f"  {commodity['commodity']}: {commodity['counties']:,} counties, {commodity['total_records']:,} records")
            
            print(f"\nBy State (Top 15):")
            for state in report['by_state'][:15]:
                print(f"  {state['state']}: {state['counties']:,} counties, {state['total_records']:,} records ({state['numeric_percentage']:.1f}% numeric)")
            
            # Save detailed report
            with open('nass_county_data_report.json', 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info("Detailed report saved to nass_county_data_report.json")
            
        else:
            # Set defaults for test mode
            if args.test:
                max_counties = 5
                years = [2022, 2023]
                logger.info("Running in test mode with 5 counties and years 2022-2023")
            else:
                max_counties = args.max_counties
                years = args.years
            
            # Run the collection
            loader.run_county_data_collection(
                years=years,
                max_counties=max_counties,
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
