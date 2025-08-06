"""
HubSpot ETL Pipeline

Main orchestration class that coordinates the extraction, transformation, and loading
of HubSpot data into Snowflake.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import json
import os
from pathlib import Path

from .hubspot_client import HubSpotClient, HubSpotAPIError, HubSpotAuthError
from .data_transformer import HubSpotDataTransformer
from .snowflake_loader import SnowflakeLoader, SnowflakeLoadError, SnowflakeConnectionError

logger = logging.getLogger(__name__)


class ETLPipelineError(Exception):
    """Raised when ETL pipeline encounters errors"""
    pass


class HubSpotETLPipeline:
    """
    Complete ETL pipeline for HubSpot to Snowflake data integration.
    
    Features:
    - Full and incremental data extraction
    - Configurable object types and properties
    - Data quality validation
    - Error handling and recovery
    - Progress tracking and logging
    - Flexible scheduling support
    """
    
    # Default object types to extract
    DEFAULT_OBJECT_TYPES = ['contacts', 'companies', 'deals', 'tickets']
    
    def __init__(self,
                 hubspot_client: Optional[HubSpotClient] = None,
                 snowflake_loader: Optional[SnowflakeLoader] = None,
                 data_transformer: Optional[HubSpotDataTransformer] = None,
                 config: Optional[Dict[str, Any]] = None):
        """
        Initialize ETL pipeline with components and configuration.
        
        Args:
            hubspot_client: HubSpot API client
            snowflake_loader: Snowflake data loader
            data_transformer: Data transformation component
            config: Pipeline configuration
        """
        self.hubspot_client = hubspot_client or HubSpotClient()
        self.snowflake_loader = snowflake_loader or SnowflakeLoader()
        self.data_transformer = data_transformer or HubSpotDataTransformer()
        
        # Load configuration
        self.config = config or self._load_default_config()
        
        # Pipeline state
        self.pipeline_state = {
            'last_run': None,
            'last_successful_run': None,
            'object_states': {},
            'errors': []
        }
        
        # Load existing state if available
        self._load_pipeline_state()
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default pipeline configuration"""
        return {
            'object_types': self.DEFAULT_OBJECT_TYPES,
            'batch_size': 10000,
            'chunk_size': 1000,
            'max_retries': 3,
            'retry_delay': 60,  # seconds
            'incremental_mode': True,
            'lookback_days': 7,  # For incremental loads
            'data_quality_checks': True,
            'create_tables': True,
            'table_prefix': 'HUBSPOT_',
            'schema_name': 'HUBSPOT',
            'state_file': 'hubspot_etl_state.json'
        }
    
    def _load_pipeline_state(self):
        """Load pipeline state from file"""
        state_file = self.config.get('state_file', 'hubspot_etl_state.json')
        
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r') as f:
                    self.pipeline_state = json.load(f)
                logger.info(f"Loaded pipeline state from {state_file}")
            except Exception as e:
                logger.warning(f"Failed to load pipeline state: {e}")
    
    def _save_pipeline_state(self):
        """Save pipeline state to file"""
        state_file = self.config.get('state_file', 'hubspot_etl_state.json')
        
        try:
            # Convert datetime objects to strings for JSON serialization
            state_to_save = self.pipeline_state.copy()
            if state_to_save.get('last_run'):
                state_to_save['last_run'] = state_to_save['last_run'].isoformat()
            if state_to_save.get('last_successful_run'):
                state_to_save['last_successful_run'] = state_to_save['last_successful_run'].isoformat()
            
            with open(state_file, 'w') as f:
                json.dump(state_to_save, f, indent=2, default=str)
            logger.info(f"Saved pipeline state to {state_file}")
        except Exception as e:
            logger.error(f"Failed to save pipeline state: {e}")
    
    def run_full_extraction(self, 
                           object_types: Optional[List[str]] = None,
                           max_records_per_type: Optional[int] = None) -> Dict[str, Any]:
        """
        Run full data extraction for specified object types.
        
        Args:
            object_types: List of object types to extract
            max_records_per_type: Maximum records per object type
            
        Returns:
            Extraction results summary
        """
        logger.info("Starting full HubSpot data extraction")
        
        object_types = object_types or self.config['object_types']
        results = {
            'start_time': datetime.now(),
            'object_results': {},
            'total_records': 0,
            'errors': [],
            'status': 'running'
        }
        
        try:
            # Test connections
            self._test_connections()
            
            # Process each object type
            for object_type in object_types:
                logger.info(f"Processing {object_type}...")
                
                try:
                    object_result = self._extract_and_load_object_type(
                        object_type=object_type,
                        incremental=False,
                        max_records=max_records_per_type
                    )
                    
                    results['object_results'][object_type] = object_result
                    results['total_records'] += object_result.get('records_loaded', 0)
                    
                except Exception as e:
                    error_msg = f"Failed to process {object_type}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['object_results'][object_type] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            # Update pipeline state
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
            results['status'] = 'completed' if not results['errors'] else 'completed_with_errors'
            
            self.pipeline_state['last_run'] = results['end_time']
            if results['status'] == 'completed':
                self.pipeline_state['last_successful_run'] = results['end_time']
            
            self._save_pipeline_state()
            
            logger.info(f"Full extraction completed. Total records: {results['total_records']}, "
                       f"Duration: {results['duration']:.2f}s, Errors: {len(results['errors'])}")
            
            return results
            
        except Exception as e:
            results['status'] = 'failed'
            results['errors'].append(str(e))
            logger.error(f"Full extraction failed: {e}")
            raise ETLPipelineError(f"Full extraction failed: {e}")
    
    def run_incremental_extraction(self, 
                                  object_types: Optional[List[str]] = None,
                                  since: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Run incremental data extraction for specified object types.
        
        Args:
            object_types: List of object types to extract
            since: Extract data modified since this datetime
            
        Returns:
            Extraction results summary
        """
        logger.info("Starting incremental HubSpot data extraction")
        
        object_types = object_types or self.config['object_types']
        
        # Determine since date
        if since is None:
            if self.pipeline_state.get('last_successful_run'):
                since = datetime.fromisoformat(self.pipeline_state['last_successful_run'])
            else:
                # Fallback to lookback days
                lookback_days = self.config.get('lookback_days', 7)
                since = datetime.now() - timedelta(days=lookback_days)
        
        logger.info(f"Extracting data modified since: {since}")
        
        results = {
            'start_time': datetime.now(),
            'since_date': since,
            'object_results': {},
            'total_records': 0,
            'errors': [],
            'status': 'running'
        }
        
        try:
            # Test connections
            self._test_connections()
            
            # Process each object type
            for object_type in object_types:
                logger.info(f"Processing {object_type} incrementally...")
                
                try:
                    object_result = self._extract_and_load_object_type(
                        object_type=object_type,
                        incremental=True,
                        since_date=since
                    )
                    
                    results['object_results'][object_type] = object_result
                    results['total_records'] += object_result.get('records_loaded', 0)
                    
                except Exception as e:
                    error_msg = f"Failed to process {object_type}: {str(e)}"
                    logger.error(error_msg)
                    results['errors'].append(error_msg)
                    results['object_results'][object_type] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            # Update results
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
            results['status'] = 'completed' if not results['errors'] else 'completed_with_errors'
            
            # Update pipeline state
            self.pipeline_state['last_run'] = results['end_time']
            if results['status'] == 'completed':
                self.pipeline_state['last_successful_run'] = results['end_time']
            
            self._save_pipeline_state()
            
            logger.info(f"Incremental extraction completed. Total records: {results['total_records']}, "
                       f"Duration: {results['duration']:.2f}s, Errors: {len(results['errors'])}")
            
            return results
            
        except Exception as e:
            results['status'] = 'failed'
            results['errors'].append(str(e))
            logger.error(f"Incremental extraction failed: {e}")
            raise ETLPipelineError(f"Incremental extraction failed: {e}")
    
    def _extract_and_load_object_type(self,
                                     object_type: str,
                                     incremental: bool = False,
                                     since_date: Optional[datetime] = None,
                                     max_records: Optional[int] = None) -> Dict[str, Any]:
        """
        Extract and load data for a specific object type.
        
        Args:
            object_type: HubSpot object type
            incremental: Whether to do incremental extraction
            since_date: For incremental, extract since this date
            max_records: Maximum records to extract
            
        Returns:
            Processing results
        """
        start_time = datetime.now()
        
        try:
            # Extract data from HubSpot
            if incremental and since_date:
                logger.info(f"Extracting {object_type} modified since {since_date}")
                raw_data = self.hubspot_client.get_recently_modified_objects(
                    object_type=object_type,
                    since=since_date
                )
            else:
                logger.info(f"Extracting all {object_type}")
                raw_data = self.hubspot_client.get_all_objects(
                    object_type=object_type,
                    max_results=max_records
                )
            
            if not raw_data:
                logger.info(f"No {object_type} data found")
                return {
                    'status': 'completed',
                    'records_extracted': 0,
                    'records_loaded': 0,
                    'duration': (datetime.now() - start_time).total_seconds()
                }
            
            logger.info(f"Extracted {len(raw_data)} {object_type} records")
            
            # Transform data
            logger.info(f"Transforming {object_type} data...")
            df = self.data_transformer.transform_objects(raw_data, object_type)
            
            # Data quality validation
            if self.config.get('data_quality_checks', True):
                quality_report = self.data_transformer.validate_data_quality(df, object_type)
                logger.info(f"Data quality report for {object_type}: {quality_report['status']}")
                
                if quality_report['issues']:
                    logger.warning(f"Data quality issues for {object_type}: {quality_report['issues']}")
            
            # Load into Snowflake
            table_name = f"{self.config.get('table_prefix', 'HUBSPOT_')}{object_type.upper()}"
            
            logger.info(f"Loading {len(df)} {object_type} records into {table_name}...")
            
            with self.snowflake_loader as loader:
                # Create schema if needed
                loader.create_schema_if_not_exists(self.config.get('schema_name', 'HUBSPOT'))
                
                # Load data
                if incremental:
                    # For incremental loads, use upsert
                    load_result = loader.upsert_data(
                        df=df,
                        table_name=table_name,
                        key_columns=['ID']
                    )
                else:
                    # For full loads, replace table
                    load_result = loader.load_dataframe(
                        df=df,
                        table_name=table_name,
                        if_exists='replace',
                        chunk_size=self.config.get('chunk_size', 1000),
                        create_table=self.config.get('create_tables', True)
                    )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                'status': 'completed',
                'records_extracted': len(raw_data),
                'records_loaded': load_result.get('rows_loaded', len(df)),
                'duration': duration,
                'table_name': table_name
            }
            
            logger.info(f"Successfully processed {object_type}: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to process {object_type}: {e}")
            raise
    
    def _test_connections(self):
        """Test connections to HubSpot and Snowflake"""
        logger.info("Testing connections...")
        
        # Test HubSpot connection
        if not self.hubspot_client.test_connection():
            raise ETLPipelineError("HubSpot connection test failed")
        
        # Test Snowflake connection
        if not self.snowflake_loader.test_connection():
            raise ETLPipelineError("Snowflake connection test failed")
        
        logger.info("Connection tests passed")
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status and statistics.
        
        Returns:
            Pipeline status information
        """
        status = {
            'pipeline_state': self.pipeline_state.copy(),
            'config': self.config,
            'hubspot_api_usage': None,
            'snowflake_tables': {}
        }
        
        try:
            # Get HubSpot API usage
            status['hubspot_api_usage'] = self.hubspot_client.get_api_usage()
        except Exception as e:
            logger.warning(f"Failed to get HubSpot API usage: {e}")
        
        try:
            # Get Snowflake table information
            with self.snowflake_loader as loader:
                for object_type in self.config['object_types']:
                    table_name = f"{self.config.get('table_prefix', 'HUBSPOT_')}{object_type.upper()}"
                    try:
                        table_info = loader.get_table_info(table_name)
                        status['snowflake_tables'][object_type] = table_info
                    except Exception as e:
                        logger.warning(f"Failed to get info for table {table_name}: {e}")
        except Exception as e:
            logger.warning(f"Failed to get Snowflake table info: {e}")
        
        return status
    
    def run_data_quality_check(self, object_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run data quality checks on loaded data.
        
        Args:
            object_types: Object types to check
            
        Returns:
            Data quality report
        """
        object_types = object_types or self.config['object_types']
        
        quality_report = {
            'timestamp': datetime.now(),
            'object_reports': {},
            'overall_status': 'passed'
        }
        
        try:
            with self.snowflake_loader as loader:
                for object_type in object_types:
                    table_name = f"{self.config.get('table_prefix', 'HUBSPOT_')}{object_type.upper()}"
                    
                    try:
                        # Get table info
                        table_info = loader.get_table_info(table_name)
                        
                        # Basic quality checks
                        checks = {
                            'table_exists': True,
                            'row_count': table_info['row_count'],
                            'column_count': len(table_info['columns']),
                            'has_data': table_info['row_count'] > 0,
                            'issues': []
                        }
                        
                        # Check for required columns
                        column_names = [col['COLUMN_NAME'].lower() for col in table_info['columns']]
                        required_columns = ['id', 'etl_loaded_at']
                        
                        for req_col in required_columns:
                            if req_col not in column_names:
                                checks['issues'].append(f"Missing required column: {req_col}")
                        
                        # Check data freshness
                        if checks['has_data']:
                            freshness_query = f"""
                            SELECT MAX(ETL_LOADED_AT) as latest_load
                            FROM {table_name}
                            """
                            result = loader.execute_query(freshness_query)
                            if result:
                                latest_load = result[0]['LATEST_LOAD']
                                if latest_load:
                                    hours_old = (datetime.now() - latest_load).total_seconds() / 3600
                                    if hours_old > 48:  # Data older than 48 hours
                                        checks['issues'].append(f"Data is {hours_old:.1f} hours old")
                        
                        checks['status'] = 'passed' if not checks['issues'] else 'warning'
                        quality_report['object_reports'][object_type] = checks
                        
                        if checks['status'] != 'passed':
                            quality_report['overall_status'] = 'warning'
                        
                    except Exception as e:
                        quality_report['object_reports'][object_type] = {
                            'table_exists': False,
                            'status': 'failed',
                            'error': str(e)
                        }
                        quality_report['overall_status'] = 'failed'
        
        except Exception as e:
            quality_report['overall_status'] = 'failed'
            quality_report['error'] = str(e)
        
        return quality_report
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> Dict[str, Any]:
        """
        Clean up old data from Snowflake tables.
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Cleanup results
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        cleanup_results = {
            'cutoff_date': cutoff_date,
            'tables_processed': {},
            'total_rows_deleted': 0
        }
        
        try:
            with self.snowflake_loader as loader:
                for object_type in self.config['object_types']:
                    table_name = f"{self.config.get('table_prefix', 'HUBSPOT_')}{object_type.upper()}"
                    
                    try:
                        # Delete old records
                        delete_query = f"""
                        DELETE FROM {table_name}
                        WHERE ETL_LOADED_AT < '{cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}'
                        """
                        
                        result = loader.execute_query(delete_query)
                        rows_deleted = result[0].get('number of rows deleted', 0) if result else 0
                        
                        cleanup_results['tables_processed'][object_type] = {
                            'table_name': table_name,
                            'rows_deleted': rows_deleted,
                            'status': 'completed'
                        }
                        
                        cleanup_results['total_rows_deleted'] += rows_deleted
                        
                        logger.info(f"Deleted {rows_deleted} old records from {table_name}")
                        
                    except Exception as e:
                        cleanup_results['tables_processed'][object_type] = {
                            'table_name': table_name,
                            'status': 'failed',
                            'error': str(e)
                        }
                        logger.error(f"Failed to cleanup {table_name}: {e}")
        
        except Exception as e:
            cleanup_results['error'] = str(e)
            logger.error(f"Cleanup operation failed: {e}")
        
        return cleanup_results
