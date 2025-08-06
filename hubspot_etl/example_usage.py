"""
Example usage of HubSpot ETL Pipeline

This script demonstrates how to use the HubSpot to Snowflake ETL pipeline
with various configuration options and use cases.
"""

import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

from hubspot_etl import HubSpotETLPipeline, HubSpotClient, SnowflakeLoader, HubSpotDataTransformer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hubspot_etl.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def example_full_extraction():
    """Example: Full data extraction from HubSpot to Snowflake"""
    logger.info("=== Example: Full Data Extraction ===")
    
    # Custom configuration
    config = {
        'object_types': ['contacts', 'companies', 'deals'],
        'batch_size': 5000,
        'chunk_size': 1000,
        'table_prefix': 'HUBSPOT_',
        'schema_name': 'CRM_DATA',
        'create_tables': True,
        'data_quality_checks': True
    }
    
    # Initialize pipeline
    pipeline = HubSpotETLPipeline(config=config)
    
    try:
        # Run full extraction
        results = pipeline.run_full_extraction(
            object_types=['contacts', 'companies'],
            max_records_per_type=10000  # Limit for testing
        )
        
        logger.info(f"Full extraction results: {results}")
        
        # Check pipeline status
        status = pipeline.get_pipeline_status()
        logger.info(f"Pipeline status: {status}")
        
    except Exception as e:
        logger.error(f"Full extraction failed: {e}")


def example_incremental_extraction():
    """Example: Incremental data extraction"""
    logger.info("=== Example: Incremental Data Extraction ===")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        # Run incremental extraction (last 24 hours)
        since_date = datetime.now() - timedelta(hours=24)
        
        results = pipeline.run_incremental_extraction(
            object_types=['contacts', 'deals'],
            since=since_date
        )
        
        logger.info(f"Incremental extraction results: {results}")
        
    except Exception as e:
        logger.error(f"Incremental extraction failed: {e}")


def example_custom_components():
    """Example: Using custom components with specific configurations"""
    logger.info("=== Example: Custom Components ===")
    
    # Custom HubSpot client with specific properties
    hubspot_client = HubSpotClient(
        api_key=os.getenv('HUBSPOT_API_KEY'),
        rate_limit_delay=0.2  # Slower rate to be conservative
    )
    
    # Custom Snowflake loader with specific settings
    snowflake_loader = SnowflakeLoader(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='HUBSPOT_CUSTOM'
    )
    
    # Custom data transformer
    data_transformer = HubSpotDataTransformer()
    
    # Custom configuration
    config = {
        'object_types': ['contacts'],
        'batch_size': 1000,
        'incremental_mode': True,
        'lookback_days': 1,
        'table_prefix': 'HS_',
        'schema_name': 'HUBSPOT_CUSTOM'
    }
    
    # Initialize pipeline with custom components
    pipeline = HubSpotETLPipeline(
        hubspot_client=hubspot_client,
        snowflake_loader=snowflake_loader,
        data_transformer=data_transformer,
        config=config
    )
    
    try:
        # Test connections
        logger.info("Testing connections...")
        pipeline._test_connections()
        logger.info("Connections successful!")
        
        # Get specific contact properties
        contacts = hubspot_client.get_all_objects(
            object_type='contacts',
            properties=['firstname', 'lastname', 'email', 'company', 'phone'],
            max_results=100
        )
        
        logger.info(f"Retrieved {len(contacts)} contacts with custom properties")
        
        # Transform and load
        if contacts:
            df = data_transformer.transform_objects(contacts, 'contacts')
            logger.info(f"Transformed data shape: {df.shape}")
            
            # Load to Snowflake
            with snowflake_loader as loader:
                result = loader.load_dataframe(
                    df=df,
                    table_name='HS_CONTACTS_SAMPLE',
                    if_exists='replace'
                )
                logger.info(f"Load result: {result}")
        
    except Exception as e:
        logger.error(f"Custom components example failed: {e}")


def example_data_quality_monitoring():
    """Example: Data quality monitoring and validation"""
    logger.info("=== Example: Data Quality Monitoring ===")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        # Run data quality checks
        quality_report = pipeline.run_data_quality_check()
        
        logger.info(f"Data quality report: {quality_report}")
        
        # Check specific issues
        for object_type, report in quality_report['object_reports'].items():
            if report.get('issues'):
                logger.warning(f"Data quality issues in {object_type}: {report['issues']}")
            else:
                logger.info(f"Data quality check passed for {object_type}")
        
    except Exception as e:
        logger.error(f"Data quality check failed: {e}")


def example_oauth_authentication():
    """Example: Using OAuth authentication instead of API key"""
    logger.info("=== Example: OAuth Authentication ===")
    
    # OAuth configuration
    hubspot_client = HubSpotClient(
        access_token=os.getenv('HUBSPOT_ACCESS_TOKEN'),
        client_id=os.getenv('HUBSPOT_CLIENT_ID'),
        client_secret=os.getenv('HUBSPOT_CLIENT_SECRET'),
        refresh_token=os.getenv('HUBSPOT_REFRESH_TOKEN')
    )
    
    try:
        # Test OAuth connection
        if hubspot_client.test_connection():
            logger.info("OAuth authentication successful")
            
            # Get API usage with OAuth
            usage = hubspot_client.get_api_usage()
            logger.info(f"API usage: {usage}")
            
            # Get recent contacts
            recent_contacts = hubspot_client.get_recently_modified_objects(
                object_type='contacts',
                since=datetime.now() - timedelta(days=1)
            )
            
            logger.info(f"Found {len(recent_contacts)} recently modified contacts")
            
        else:
            logger.error("OAuth authentication failed")
            
    except Exception as e:
        logger.error(f"OAuth example failed: {e}")


def example_bulk_csv_loading():
    """Example: Bulk loading from CSV files"""
    logger.info("=== Example: Bulk CSV Loading ===")
    
    snowflake_loader = SnowflakeLoader()
    
    try:
        with snowflake_loader as loader:
            # Example: Load contacts from CSV
            csv_file = "sample_contacts.csv"
            
            # Create sample CSV for demonstration
            import pandas as pd
            sample_data = pd.DataFrame({
                'ID': ['1', '2', '3'],
                'FIRSTNAME': ['John', 'Jane', 'Bob'],
                'LASTNAME': ['Doe', 'Smith', 'Johnson'],
                'EMAIL': ['john@example.com', 'jane@example.com', 'bob@example.com'],
                'COMPANY': ['Acme Corp', 'Tech Inc', 'Data LLC']
            })
            sample_data.to_csv(csv_file, index=False)
            
            # Bulk load from CSV
            result = loader.bulk_load_from_csv(
                csv_file_path=csv_file,
                table_name='HUBSPOT_CONTACTS_BULK'
            )
            
            logger.info(f"Bulk load result: {result}")
            
            # Clean up
            os.remove(csv_file)
            
    except Exception as e:
        logger.error(f"Bulk CSV loading failed: {e}")


def example_upsert_operations():
    """Example: Upsert operations for incremental updates"""
    logger.info("=== Example: Upsert Operations ===")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        # Get recent deals
        recent_deals = pipeline.hubspot_client.get_recently_modified_objects(
            object_type='deals',
            since=datetime.now() - timedelta(hours=6)
        )
        
        if recent_deals:
            logger.info(f"Found {len(recent_deals)} recently modified deals")
            
            # Transform data
            df = pipeline.data_transformer.transform_objects(recent_deals, 'deals')
            
            # Upsert into existing table
            with pipeline.snowflake_loader as loader:
                result = loader.upsert_data(
                    df=df,
                    table_name='HUBSPOT_DEALS',
                    key_columns=['ID'],
                    update_columns=['DEALNAME', 'AMOUNT', 'DEALSTAGE', 'LASTMODIFIEDDATE']
                )
                
                logger.info(f"Upsert result: {result}")
        else:
            logger.info("No recent deals found for upsert")
            
    except Exception as e:
        logger.error(f"Upsert example failed: {e}")


def example_cleanup_operations():
    """Example: Data cleanup and maintenance"""
    logger.info("=== Example: Data Cleanup ===")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        # Clean up data older than 30 days
        cleanup_result = pipeline.cleanup_old_data(days_to_keep=30)
        
        logger.info(f"Cleanup result: {cleanup_result}")
        logger.info(f"Total rows deleted: {cleanup_result['total_rows_deleted']}")
        
    except Exception as e:
        logger.error(f"Cleanup example failed: {e}")


def main():
    """Run all examples"""
    logger.info("Starting HubSpot ETL Pipeline Examples")
    
    # Check environment variables
    required_vars = [
        'HUBSPOT_API_KEY', 'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 
        'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        logger.info("Please set these variables in your .env file or environment")
        return
    
    # Run examples (comment out as needed)
    try:
        # example_full_extraction()
        # example_incremental_extraction()
        example_custom_components()
        example_data_quality_monitoring()
        # example_oauth_authentication()  # Requires OAuth setup
        # example_bulk_csv_loading()
        # example_upsert_operations()
        # example_cleanup_operations()
        
    except Exception as e:
        logger.error(f"Example execution failed: {e}")
    
    logger.info("Examples completed")


if __name__ == "__main__":
    main()
