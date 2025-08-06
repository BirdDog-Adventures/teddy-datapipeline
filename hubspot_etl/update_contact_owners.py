#!/usr/bin/env python3
"""
Update HubSpot Contacts with Owner Information

This script fetches contact owner data from HubSpot API and updates the 
HUBSPOT_CONTACTS table in Snowflake with the missing owner fields.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from hubspot_client import HubSpotClient
from snowflake_loader import SnowflakeLoader
from data_transformer import HubSpotDataTransformer

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def fetch_contacts_with_owners():
    """Fetch all contacts from HubSpot with owner information."""
    logger.info("Initializing HubSpot client...")
    client = HubSpotClient()
    
    if not client.test_connection():
        raise Exception("HubSpot connection failed")
    
    logger.info("HubSpot connection successful")
    
    # Properties to fetch (including owner fields)
    properties = [
        'firstname', 'lastname', 'email', 'phone', 'company', 'website',
        'jobtitle', 'lifecyclestage', 'lead_status', 'hs_lead_status',
        'createdate', 'lastmodifieddate', 'hs_analytics_source',
        'hs_analytics_source_data_1', 'hs_analytics_source_data_2',
        'hubspot_owner_id', 'hubspot_owner_assigneddate', 'hubspot_team_id',
        'hs_all_owner_ids'
    ]
    
    logger.info("Fetching all contacts from HubSpot...")
    contacts = client.get_all_objects(
        object_type='contacts',
        properties=properties
    )
    
    logger.info(f"Fetched {len(contacts)} contacts from HubSpot")
    return contacts


def update_contacts_in_snowflake(contacts):
    """Update the HUBSPOT_CONTACTS table with owner information."""
    logger.info("Initializing Snowflake loader...")
    loader = SnowflakeLoader()
    
    with loader as sf:
        logger.info("Connected to Snowflake")
        
        # Transform the HubSpot data
        transformer = HubSpotDataTransformer()
        df = transformer.transform_objects(contacts, 'contacts')
        
        logger.info(f"Transformed {len(df)} contact records")
        
        # Check which columns exist in our DataFrame
        owner_columns = [col for col in df.columns 
                        if any(keyword in col.lower() 
                              for keyword in ['owner', 'team'])]
        
        logger.info(f"Owner columns in transformed data: {owner_columns}")
        
        # Show sample of owner data (check actual column names)
        logger.info("Available columns in DataFrame:")
        logger.info(f"  {sorted(df.columns.tolist())}")
        
        # Find the actual owner column name
        owner_col = None
        for col in df.columns:
            if 'owner_id' in col.lower():
                owner_col = col
                break
        
        if owner_col:
            owner_data = df[df[owner_col].notna()]
            logger.info(f"Found {len(owner_data)} contacts with owner data")
            
            if len(owner_data) > 0:
                logger.info("Sample owner data:")
                for idx, row in owner_data.head(5).iterrows():
                    logger.info(f"  ID: {row.get('id', row.get('ID', 'N/A'))} | "
                              f"Owner: {row[owner_col]} | "
                              f"Name: {row.get('firstname', row.get('FIRSTNAME', ''))} "
                              f"{row.get('lastname', row.get('LASTNAME', ''))}")
        else:
            logger.warning("Could not find owner column in DataFrame")
            owner_data = pd.DataFrame()  # Empty DataFrame
        
        # Update records with owner information using MERGE
        if len(owner_data) > 0:
            logger.info("Updating Snowflake table with owner data...")
            
            # Create temporary table with owner data
            temp_table = f"HUBSPOT_CONTACTS_OWNER_UPDATE_{int(datetime.now().timestamp())}"
            
            # Select only the columns we need for the update
            # Map to actual column names in the DataFrame
            column_mapping = {}
            for col in df.columns:
                col_lower = col.lower()
                if col_lower == 'id':
                    column_mapping['ID'] = col
                elif 'hubspot_owner_id' in col_lower:
                    column_mapping['HUBSPOT_OWNER_ID'] = col
                elif 'hubspot_owner_assigned' in col_lower:
                    column_mapping['HUBSPOT_OWNER_ASSIGNEDDATE'] = col
                elif 'hubspot_team_id' in col_lower:
                    column_mapping['HUBSPOT_TEAM_ID'] = col
                elif 'all_owner_ids' in col_lower:
                    column_mapping['HS_ALL_OWNER_IDS'] = col
            
            logger.info(f"Column mapping: {column_mapping}")
            
            # Select columns that exist
            available_columns = [col for col in column_mapping.values() if col in df.columns]
            owner_update_df = df[available_columns].copy()
            
            # Rename columns to match Snowflake schema
            rename_mapping = {v: k for k, v in column_mapping.items() if v in available_columns}
            owner_update_df = owner_update_df.rename(columns=rename_mapping)
            
            # Load to temporary table
            result = loader.load_dataframe(
                df=owner_update_df,
                table_name=temp_table,
                if_exists='replace',
                create_table=True
            )
            
            logger.info(f"Loaded {result['rows_loaded']} records to temporary table")
            
            # Update main table using MERGE
            merge_sql = f"""
            MERGE INTO HUBSPOT_CONTACTS AS target
            USING {temp_table.upper()} AS source
            ON target.ID = source.ID
            WHEN MATCHED THEN
                UPDATE SET 
                    HUBSPOT_OWNER_ID = source.HUBSPOT_OWNER_ID,
                    HUBSPOT_OWNER_ASSIGNEDDATE = source.HUBSPOT_OWNER_ASSIGNEDDATE,
                    HUBSPOT_TEAM_ID = source.HUBSPOT_TEAM_ID,
                    HS_ALL_OWNER_IDS = source.HS_ALL_OWNER_IDS
            """
            
            merge_result = sf.execute_query(merge_sql)
            logger.info(f"MERGE operation completed")
            
            # Clean up temporary table
            sf.execute_query(f"DROP TABLE {temp_table.upper()}")
            logger.info("Cleaned up temporary table")
            
            # Verify the update
            verification_query = """
            SELECT 
                COUNT(*) as total_contacts,
                COUNT(HUBSPOT_OWNER_ID) as contacts_with_owner,
                COUNT(DISTINCT HUBSPOT_OWNER_ID) as unique_owners
            FROM HUBSPOT_CONTACTS
            WHERE HUBSPOT_OWNER_ID IS NOT NULL
            """
            
            verification_result = sf.execute_query(verification_query)
            if verification_result:
                stats = verification_result[0]
                logger.info(f"Update verification:")
                logger.info(f"  - Total contacts: {stats['TOTAL_CONTACTS']:,}")
                logger.info(f"  - Contacts with owner: {stats['CONTACTS_WITH_OWNER']:,}")
                logger.info(f"  - Unique owners: {stats['UNIQUE_OWNERS']:,}")
            
        else:
            logger.warning("No contacts with owner data found to update")


def main():
    """Main execution function."""
    try:
        logger.info("=" * 60)
        logger.info("STARTING HUBSPOT CONTACTS OWNER UPDATE")
        logger.info("=" * 60)
        
        # Fetch contacts from HubSpot
        contacts = fetch_contacts_with_owners()
        
        # Update Snowflake
        update_contacts_in_snowflake(contacts)
        
        logger.info("=" * 60)
        logger.info("HUBSPOT CONTACTS OWNER UPDATE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Update failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()