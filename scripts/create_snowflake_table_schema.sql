-- Snowflake Table Creation Script for Teddy Data Pipeline
-- This script creates the PARCEL_DATA_RAW table to match the existing schema structure
-- Run this as ACCOUNTADMIN or a role with CREATE TABLE privileges

USE ROLE ACCOUNTADMIN;
USE DATABASE TEDDY_DATA;
USE SCHEMA RAW;

-- Drop table if it exists (uncomment if you need to recreate)
-- DROP TABLE IF EXISTS PARCEL_DATA_RAW;

-- Create PARCEL_DATA_RAW table with the correct schema
CREATE TABLE IF NOT EXISTS PARCEL_DATA_RAW (
    FILE_NAME VARCHAR(500) NOT NULL COMMENT 'Name of the source file or S3 key',
    FILE_ROW_NUMBER NUMBER(10,0) NOT NULL COMMENT 'Row number within the source file',
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'When the data was ingested',
    INGESTION_TYPE VARCHAR(50) NOT NULL COMMENT 'Type of ingestion: api, bulk, s3',
    COUNTY VARCHAR(100) COMMENT 'County name extracted from parcel data',
    CHUNK_INDEX NUMBER(10,0) DEFAULT 1 COMMENT 'Index for data chunking in bulk operations',
    PARCEL_COUNT NUMBER(10,0) DEFAULT 1 COMMENT 'Number of parcels in this record',
    RAW_DATA VARIANT NOT NULL COMMENT 'Complete JSON payload from API response'
) COMMENT = 'Raw parcel data from various ingestion sources (API, bulk files, etc.)';

-- Note: Snowflake automatically manages indexing and clustering for performance
-- Manual indexes are only available for Hybrid Tables (not regular tables)
-- Snowflake uses micro-partitions and automatic clustering instead

-- Grant permissions to TEDDY_PIPELINE_USER
GRANT INSERT, SELECT ON TABLE PARCEL_DATA_RAW TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON SCHEMA RAW TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON DATABASE TEDDY_DATA TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON WAREHOUSE TEDDY_INGESTION_WH TO USER TEDDY_PIPELINE_USER;

-- Verify table structure
DESC TABLE PARCEL_DATA_RAW;

-- Show sample of what the data should look like
SELECT 
    'Example API ingestion record' AS description,
    'parcel:coords:31.555555:-97.888888.json' AS file_name,
    1 AS file_row_number,
    CURRENT_TIMESTAMP() AS ingestion_timestamp,
    'api' AS ingestion_type,
    'CORYELL' AS county,
    1 AS chunk_index,
    1 AS parcel_count,
    PARSE_JSON('{
        "query": {"type": "coordinates", "latitude": 31.555555, "longitude": -97.888888},
        "result": {"properties": {"headline": "1005 Beechley Rd"}},
        "timestamp": "2025-08-07T15:24:14.592802",
        "source": "regrid_api"
    }') AS raw_data;

-- Test insert statement (comment out after testing)
/*
INSERT INTO PARCEL_DATA_RAW (
    FILE_NAME, 
    FILE_ROW_NUMBER, 
    INGESTION_TIMESTAMP, 
    INGESTION_TYPE, 
    COUNTY, 
    CHUNK_INDEX, 
    PARCEL_COUNT, 
    RAW_DATA
) VALUES (
    'test-parcel-api.json',
    1,
    CURRENT_TIMESTAMP(),
    'api',
    'TEST_COUNTY',
    1,
    1,
    PARSE_JSON('{"test": "data", "parcel_id": "12345"}')
);
*/

-- Verify the insert worked
SELECT COUNT(*) as total_records FROM PARCEL_DATA_RAW;

SHOW GRANTS TO USER TEDDY_PIPELINE_USER;

-- Display final table structure
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA, 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    ORDINAL_POSITION,
    COMMENT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'PARCEL_DATA_RAW' 
    AND TABLE_SCHEMA = 'RAW'
    AND TABLE_CATALOG = 'TEDDY_DATA'
ORDER BY ORDINAL_POSITION;