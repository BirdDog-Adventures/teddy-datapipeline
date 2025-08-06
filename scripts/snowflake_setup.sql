-- Teddy Data Pipeline - Snowflake Setup Script
-- This script sets up the Snowflake infrastructure for parcel data ingestion

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS TEDDY_DATA;
USE DATABASE TEDDY_DATA;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS MONITORING;

-- Create storage integration for S3
CREATE OR REPLACE STORAGE INTEGRATION TEDDY_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://teddy-data-lake-prod/raw/', 's3://teddy-data-lake-prod/processed/');

-- Show integration details (run this to get the external ID for AWS role trust policy)
DESC STORAGE INTEGRATION TEDDY_S3_INTEGRATION;

-- Create external stage for parcel data
CREATE OR REPLACE STAGE TEDDY_DATA.RAW.S3_PARCEL_STAGE
  STORAGE_INTEGRATION = TEDDY_S3_INTEGRATION
  URL = 's3://teddy-data-lake-prod/raw/parcel/'
  FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    ALLOW_DUPLICATE = FALSE
    NULL_IF = ('\\N', 'NULL', 'null', '')
  );

-- Test the stage
LIST @TEDDY_DATA.RAW.S3_PARCEL_STAGE;

-- Create raw parcel data table
CREATE OR REPLACE TABLE TEDDY_DATA.RAW.PARCEL_DATA_RAW (
    FILE_NAME VARCHAR(500),
    FILE_ROW_NUMBER NUMBER,
    RAW_DATA VARIANT,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    INGESTION_TYPE VARCHAR(50),
    COUNTY VARCHAR(100),
    CHUNK_INDEX NUMBER,
    PARCEL_COUNT NUMBER
);

-- Create Snowpipe for automatic ingestion
CREATE OR REPLACE PIPE TEDDY_DATA.RAW.PARCEL_DATA_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO TEDDY_DATA.RAW.PARCEL_DATA_RAW (
    FILE_NAME,
    FILE_ROW_NUMBER,
    RAW_DATA,
    INGESTION_TYPE,
    COUNTY,
    CHUNK_INDEX,
    PARCEL_COUNT
  )
  FROM (
    SELECT 
      METADATA$FILENAME,
      METADATA$FILE_ROW_NUMBER,
      $1,
      $1:ingestion_type::STRING,
      $1:county::STRING,
      $1:chunk_index::NUMBER,
      $1:parcel_count::NUMBER
    FROM @TEDDY_DATA.RAW.S3_PARCEL_STAGE
  )
  FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
  );

-- Show Snowpipe details (use this notification channel for SNS)
SHOW PIPES IN DATABASE TEDDY_DATA;

-- Create curated parcel profile table with comprehensive schema
CREATE OR REPLACE TABLE TEDDY_DATA.CURATED.PARCEL_PROFILES (
    PARCEL_ID VARCHAR(100) PRIMARY KEY,                    -- 1. Unique parcel identifier
    COUNTY_ID VARCHAR(50),                                 -- 2. County identifier code
    STATE_CODE VARCHAR(10),                                -- 3. State code (e.g., TX, CA)
    ADDRESS VARCHAR(500),                                  -- 4. Property address
    CITY VARCHAR(100),                                     -- 5. City name
    ZIP_CODE VARCHAR(20),                                  -- 6. ZIP code
    ACRES NUMBER(12,4),                                    -- 7. Total acres
    OWNER_NAME VARCHAR(500),                               -- 8. Property owner name
    OWNERSHIP_TYPE VARCHAR(100),                           -- 9. Type of ownership
    ACQUISITION_DATE DATE,                                 -- 10. Date property was acquired
    PARCEL_GEOJSON VARIANT,                               -- 11. GeoJSON geometry data
    PARCEL_WKT VARCHAR(16777216),                         -- 12. Well-Known Text geometry
    GEOID VARCHAR(50),                                    -- 13. Geographic identifier
    LEGAL_DESC VARCHAR(16777216),                         -- 14. Legal description
    LAND_VALUE NUMBER(15,2),                              -- 15. Assessed land value
    TOTAL_VALUE NUMBER(15,2),                             -- 16. Total assessed value
    AGVAL NUMBER(15,2),                                   -- 17. Agricultural value
    SALEPRICE NUMBER(15,2),                               -- 18. Last sale price
    SALEDATE DATE,                                        -- 19. Last sale date
    TAXAMT NUMBER(12,2),                                  -- 20. Annual tax amount
    TAX_YEAR VARCHAR(10),                                 -- 21. Tax year
    DEEDED_ACRES NUMBER(12,4),                            -- 22. Deeded acres
    GISACRE NUMBER(12,4),                                 -- 23. GIS calculated acres
    LONGITUDE VARCHAR(50),                                -- 24. Longitude coordinate
    LATITUDE VARCHAR(50),                                 -- 25. Latitude coordinate
    
    -- System fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create comprehensive soil profile table
CREATE OR REPLACE TABLE TEDDY_DATA.CURATED.SOIL_PROFILES (
    PARCEL_ID VARCHAR(100),                               -- 1. Parcel identifier (FK)
    MUKEY VARCHAR(50),                                    -- 2. Map unit key
    MAP_UNIT_SYMBOL VARCHAR(100),                         -- 3. Map unit symbol
    COMPONENT_KEY VARCHAR(100),                           -- 4. Component key identifier
    SOIL_SERIES VARCHAR(200),                             -- 5. Soil series name
    DISTANCE_MILES VARCHAR(50),                           -- 6. Distance in miles
    CONFIDENCE_SCORE NUMBER(5,2),                         -- 7. Confidence score (0-100)
    MATCH_QUALITY VARCHAR(50),                            -- 8. Match quality indicator
    SOIL_TYPE VARCHAR(200),                               -- 9. Soil type classification
    FERTILITY_CLASS VARCHAR(100),                         -- 10. Fertility class
    ORGANIC_MATTER_PCT FLOAT,                             -- 11. Organic matter percentage
    PH_LEVEL FLOAT,                                       -- 12. pH level
    CATION_EXCHANGE_CAPACITY FLOAT,                       -- 13. Cation exchange capacity
    DRAINAGE_CLASS VARCHAR(100),                          -- 14. Drainage class
    HYDROLOGIC_GROUP VARCHAR(10),                         -- 15. Hydrologic group
    SLOPE_PERCENT FLOAT,                                  -- 16. Slope percentage
    AVAILABLE_WATER_CAPACITY FLOAT,                       -- 17. Available water capacity
    NITROGEN_PPM NUMBER(10,2),                            -- 18. Nitrogen parts per million
    PHOSPHORUS_PPM NUMBER(10,2),                          -- 19. Phosphorus parts per million
    POTASSIUM_PPM NUMBER(10,2),                           -- 20. Potassium parts per million
    AGRICULTURAL_CAPABILITY VARCHAR(100),                 -- 21. Agricultural capability class
    TAXONOMIC_CLASS VARCHAR(200),                         -- 22. Taxonomic classification
    COMPONENT_PERCENTAGE NUMBER(5,2),                     -- 23. Component percentage
    SAMPLING_DEPTH_CM NUMBER(8,2),                        -- 24. Sampling depth in centimeters
    LAST_UPDATED TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), -- 25. Last updated timestamp
    
    -- System fields
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key relationship
    FOREIGN KEY (PARCEL_ID) REFERENCES TEDDY_DATA.CURATED.PARCEL_PROFILES(PARCEL_ID)
);

-- Create comprehensive climate data table
CREATE OR REPLACE TABLE TEDDY_DATA.CURATED.CLIMATE_DATA (
    CLIMATE_ID NUMBER AUTOINCREMENT PRIMARY KEY,          -- 1. Unique climate record identifier
    PARCEL_ID VARCHAR(100),                               -- 2. Parcel identifier (FK)
    COUNTY_ID VARCHAR(50),                                -- 3. County identifier
    STATE_CODE VARCHAR(10),                               -- 4. State code
    DATA_YEAR NUMBER(4),                                  -- 5. Data year
    ANNUAL_PRECIPITATION_INCHES NUMBER(8,2),              -- 6. Annual precipitation in inches
    ANNUAL_PRECIPITATION_MM NUMBER(8,2),                  -- 7. Annual precipitation in millimeters
    ANNUAL_SNOWFALL_INCHES NUMBER(8,2),                   -- 8. Annual snowfall in inches
    GROWING_DEGREE_DAYS NUMBER(8,0),                      -- 9. Growing degree days
    AVG_TEMPERATURE_F NUMBER(6,2),                        -- 10. Average temperature Fahrenheit
    MAX_TEMPERATURE_F NUMBER(6,2),                        -- 11. Maximum temperature Fahrenheit
    MIN_TEMPERATURE_F NUMBER(6,2),                        -- 12. Minimum temperature Fahrenheit
    CLIMATE_CLASSIFICATION VARCHAR(100),                  -- 13. Climate classification (Köppen, etc.)
    WEATHER_STATION_ID VARCHAR(50),                       -- 14. Weather station identifier
    WEATHER_STATION_NAME VARCHAR(200),                    -- 15. Weather station name
    STATION_DISTANCE_MILES NUMBER(8,2),                   -- 16. Distance to weather station in miles
    DATA_PERIOD VARCHAR(50),                              -- 17. Data period description
    IS_MULTI_YEAR_AVERAGE BOOLEAN DEFAULT FALSE,          -- 18. Multi-year average flag
    YEARS_OF_DATA NUMBER(4),                              -- 19. Number of years of data
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),  -- 20. Created timestamp with timezone
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),  -- 21. Updated timestamp with timezone
    
    -- Foreign key relationship
    FOREIGN KEY (PARCEL_ID) REFERENCES TEDDY_DATA.CURATED.PARCEL_PROFILES(PARCEL_ID)
);

-- Create data quality monitoring views
CREATE OR REPLACE VIEW TEDDY_DATA.MONITORING.PARCEL_DATA_QUALITY AS
SELECT 
    DATE(INGESTION_TIMESTAMP) as ingestion_date,
    INGESTION_TYPE,
    COUNTY,
    COUNT(*) as total_records,
    COUNT(CASE WHEN RAW_DATA:parcels IS NOT NULL THEN 1 END) as valid_parcel_records,
    COUNT(CASE WHEN RAW_DATA:error IS NOT NULL THEN 1 END) as error_records,
    AVG(PARCEL_COUNT) as avg_parcels_per_file,
    MIN(INGESTION_TIMESTAMP) as first_ingestion,
    MAX(INGESTION_TIMESTAMP) as last_ingestion
FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW
WHERE INGESTION_TIMESTAMP >= CURRENT_DATE - 7
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- Create data quality alerts table
CREATE OR REPLACE TABLE TEDDY_DATA.MONITORING.DATA_QUALITY_ALERTS (
    ALERT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    ALERT_TYPE VARCHAR(100),
    ALERT_MESSAGE VARCHAR(1000),
    ALERT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SEVERITY VARCHAR(20),
    RESOLVED BOOLEAN DEFAULT FALSE
);

-- Create warehouse for data processing
CREATE OR REPLACE WAREHOUSE TEDDY_INGESTION_WH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300  -- 5 minutes
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Create warehouse for monitoring
CREATE OR REPLACE WAREHOUSE TEDDY_MONITORING_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Create task for data quality monitoring
CREATE OR REPLACE TASK TEDDY_DATA.MONITORING.PARCEL_QUALITY_CHECK
  WAREHOUSE = 'TEDDY_MONITORING_WH'
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
AS
  INSERT INTO TEDDY_DATA.MONITORING.DATA_QUALITY_ALERTS (
    alert_type,
    alert_message,
    alert_timestamp,
    severity
  )
  SELECT 
    'PARCEL_DATA_QUALITY',
    'High error rate detected: ' || error_rate || '% errors in ' || county || ' data',
    CURRENT_TIMESTAMP(),
    CASE 
      WHEN error_rate > 10 THEN 'CRITICAL'
      WHEN error_rate > 5 THEN 'WARNING'
      ELSE 'INFO'
    END
  FROM (
    SELECT 
      COUNTY,
      (error_records::FLOAT / total_records::FLOAT) * 100 as error_rate
    FROM TEDDY_DATA.MONITORING.PARCEL_DATA_QUALITY
    WHERE ingestion_date = CURRENT_DATE
      AND total_records > 0
  )
  WHERE error_rate > 5;

-- Start the monitoring task
ALTER TASK TEDDY_DATA.MONITORING.PARCEL_QUALITY_CHECK RESUME;

-- Create stored procedure for parcel data transformation
CREATE OR REPLACE PROCEDURE TEDDY_DATA.CURATED.TRANSFORM_PARCEL_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Transform raw parcel data to curated format with comprehensive mapping
    MERGE INTO TEDDY_DATA.CURATED.PARCEL_PROFILES p
    USING (
        SELECT DISTINCT
            -- Core identifiers
            raw_data:parcels[0]:parcel_id::STRING as parcel_id,
            raw_data:parcels[0]:county_id::STRING as county_id,
            raw_data:parcels[0]:state_code::STRING as state_code,
            
            -- Address information
            raw_data:parcels[0]:address::STRING as address,
            raw_data:parcels[0]:city::STRING as city,
            raw_data:parcels[0]:zip_code::STRING as zip_code,
            
            -- Property details
            raw_data:parcels[0]:acres::NUMBER as acres,
            raw_data:parcels[0]:owner_name::STRING as owner_name,
            raw_data:parcels[0]:ownership_type::STRING as ownership_type,
            raw_data:parcels[0]:acquisition_date::DATE as acquisition_date,
            
            -- Geometry data
            raw_data:parcels[0]:parcel_geojson as parcel_geojson,
            raw_data:parcels[0]:parcel_wkt::STRING as parcel_wkt,
            raw_data:parcels[0]:geoid::STRING as geoid,
            raw_data:parcels[0]:legal_desc::STRING as legal_desc,
            
            -- Financial data
            raw_data:parcels[0]:land_value::NUMBER as land_value,
            raw_data:parcels[0]:total_value::NUMBER as total_value,
            raw_data:parcels[0]:agval::NUMBER as agval,
            raw_data:parcels[0]:saleprice::NUMBER as saleprice,
            raw_data:parcels[0]:saledate::DATE as saledate,
            raw_data:parcels[0]:taxamt::NUMBER as taxamt,
            raw_data:parcels[0]:tax_year::STRING as tax_year,
            
            -- Acreage details
            raw_data:parcels[0]:deeded_acres::NUMBER as deeded_acres,
            raw_data:parcels[0]:gisacre::NUMBER as gisacre,
            
            -- Coordinates
            raw_data:parcels[0]:longitude::STRING as longitude,
            raw_data:parcels[0]:latitude::STRING as latitude
            
        FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW
        WHERE raw_data:parcels IS NOT NULL
          AND ARRAY_SIZE(raw_data:parcels) > 0
          AND ingestion_timestamp >= CURRENT_DATE - 1
    ) src ON p.parcel_id = src.parcel_id
    WHEN MATCHED THEN UPDATE SET
        p.county_id = src.county_id,
        p.state_code = src.state_code,
        p.address = src.address,
        p.city = src.city,
        p.zip_code = src.zip_code,
        p.acres = src.acres,
        p.owner_name = src.owner_name,
        p.ownership_type = src.ownership_type,
        p.acquisition_date = src.acquisition_date,
        p.parcel_geojson = src.parcel_geojson,
        p.parcel_wkt = src.parcel_wkt,
        p.geoid = src.geoid,
        p.legal_desc = src.legal_desc,
        p.land_value = src.land_value,
        p.total_value = src.total_value,
        p.agval = src.agval,
        p.saleprice = src.saleprice,
        p.saledate = src.saledate,
        p.taxamt = src.taxamt,
        p.tax_year = src.tax_year,
        p.deeded_acres = src.deeded_acres,
        p.gisacre = src.gisacre,
        p.longitude = src.longitude,
        p.latitude = src.latitude,
        p.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        parcel_id, county_id, state_code, address, city, zip_code,
        acres, owner_name, ownership_type, acquisition_date,
        parcel_geojson, parcel_wkt, geoid, legal_desc,
        land_value, total_value, agval, saleprice, saledate,
        taxamt, tax_year, deeded_acres, gisacre, longitude, latitude
    ) VALUES (
        src.parcel_id, src.county_id, src.state_code, src.address, src.city, src.zip_code,
        src.acres, src.owner_name, src.ownership_type, src.acquisition_date,
        src.parcel_geojson, src.parcel_wkt, src.geoid, src.legal_desc,
        src.land_value, src.total_value, src.agval, src.saleprice, src.saledate,
        src.taxamt, src.tax_year, src.deeded_acres, src.gisacre, src.longitude, src.latitude
    );
    
    RETURN 'Parcel data transformation completed successfully';
END;
$$;

-- Create task for daily data transformation
CREATE OR REPLACE TASK TEDDY_DATA.CURATED.DAILY_PARCEL_TRANSFORM
  WAREHOUSE = 'TEDDY_INGESTION_WH'
  SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- 8 AM daily
AS
  CALL TEDDY_DATA.CURATED.TRANSFORM_PARCEL_DATA();

-- Start the transformation task
ALTER TASK TEDDY_DATA.CURATED.DAILY_PARCEL_TRANSFORM RESUME;

-- Grant permissions to pipeline user
GRANT USAGE ON DATABASE TEDDY_DATA TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TEDDY_DATA.RAW TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TEDDY_DATA.CURATED TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA TEDDY_DATA.MONITORING TO ROLE SYSADMIN;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA TEDDY_DATA.RAW TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA TEDDY_DATA.CURATED TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA TEDDY_DATA.MONITORING TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE TEDDY_INGESTION_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE TEDDY_MONITORING_WH TO ROLE SYSADMIN;

-- Show pipeline status
SELECT 'Setup completed successfully' as status;

-- Useful queries for monitoring
/*
-- Check Snowpipe status
SHOW PIPES IN DATABASE TEDDY_DATA;

-- Check Snowpipe ingestion history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START=>DATEADD('hours', -24, CURRENT_TIMESTAMP()),
    DATE_RANGE_END=>CURRENT_TIMESTAMP(),
    PIPE_NAME=>'TEDDY_DATA.RAW.PARCEL_DATA_PIPE'
));

-- Check copy history for errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    DATE_RANGE_START=>DATEADD('hours', -24, CURRENT_TIMESTAMP()),
    DATE_RANGE_END=>CURRENT_TIMESTAMP()
))
WHERE TABLE_NAME = 'PARCEL_DATA_RAW';

-- Check data quality
SELECT * FROM TEDDY_DATA.MONITORING.PARCEL_DATA_QUALITY;

-- Check recent parcel data
SELECT 
    county,
    ingestion_type,
    COUNT(*) as record_count,
    MAX(ingestion_timestamp) as latest_ingestion
FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW
WHERE ingestion_timestamp >= CURRENT_DATE - 1
GROUP BY 1, 2
ORDER BY 4 DESC;
*/
