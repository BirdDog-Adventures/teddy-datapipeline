{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH raw_parcel_data AS (
    SELECT 
        file_name,
        file_row_number,
        raw_data,
        ingestion_timestamp,
        ingestion_type,
        county,
        chunk_index,
        parcel_count
    FROM {{ source('raw', 'parcel_data_raw') }}
    WHERE ingestion_timestamp >= current_date - {{ var('parcel_data_retention_days') }}
),

flattened_parcels AS (
    SELECT 
        file_name,
        file_row_number,
        ingestion_timestamp,
        ingestion_type,
        county,
        chunk_index,
        parcel_count,
        -- Extract parcel array and flatten
        f.value as parcel_json,
        f.index as parcel_index_in_chunk
    FROM raw_parcel_data,
    LATERAL FLATTEN(input => raw_data:parcels) f
    WHERE raw_data:parcels IS NOT NULL
      AND ARRAY_SIZE(raw_data:parcels) > 0
),

parsed_parcels AS (
    SELECT 
        -- File metadata
        file_name,
        file_row_number,
        ingestion_timestamp,
        ingestion_type,
        county,
        chunk_index,
        parcel_count,
        parcel_index_in_chunk,
        
        -- Parcel identification
        parcel_json:id::STRING as parcel_id,
        parcel_json:regrid_id::STRING as regrid_id,
        parcel_json:fips_code::STRING as fips_code,
        
        -- Location information
        parcel_json:state::STRING as state,
        parcel_json:county::STRING as parcel_county,
        parcel_json:address::STRING as address,
        parcel_json:city::STRING as city,
        parcel_json:zip_code::STRING as zip_code,
        
        -- Property details
        parcel_json:owner_name::STRING as owner_name,
        parcel_json:owner_address::STRING as owner_address,
        parcel_json:acres::FLOAT as acres,
        parcel_json:square_feet::NUMBER as square_feet,
        parcel_json:property_type::STRING as property_type,
        parcel_json:land_use_code::STRING as land_use_code,
        parcel_json:land_use_description::STRING as land_use_description,
        parcel_json:zoning::STRING as zoning,
        
        -- Financial information
        parcel_json:assessed_value::NUMBER(12,2) as assessed_value,
        parcel_json:market_value::NUMBER(12,2) as market_value,
        parcel_json:tax_amount::NUMBER(10,2) as tax_amount,
        parcel_json:tax_year::NUMBER(4) as tax_year,
        
        -- Geometry (stored as GeoJSON)
        parcel_json:geometry as geometry_json,
        
        -- Additional attributes
        parcel_json:year_built::NUMBER(4) as year_built,
        parcel_json:building_count::NUMBER as building_count,
        parcel_json:improvement_value::NUMBER(12,2) as improvement_value,
        parcel_json:land_value::NUMBER(12,2) as land_value,
        
        -- Data quality indicators
        CASE 
            WHEN parcel_json:id IS NULL THEN 'missing_id'
            WHEN parcel_json:acres IS NULL OR parcel_json:acres <= 0 THEN 'invalid_acres'
            WHEN parcel_json:geometry IS NULL THEN 'missing_geometry'
            ELSE 'valid'
        END as data_quality_flag,
        
        -- Create unique record identifier
        {{ dbt_utils.generate_surrogate_key(['file_name', 'file_row_number', 'parcel_index_in_chunk']) }} as record_id
        
    FROM flattened_parcels
)

SELECT 
    record_id,
    file_name,
    file_row_number,
    ingestion_timestamp,
    ingestion_type,
    county,
    chunk_index,
    parcel_count,
    parcel_index_in_chunk,
    
    -- Parcel identification
    parcel_id,
    regrid_id,
    fips_code,
    
    -- Location information
    UPPER(TRIM(state)) as state,
    UPPER(TRIM(COALESCE(parcel_county, county))) as county_name,
    TRIM(address) as address,
    TRIM(city) as city,
    TRIM(zip_code) as zip_code,
    
    -- Property details
    TRIM(owner_name) as owner_name,
    TRIM(owner_address) as owner_address,
    acres,
    square_feet,
    TRIM(property_type) as property_type,
    TRIM(land_use_code) as land_use_code,
    TRIM(land_use_description) as land_use_description,
    TRIM(zoning) as zoning,
    
    -- Financial information
    assessed_value,
    market_value,
    tax_amount,
    tax_year,
    
    -- Geometry
    geometry_json,
    
    -- Additional attributes
    year_built,
    building_count,
    improvement_value,
    land_value,
    
    -- Data quality
    data_quality_flag,
    
    -- Calculated fields
    CASE 
        WHEN acres > 0 THEN assessed_value / acres 
        ELSE NULL 
    END as assessed_value_per_acre,
    
    CASE 
        WHEN market_value > 0 AND assessed_value > 0 
        THEN assessed_value / market_value 
        ELSE NULL 
    END as assessment_ratio,
    
    -- Categorize parcel size
    CASE 
        WHEN acres IS NULL THEN 'unknown'
        WHEN acres < 1 THEN 'small'
        WHEN acres BETWEEN 1 AND 10 THEN 'medium'
        WHEN acres BETWEEN 10 AND 100 THEN 'large'
        ELSE 'very_large'
    END as size_category,
    
    -- Agricultural classification based on land use
    CASE 
        WHEN LOWER(land_use_description) LIKE '%farm%' 
          OR LOWER(land_use_description) LIKE '%agriculture%'
          OR LOWER(land_use_description) LIKE '%crop%'
          OR LOWER(land_use_description) LIKE '%pasture%'
          OR LOWER(land_use_description) LIKE '%ranch%'
        THEN TRUE
        ELSE FALSE
    END as is_agricultural,
    
    -- Current timestamp for processing
    CURRENT_TIMESTAMP() as processed_at

FROM parsed_parcels
WHERE data_quality_flag != 'missing_id'  -- Filter out records without parcel ID

-- Add data quality tests as comments for documentation
-- Tests to be implemented:
-- 1. Unique parcel_id per ingestion batch
-- 2. Valid state codes (2 characters)
-- 3. Reasonable acres values (> 0 and < 100000)
-- 4. Valid geometry JSON structure
-- 5. Assessed value > 0 for most parcels
