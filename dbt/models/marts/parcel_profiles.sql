{{
  config(
    materialized='table',
    schema='curated',
    unique_key='parcel_id',
    post_hook="ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(parcel_id, county_name, state, owner_name)"
  )
}}

WITH latest_parcel_data AS (
    SELECT 
        parcel_id,
        regrid_id,
        fips_code,
        state,
        county_name,
        address,
        city,
        zip_code,
        owner_name,
        owner_address,
        acres,
        square_feet,
        property_type,
        land_use_code,
        land_use_description,
        zoning,
        assessed_value,
        market_value,
        tax_amount,
        tax_year,
        geometry_json,
        year_built,
        building_count,
        improvement_value,
        land_value,
        assessed_value_per_acre,
        assessment_ratio,
        size_category,
        is_agricultural,
        ingestion_timestamp,
        processed_at,
        -- Rank by most recent ingestion to get latest data per parcel
        ROW_NUMBER() OVER (
            PARTITION BY parcel_id 
            ORDER BY ingestion_timestamp DESC, processed_at DESC
        ) as rn
    FROM {{ ref('stg_parcel_data_raw') }}
    WHERE data_quality_flag = 'valid'
),

deduplicated_parcels AS (
    SELECT 
        parcel_id,
        regrid_id,
        fips_code,
        state,
        county_name,
        address,
        city,
        zip_code,
        owner_name,
        owner_address,
        acres,
        square_feet,
        property_type,
        land_use_code,
        land_use_description,
        zoning,
        assessed_value,
        market_value,
        tax_amount,
        tax_year,
        geometry_json,
        year_built,
        building_count,
        improvement_value,
        land_value,
        assessed_value_per_acre,
        assessment_ratio,
        size_category,
        is_agricultural,
        ingestion_timestamp,
        processed_at
    FROM latest_parcel_data
    WHERE rn = 1
),

enriched_parcels AS (
    SELECT 
        -- Primary identifiers
        parcel_id,
        regrid_id,
        fips_code,
        
        -- Location information
        state,
        county_name,
        COALESCE(NULLIF(TRIM(address), ''), 'Address Not Available') as address,
        COALESCE(NULLIF(TRIM(city), ''), 'Unknown') as city,
        CASE 
            WHEN LENGTH(TRIM(zip_code)) = 5 AND zip_code REGEXP '^[0-9]{5}$' 
            THEN zip_code 
            ELSE NULL 
        END as zip_code,
        
        -- Property ownership
        COALESCE(NULLIF(TRIM(owner_name), ''), 'Owner Not Available') as owner_name,
        TRIM(owner_address) as owner_address,
        
        -- Property characteristics
        CASE 
            WHEN acres > 0 AND acres < 100000 THEN acres 
            ELSE NULL 
        END as acres,
        CASE 
            WHEN square_feet > 0 AND square_feet < 1000000000 THEN square_feet 
            ELSE NULL 
        END as square_feet,
        COALESCE(NULLIF(TRIM(property_type), ''), 'Unknown') as property_type,
        TRIM(land_use_code) as land_use_code,
        COALESCE(NULLIF(TRIM(land_use_description), ''), 'Unknown') as land_use_description,
        TRIM(zoning) as zoning,
        
        -- Financial information
        CASE 
            WHEN assessed_value > 0 AND assessed_value < 1000000000 THEN assessed_value 
            ELSE NULL 
        END as assessed_value,
        CASE 
            WHEN market_value > 0 AND market_value < 1000000000 THEN market_value 
            ELSE NULL 
        END as market_value,
        CASE 
            WHEN tax_amount >= 0 AND tax_amount < 10000000 THEN tax_amount 
            ELSE NULL 
        END as tax_amount,
        CASE 
            WHEN tax_year BETWEEN 1900 AND YEAR(CURRENT_DATE()) THEN tax_year 
            ELSE NULL 
        END as tax_year,
        
        -- Geometry - convert to Snowflake GEOGRAPHY type
        CASE 
            WHEN geometry_json IS NOT NULL 
            THEN TRY_TO_GEOGRAPHY(geometry_json::STRING)
            ELSE NULL 
        END as parcel_geom,
        
        -- Building information
        CASE 
            WHEN year_built BETWEEN 1800 AND YEAR(CURRENT_DATE()) THEN year_built 
            ELSE NULL 
        END as year_built,
        CASE 
            WHEN building_count >= 0 AND building_count < 1000 THEN building_count 
            ELSE NULL 
        END as building_count,
        CASE 
            WHEN improvement_value >= 0 AND improvement_value < 1000000000 THEN improvement_value 
            ELSE NULL 
        END as improvement_value,
        CASE 
            WHEN land_value >= 0 AND land_value < 1000000000 THEN land_value 
            ELSE NULL 
        END as land_value,
        
        -- Calculated metrics
        assessed_value_per_acre,
        assessment_ratio,
        size_category,
        is_agricultural,
        
        -- Enhanced categorizations
        CASE 
            WHEN LOWER(property_type) LIKE '%residential%' THEN 'Residential'
            WHEN LOWER(property_type) LIKE '%commercial%' THEN 'Commercial'
            WHEN LOWER(property_type) LIKE '%industrial%' THEN 'Industrial'
            WHEN LOWER(property_type) LIKE '%agricultural%' OR is_agricultural THEN 'Agricultural'
            WHEN LOWER(property_type) LIKE '%vacant%' THEN 'Vacant'
            ELSE 'Other'
        END as property_category,
        
        -- Market value categories
        CASE 
            WHEN assessed_value IS NULL THEN 'Unknown'
            WHEN assessed_value < 50000 THEN 'Low Value'
            WHEN assessed_value BETWEEN 50000 AND 200000 THEN 'Medium Value'
            WHEN assessed_value BETWEEN 200000 AND 500000 THEN 'High Value'
            ELSE 'Premium Value'
        END as value_category,
        
        -- Age categories
        CASE 
            WHEN year_built IS NULL THEN 'Unknown'
            WHEN year_built >= YEAR(CURRENT_DATE()) - 10 THEN 'New (0-10 years)'
            WHEN year_built >= YEAR(CURRENT_DATE()) - 30 THEN 'Modern (11-30 years)'
            WHEN year_built >= YEAR(CURRENT_DATE()) - 50 THEN 'Mature (31-50 years)'
            ELSE 'Historic (50+ years)'
        END as age_category,
        
        -- Investment potential score (simple heuristic)
        CASE 
            WHEN assessed_value IS NULL OR acres IS NULL THEN NULL
            WHEN is_agricultural AND acres > 10 AND assessed_value_per_acre < 10000 THEN 'High'
            WHEN property_category = 'Residential' AND assessed_value BETWEEN 100000 AND 300000 THEN 'Medium'
            WHEN property_category = 'Commercial' AND assessed_value > 200000 THEN 'High'
            ELSE 'Low'
        END as investment_potential,
        
        -- Data lineage
        ingestion_timestamp as source_ingestion_timestamp,
        processed_at as source_processed_timestamp,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
        
    FROM deduplicated_parcels
)

SELECT 
    -- Generate a hash for change detection
    {{ dbt_utils.generate_surrogate_key([
        'parcel_id', 'regrid_id', 'address', 'owner_name', 'acres', 
        'assessed_value', 'market_value', 'property_type'
    ]) }} as parcel_hash,
    
    *
    
FROM enriched_parcels

-- Add model documentation
{{ config(
    description="Curated parcel profiles table containing cleaned and enriched parcel data from Regrid API. This table serves as the primary source for parcel information in the Teddy platform.",
    
    columns={
        'parcel_id': 'Unique identifier for the parcel',
        'regrid_id': 'Regrid API unique identifier',
        'fips_code': 'Federal Information Processing Standards county code',
        'state': 'Two-letter state code',
        'county_name': 'County name',
        'address': 'Property address',
        'owner_name': 'Property owner name',
        'acres': 'Property size in acres',
        'assessed_value': 'Tax assessed value',
        'parcel_geom': 'Property boundary geometry',
        'is_agricultural': 'Boolean flag indicating agricultural use',
        'property_category': 'High-level property type categorization',
        'investment_potential': 'Investment potential score (High/Medium/Low)'
    }
) }}
