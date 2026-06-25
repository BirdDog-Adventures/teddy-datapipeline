-- Dataset-2 (assessment) verification — run in a Snowflake worksheet (role
-- TEDDY_PIPELINE_ROLE). The stg_parcel_data_raw COALESCE fix captures the value
-- regardless of key name; these queries confirm WHICH key actually fires and
-- that coverage went non-zero after `dbt run -s stg_parcel_data_raw+`.

-- 1) Coverage BEFORE/AFTER — pct of curated profiles with financial values.
SELECT
  COUNT(*)                                                        AS total,
  ROUND(100.0 * COUNT(assessed_value)    / NULLIF(COUNT(*), 0), 1) AS pct_assessed_value,
  ROUND(100.0 * COUNT(land_value)        / NULLIF(COUNT(*), 0), 1) AS pct_land_value,
  ROUND(100.0 * COUNT(improvement_value) / NULLIF(COUNT(*), 0), 1) AS pct_improvement_value,
  ROUND(100.0 * COUNT(market_value)      / NULLIF(COUNT(*), 0), 1) AS pct_market_value,
  ROUND(100.0 * COUNT(tax_amount)        / NULLIF(COUNT(*), 0), 1) AS pct_tax_amount,
  ROUND(100.0 * COUNT(tax_year)          / NULLIF(COUNT(*), 0), 1) AS pct_tax_year
FROM TEDDY_DATA.CURATED.PARCEL_PROFILES;

-- 2) Which key actually holds the value? (confirm native vs generic vs :fields)
--    Replace the FROM with the raw source feeding stg_parcel_data_raw if needed.
SELECT
  OBJECT_KEYS(f.value)            AS top_level_keys,
  OBJECT_KEYS(f.value:fields)     AS field_keys,
  f.value:assessed_value          AS k_assessed_value,
  f.value:parval                  AS k_parval,
  f.value:fields:parval           AS k_fields_parval,
  f.value:land_value              AS k_land_value,
  f.value:landval                 AS k_landval
FROM TEDDY_DATA.RAW.PARCEL_DATA_RAW,  -- adjust to the real raw source
     LATERAL FLATTEN(input => raw_data:parcels) f
LIMIT 5;

-- 3) If coverage is still ~0 after the fix AND no key holds a value, the Regrid
--    subscription tier likely doesn't include standardized tax/assessment fields
--    (runbook Phase 8d) — confirm with the Regrid account; no SQL fix helps then.
