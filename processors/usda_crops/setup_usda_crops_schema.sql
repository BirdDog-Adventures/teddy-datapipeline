-- USDA Crop Yields Schema Setup for TEDDY_DATA (Dataset 1)
-- Stores county-level trailing yields / inventory from the USDA NASS QuickStats API,
-- one row per (parcel, commodity, reporting period). Surfaced read-only by Teddy as
-- "Land like yours in {county} averaged {yield}/acre."
--
-- Run once per environment:
--   snowsql -f processors/usda_crops/setup_usda_crops_schema.sql

USE SCHEMA TEDDY_DATA.RAW;

CREATE TABLE IF NOT EXISTS USDA_CROP_YIELDS (
    ID                  INTEGER AUTOINCREMENT PRIMARY KEY,
    PARCEL_ID           VARCHAR(100)   NOT NULL,
    COUNTY_FIPS         VARCHAR(5),                 -- 5-digit state+county FIPS
    STATE_CODE          VARCHAR(2),                 -- 2-letter postal abbreviation
    COMMODITY_DESC      VARCHAR(50)    NOT NULL,    -- CORN / SOYBEANS / WHEAT / HAY / CATTLE
    STATISTIC_CATEGORY  VARCHAR(50)    NOT NULL,    -- YIELD / INVENTORY
    SHORT_DESC          VARCHAR(255),               -- e.g. "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE"
    YEAR                INTEGER        NOT NULL,
    REFERENCE_PERIOD    VARCHAR(50),                -- e.g. "YEAR"
    VALUE               FLOAT,                      -- numeric value (suppressed values omitted at ingest)
    UNIT_DESC           VARCHAR(50),                -- e.g. "BU / ACRE", "HEAD"
    SOURCE_DESC         VARCHAR(50),                -- SURVEY / CENSUS
    DATA_SOURCE         VARCHAR(50)    NOT NULL DEFAULT 'usda_nass_quickstats',
    API_RESPONSE_JSON   VARIANT,
    LAST_PROCESSED_AT   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Lookups are always per parcel; most reads also filter/sort by commodity + year.
CREATE INDEX IF NOT EXISTS IDX_USDA_CROP_YIELDS_PARCEL
    ON USDA_CROP_YIELDS (PARCEL_ID);
CREATE INDEX IF NOT EXISTS IDX_USDA_CROP_YIELDS_PARCEL_COMMODITY
    ON USDA_CROP_YIELDS (PARCEL_ID, COMMODITY_DESC, YEAR);
