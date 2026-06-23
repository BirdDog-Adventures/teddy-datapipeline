-- Water Rights Schema Setup for TEDDY_DATA (Dataset 5 — region-gated)
-- Stores a parcel-level status row (always) plus per-right detail (when available), from
-- state water-rights sources (Colorado DWR first). Surfaced read-only by Teddy as
-- "This parcel holds water rights with a {priority date} priority date."
--
-- Run once per environment:
--   snowsql -f processors/water_rights/setup_water_rights_schema.sql

USE SCHEMA TEDDY_DATA.RAW;

-- Parcel-level status: one row per parcel. AVAILABLE=FALSE + REASON captures the gated /
-- not-found cases so Teddy gets a definitive answer without re-querying.
CREATE TABLE IF NOT EXISTS WATER_RIGHTS_STATUS (
    ID                          INTEGER AUTOINCREMENT PRIMARY KEY,
    PARCEL_ID                   VARCHAR(100) NOT NULL,
    STATE_CODE                  VARCHAR(2),
    AVAILABLE                   BOOLEAN NOT NULL DEFAULT FALSE,
    REASON                      VARCHAR(50),  -- ok | no_rights_found | state_not_supported | no_coordinates
    RIGHTS_COUNT                INTEGER DEFAULT 0,
    MOST_SENIOR_PRIORITY_DATE   DATE,
    MOST_SENIOR_STRUCTURE       VARCHAR(255),
    LAST_PROCESSED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Per-right detail: zero or more rows per parcel (only when AVAILABLE).
CREATE TABLE IF NOT EXISTS WATER_RIGHTS (
    ID                  INTEGER AUTOINCREMENT PRIMARY KEY,
    PARCEL_ID           VARCHAR(100) NOT NULL,
    STATE_CODE          VARCHAR(2),
    STRUCTURE_NAME      VARCHAR(255),
    STRUCTURE_TYPE      VARCHAR(100),
    WATER_SOURCE        VARCHAR(255),
    APPROPRIATION_DATE  DATE,          -- priority date (seniority)
    ADJUDICATION_DATE   DATE,
    ADMIN_NUMBER        VARCHAR(50),   -- administrative number (relative seniority)
    PRIORITY_NUMBER     VARCHAR(50),
    NET_ABSOLUTE        FLOAT,         -- decreed absolute amount
    NET_CONDITIONAL     FLOAT,
    DECREED_USES        VARCHAR(100),
    LATITUDE            FLOAT,         -- point of diversion
    LONGITUDE           FLOAT,
    WDID                VARCHAR(50),
    DIVISION            VARCHAR(20),
    WATER_DISTRICT      VARCHAR(20),
    COUNTY              VARCHAR(100),
    DATA_SOURCE         VARCHAR(50) NOT NULL DEFAULT 'co_dwr_cdss',
    API_RESPONSE_JSON   VARIANT,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX IF NOT EXISTS IDX_WATER_RIGHTS_STATUS_PARCEL ON WATER_RIGHTS_STATUS (PARCEL_ID);
CREATE INDEX IF NOT EXISTS IDX_WATER_RIGHTS_PARCEL ON WATER_RIGHTS (PARCEL_ID);
