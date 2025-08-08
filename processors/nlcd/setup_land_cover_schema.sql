-- NLCD Land Cover Schema Setup for TEDDY_DATA Database
-- This script creates the NLCD tables in the RAW schema
-- Run this script to set up the database structure for NLCD integration

-- Use the RAW schema (assuming it already exists)
USE SCHEMA TEDDY_DATA.RAW;

-- Create LAND_COVER_CLASSES lookup table
CREATE OR REPLACE TABLE LAND_COVER_CLASSES (
    NLCD_CODE INTEGER PRIMARY KEY,
    CLASS_NAME VARCHAR(100) NOT NULL,
    CLASS_CATEGORY VARCHAR(50) NOT NULL,
    CLASS_COLOR VARCHAR(10) NOT NULL,
    IS_AGRICULTURAL BOOLEAN NOT NULL DEFAULT FALSE,
    IS_NATURAL BOOLEAN NOT NULL DEFAULT FALSE,
    DESCRIPTION TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert NLCD land cover class definitions
INSERT INTO LAND_COVER_CLASSES (NLCD_CODE, CLASS_NAME, CLASS_CATEGORY, CLASS_COLOR, IS_AGRICULTURAL, IS_NATURAL, DESCRIPTION) VALUES
(11, 'Open Water', 'Water', '#476BA0', FALSE, TRUE, 'Areas of open water, generally with less than 25% cover of vegetation or soil'),
(12, 'Perennial Ice/Snow', 'Ice/Snow', '#D1DDF9', FALSE, TRUE, 'Areas characterized by a perennial cover of ice and/or snow'),
(21, 'Developed, Open Space', 'Developed', '#DECACA', FALSE, FALSE, 'Areas with a mixture of some constructed materials, but mostly vegetation in the form of lawn grasses'),
(22, 'Developed, Low Intensity', 'Developed', '#D99482', FALSE, FALSE, 'Areas with a mixture of constructed materials and vegetation. Impervious surfaces account for 20% to 49% percent of total cover'),
(23, 'Developed, Medium Intensity', 'Developed', '#EB0000', FALSE, FALSE, 'Areas with a mixture of constructed materials and vegetation. Impervious surfaces account for 50% to 79% of the total cover'),
(24, 'Developed, High Intensity', 'Developed', '#AB0000', FALSE, FALSE, 'Highly developed areas where people reside or work in high numbers. Impervious surfaces account for 80% to 100% of the total cover'),
(31, 'Barren Land (Rock/Sand/Clay)', 'Barren', '#B3AC9F', FALSE, TRUE, 'Areas of bedrock, desert pavement, scarps, talus, slides, volcanic material, glacial debris, sand dunes, strip mines, gravel pits and other accumulations of earthen material'),
(41, 'Deciduous Forest', 'Forest', '#68AB5F', FALSE, TRUE, 'Areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. More than 75% of the tree species shed foliage simultaneously in response to seasonal change'),
(42, 'Evergreen Forest', 'Forest', '#1C5F2C', FALSE, TRUE, 'Areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. More than 75% of the tree species maintain their leaves all year'),
(43, 'Mixed Forest', 'Forest', '#B5C58F', FALSE, TRUE, 'Areas dominated by trees generally greater than 5 meters tall, and greater than 20% of total vegetation cover. Neither deciduous nor evergreen species are greater than 75% of total tree cover'),
(51, 'Dwarf Scrub', 'Shrubland', '#AF963C', FALSE, TRUE, 'Alaska only areas dominated by shrubs less than 20 centimeters tall with shrub canopy typically greater than 20% of total vegetation'),
(52, 'Shrub/Scrub', 'Shrubland', '#CCB879', FALSE, TRUE, 'Areas dominated by shrubs; less than 5 meters tall with shrub canopy typically greater than 20% of total vegetation'),
(71, 'Grassland/Herbaceous', 'Herbaceous', '#DFDFC2', FALSE, TRUE, 'Areas dominated by grammanoid or herbaceous vegetation, generally greater than 80% of total vegetation'),
(72, 'Sedge/Herbaceous', 'Herbaceous', '#D1D182', FALSE, TRUE, 'Alaska only areas dominated by sedges and forbs, generally greater than 80% of total vegetation'),
(73, 'Lichens', 'Herbaceous', '#A3CC51', FALSE, TRUE, 'Alaska only areas dominated by fruticose or foliose lichens generally greater than 80% of total vegetation'),
(74, 'Moss', 'Herbaceous', '#82BA9E', FALSE, TRUE, 'Alaska only areas dominated by mosses, generally greater than 80% of total vegetation'),
(81, 'Pasture/Hay', 'Planted/Cultivated', '#DCD939', TRUE, FALSE, 'Areas of grasses, legumes, or grass-legume mixtures planted for livestock grazing or the production of seed or hay crops'),
(82, 'Cultivated Crops', 'Planted/Cultivated', '#AB6C28', TRUE, FALSE, 'Areas used for the production of annual crops, such as corn, soybeans, vegetables, tobacco, and cotton'),
(90, 'Woody Wetlands', 'Wetlands', '#B8D9EB', FALSE, TRUE, 'Areas where forest or shrubland vegetation accounts for greater than 20% of vegetative cover and the soil or substrate is periodically saturated with or covered with water'),
(95, 'Emergent Herbaceous Wetlands', 'Wetlands', '#6C9FB8', FALSE, TRUE, 'Areas where perennial herbaceous vegetation accounts for greater than 80% of vegetative cover and the soil or substrate is periodically saturated with or covered with water');

-- Create LAND_COVER main data table
CREATE OR REPLACE TABLE LAND_COVER (
    ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PARCEL_ID VARCHAR(100) NOT NULL,
    NLCD_CODE INTEGER NOT NULL,
    NLCD_YEAR INTEGER NOT NULL,
    COVERAGE_PERCENTAGE DECIMAL(5,2) NOT NULL,
    COVERAGE_AREA_SQFT DECIMAL(15,2),
    IS_DOMINANT_CLASS BOOLEAN NOT NULL DEFAULT FALSE,
    PROCESSING_METHOD VARCHAR(50) NOT NULL, -- 'polygon', 'point_sample', 'wfs', 'wms'
    DATA_SOURCE VARCHAR(50) NOT NULL DEFAULT 'nlcd_api',
    CONFIDENCE_SCORE DECIMAL(3,2) DEFAULT 1.0,
    API_RESPONSE_JSON VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key constraint
    CONSTRAINT FK_LAND_COVER_CLASS FOREIGN KEY (NLCD_CODE) REFERENCES LAND_COVER_CLASSES(NLCD_CODE)
);

-- Create LAND_COVER_SUMMARY for aggregated data per parcel
CREATE OR REPLACE TABLE LAND_COVER_SUMMARY (
    PARCEL_ID VARCHAR(100) PRIMARY KEY,
    NLCD_YEAR INTEGER NOT NULL,
    DOMINANT_NLCD_CODE INTEGER NOT NULL,
    DOMINANT_CLASS_NAME VARCHAR(100) NOT NULL,
    DOMINANT_CLASS_PERCENTAGE DECIMAL(5,2) NOT NULL,
    TOTAL_CLASSES_COUNT INTEGER NOT NULL,
    IS_AGRICULTURAL BOOLEAN NOT NULL,
    IS_NATURAL BOOLEAN NOT NULL,
    IS_DEVELOPED BOOLEAN NOT NULL,
    AGRICULTURAL_PERCENTAGE DECIMAL(5,2) DEFAULT 0.0,
    NATURAL_PERCENTAGE DECIMAL(5,2) DEFAULT 0.0,
    DEVELOPED_PERCENTAGE DECIMAL(5,2) DEFAULT 0.0,
    PROCESSING_METHOD VARCHAR(50) NOT NULL,
    TOTAL_AREA_SQFT DECIMAL(15,2),
    LAST_PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Foreign key constraint
    CONSTRAINT FK_SUMMARY_LAND_COVER_CLASS FOREIGN KEY (DOMINANT_NLCD_CODE) REFERENCES LAND_COVER_CLASSES(NLCD_CODE)
);

-- Create PROCESSING_STATUS table for tracking bulk operations
CREATE OR REPLACE TABLE PROCESSING_STATUS (
    ID INTEGER AUTOINCREMENT PRIMARY KEY,
    JOB_ID VARCHAR(100) NOT NULL UNIQUE,
    STATE_CODE VARCHAR(2) NOT NULL,
    STATUS VARCHAR(50) NOT NULL, -- 'pending', 'running', 'completed', 'failed', 'paused'
    TOTAL_PARCELS INTEGER,
    PROCESSED_PARCELS INTEGER DEFAULT 0,
    FAILED_PARCELS INTEGER DEFAULT 0,
    BATCH_SIZE INTEGER DEFAULT 100,
    CURRENT_BATCH INTEGER DEFAULT 0,
    LAST_PROCESSED_PARCEL_ID VARCHAR(100),
    ERROR_MESSAGE TEXT,
    STARTED_AT TIMESTAMP_NTZ,
    COMPLETED_AT TIMESTAMP_NTZ,
    ESTIMATED_COMPLETION TIMESTAMP_NTZ,
    PROCESSING_RATE_PER_HOUR DECIMAL(10,2),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS IDX_LAND_COVER_PARCEL_ID ON LAND_COVER(PARCEL_ID);
CREATE INDEX IF NOT EXISTS IDX_LAND_COVER_NLCD_CODE ON LAND_COVER(NLCD_CODE);
CREATE INDEX IF NOT EXISTS IDX_LAND_COVER_YEAR ON LAND_COVER(NLCD_YEAR);
CREATE INDEX IF NOT EXISTS IDX_LAND_COVER_DOMINANT ON LAND_COVER(IS_DOMINANT_CLASS);
CREATE INDEX IF NOT EXISTS IDX_LAND_COVER_CREATED ON LAND_COVER(CREATED_AT);

CREATE INDEX IF NOT EXISTS IDX_SUMMARY_PARCEL_ID ON LAND_COVER_SUMMARY(PARCEL_ID);
CREATE INDEX IF NOT EXISTS IDX_SUMMARY_DOMINANT_CODE ON LAND_COVER_SUMMARY(DOMINANT_NLCD_CODE);
CREATE INDEX IF NOT EXISTS IDX_SUMMARY_AGRICULTURAL ON LAND_COVER_SUMMARY(IS_AGRICULTURAL);
CREATE INDEX IF NOT EXISTS IDX_SUMMARY_PROCESSED ON LAND_COVER_SUMMARY(LAST_PROCESSED_AT);

CREATE INDEX IF NOT EXISTS IDX_PROCESSING_STATUS_JOB_ID ON PROCESSING_STATUS(JOB_ID);
CREATE INDEX IF NOT EXISTS IDX_PROCESSING_STATUS_STATE ON PROCESSING_STATUS(STATE_CODE);
CREATE INDEX IF NOT EXISTS IDX_PROCESSING_STATUS_STATUS ON PROCESSING_STATUS(STATUS);

-- Create views for common queries
CREATE OR REPLACE VIEW VW_AGRICULTURAL_PARCELS AS
SELECT 
    s.PARCEL_ID,
    s.DOMINANT_CLASS_NAME,
    s.DOMINANT_CLASS_PERCENTAGE,
    s.AGRICULTURAL_PERCENTAGE,
    s.NLCD_YEAR,
    s.LAST_PROCESSED_AT
FROM LAND_COVER_SUMMARY s
WHERE s.IS_AGRICULTURAL = TRUE;

CREATE OR REPLACE VIEW VW_LAND_COVER_DETAIL AS
SELECT 
    plc.PARCEL_ID,
    plc.NLCD_YEAR,
    plc.NLCD_CODE,
    lcc.CLASS_NAME,
    lcc.CLASS_CATEGORY,
    lcc.CLASS_COLOR,
    plc.COVERAGE_PERCENTAGE,
    plc.COVERAGE_AREA_SQFT,
    plc.IS_DOMINANT_CLASS,
    plc.PROCESSING_METHOD,
    plc.CONFIDENCE_SCORE,
    lcc.IS_AGRICULTURAL,
    lcc.IS_NATURAL,
    plc.CREATED_AT
FROM LAND_COVER plc
JOIN LAND_COVER_CLASSES lcc ON plc.NLCD_CODE = lcc.NLCD_CODE
ORDER BY plc.PARCEL_ID, plc.COVERAGE_PERCENTAGE DESC;

-- Create stored procedures for common operations
CREATE OR REPLACE PROCEDURE SP_UPDATE_PARCEL_SUMMARY(PARCEL_ID_PARAM VARCHAR(100))
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING;
BEGIN
    -- Delete existing summary
    DELETE FROM LAND_COVER_SUMMARY WHERE PARCEL_ID = PARCEL_ID_PARAM;
    
    -- Insert new summary
    INSERT INTO LAND_COVER_SUMMARY (
        PARCEL_ID, NLCD_YEAR, DOMINANT_NLCD_CODE, DOMINANT_CLASS_NAME, 
        DOMINANT_CLASS_PERCENTAGE, TOTAL_CLASSES_COUNT, IS_AGRICULTURAL, 
        IS_NATURAL, IS_DEVELOPED, AGRICULTURAL_PERCENTAGE, NATURAL_PERCENTAGE, 
        DEVELOPED_PERCENTAGE, PROCESSING_METHOD, TOTAL_AREA_SQFT
    )
    SELECT 
        PARCEL_ID_PARAM,
        MAX(plc.NLCD_YEAR) as NLCD_YEAR,
        dominant.NLCD_CODE as DOMINANT_NLCD_CODE,
        dominant.CLASS_NAME as DOMINANT_CLASS_NAME,
        dominant.COVERAGE_PERCENTAGE as DOMINANT_CLASS_PERCENTAGE,
        COUNT(*) as TOTAL_CLASSES_COUNT,
        MAX(CASE WHEN lcc.IS_AGRICULTURAL THEN TRUE ELSE FALSE END) as IS_AGRICULTURAL,
        MAX(CASE WHEN lcc.IS_NATURAL THEN TRUE ELSE FALSE END) as IS_NATURAL,
        MAX(CASE WHEN lcc.CLASS_CATEGORY = 'Developed' THEN TRUE ELSE FALSE END) as IS_DEVELOPED,
        COALESCE(SUM(CASE WHEN lcc.IS_AGRICULTURAL THEN plc.COVERAGE_PERCENTAGE ELSE 0 END), 0) as AGRICULTURAL_PERCENTAGE,
        COALESCE(SUM(CASE WHEN lcc.IS_NATURAL THEN plc.COVERAGE_PERCENTAGE ELSE 0 END), 0) as NATURAL_PERCENTAGE,
        COALESCE(SUM(CASE WHEN lcc.CLASS_CATEGORY = 'Developed' THEN plc.COVERAGE_PERCENTAGE ELSE 0 END), 0) as DEVELOPED_PERCENTAGE,
        MAX(plc.PROCESSING_METHOD) as PROCESSING_METHOD,
        SUM(plc.COVERAGE_AREA_SQFT) as TOTAL_AREA_SQFT
    FROM LAND_COVER plc
    JOIN LAND_COVER_CLASSES lcc ON plc.NLCD_CODE = lcc.NLCD_CODE
    JOIN (
        SELECT plc2.NLCD_CODE, lcc2.CLASS_NAME, plc2.COVERAGE_PERCENTAGE
        FROM LAND_COVER plc2
        JOIN LAND_COVER_CLASSES lcc2 ON plc2.NLCD_CODE = lcc2.NLCD_CODE
        WHERE plc2.PARCEL_ID = PARCEL_ID_PARAM
        ORDER BY plc2.COVERAGE_PERCENTAGE DESC
        LIMIT 1
    ) dominant ON plc.NLCD_CODE = dominant.NLCD_CODE
    WHERE plc.PARCEL_ID = PARCEL_ID_PARAM
    GROUP BY PARCEL_ID_PARAM, dominant.NLCD_CODE, dominant.CLASS_NAME, dominant.COVERAGE_PERCENTAGE;
    
    result_msg := 'Summary updated for parcel: ' || PARCEL_ID_PARAM;
    RETURN result_msg;
END;
$$;

-- Create function to get parcel land cover summary
CREATE OR REPLACE FUNCTION FN_GET_LAND_COVER_JSON(PARCEL_ID_PARAM VARCHAR(100))
RETURNS OBJECT
LANGUAGE SQL
AS
$$
    SELECT OBJECT_CONSTRUCT(
        'parcel_id', PARCEL_ID_PARAM,
        'summary', (
            SELECT OBJECT_CONSTRUCT(
                'dominant_class', DOMINANT_CLASS_NAME,
                'dominant_percentage', DOMINANT_CLASS_PERCENTAGE,
                'is_agricultural', IS_AGRICULTURAL,
                'is_natural', IS_NATURAL,
                'is_developed', IS_DEVELOPED,
                'agricultural_percentage', AGRICULTURAL_PERCENTAGE,
                'natural_percentage', NATURAL_PERCENTAGE,
                'developed_percentage', DEVELOPED_PERCENTAGE,
                'nlcd_year', NLCD_YEAR,
                'last_processed', LAST_PROCESSED_AT
            )
            FROM LAND_COVER_SUMMARY 
            WHERE PARCEL_ID = PARCEL_ID_PARAM
        ),
        'classes', (
            SELECT ARRAY_AGG(
                OBJECT_CONSTRUCT(
                    'nlcd_code', NLCD_CODE,
                    'class_name', CLASS_NAME,
                    'class_category', CLASS_CATEGORY,
                    'coverage_percentage', COVERAGE_PERCENTAGE,
                    'coverage_area_sqft', COVERAGE_AREA_SQFT,
                    'is_dominant', IS_DOMINANT_CLASS,
                    'is_agricultural', IS_AGRICULTURAL,
                    'is_natural', IS_NATURAL,
                    'color', CLASS_COLOR
                )
            )
            FROM VW_LAND_COVER_DETAIL
            WHERE PARCEL_ID = PARCEL_ID_PARAM
            ORDER BY COVERAGE_PERCENTAGE DESC
        )
    )
$$;

-- Grant permissions (adjust as needed for your security model)
GRANT USAGE ON SCHEMA TEDDY_DATA.RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA TEDDY_DATA.RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL VIEWS IN SCHEMA TEDDY_DATA.RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL PROCEDURES IN SCHEMA TEDDY_DATA.RAW TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA TEDDY_DATA.RAW TO ROLE ACCOUNTADMIN;

-- Insert test data verification
SELECT 'Schema setup completed successfully!' as STATUS;
SELECT COUNT(*) as LAND_COVER_CLASSES_COUNT FROM LAND_COVER_CLASSES;

-- Show created objects
SHOW TABLES IN SCHEMA TEDDY_DATA.RAW;
SHOW VIEWS IN SCHEMA TEDDY_DATA.RAW;
SHOW PROCEDURES IN SCHEMA TEDDY_DATA.RAW;
SHOW FUNCTIONS IN SCHEMA TEDDY_DATA.RAW;
