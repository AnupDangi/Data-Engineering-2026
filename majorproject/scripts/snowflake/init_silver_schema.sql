-- ============================================================================
-- SILVER LAYER SCHEMA
-- Cleaned, deduplicated, and enriched data
-- ============================================================================

USE DATABASE FLOWGUARD_DB;
CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- ============================================================================
-- ORDERS_CLEAN: Deduplicated and validated orders
-- ============================================================================
CREATE TABLE IF NOT EXISTS ORDERS_CLEAN (
    -- Primary identifiers
    ORDER_ID VARCHAR(36) PRIMARY KEY,
    EVENT_ID VARCHAR(20),
    USER_ID VARCHAR(50) NOT NULL,
    
    -- Order details
    ITEM_ID VARCHAR(50) NOT NULL,
    ITEM_NAME VARCHAR(200),
    PRICE FLOAT NOT NULL,
    STATUS VARCHAR(20),
    
    -- Timestamps
    ORDER_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    DATE_PARTITION DATE NOT NULL,  -- For partitioning and efficient queries
    
    -- Enrichment from food catalog
    ENRICHED_CATEGORY VARCHAR(50),
    ENRICHED_DESCRIPTION VARCHAR(500),
    ENRICHED_PREPARATION_TIME INT,
    
    -- Data quality flags
    IS_DUPLICATE BOOLEAN DEFAULT FALSE,
    IS_VALID BOOLEAN DEFAULT TRUE,
    VALIDATION_ERRORS VARCHAR(500),
    
    -- Metadata
    INGESTION_ID VARCHAR(100),
    SOURCE_TABLE VARCHAR(50) DEFAULT 'BRONZE.ORDERS_RAW',
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0'
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_orders_date ON ORDERS_CLEAN(DATE_PARTITION);
CREATE INDEX IF NOT EXISTS idx_orders_user ON ORDERS_CLEAN(USER_ID);
CREATE INDEX IF NOT EXISTS idx_orders_item ON ORDERS_CLEAN(ITEM_ID);
CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON ORDERS_CLEAN(ORDER_TIMESTAMP);


-- ============================================================================
-- CLICKS_CLEAN: Deduplicated and validated clicks/impressions
-- ============================================================================
CREATE TABLE IF NOT EXISTS CLICKS_CLEAN (
    -- Primary identifiers
    EVENT_ID VARCHAR(20) PRIMARY KEY,
    USER_ID VARCHAR(50) NOT NULL,
    SESSION_ID VARCHAR(100),
    
    -- Click details
    ITEM_ID VARCHAR(50),
    EVENT_TYPE VARCHAR(20),  -- 'click' or 'impression'
    
    -- Timestamps
    CLICK_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    DATE_PARTITION DATE NOT NULL,
    
    -- Enrichment
    ENRICHED_ITEM_NAME VARCHAR(200),
    ENRICHED_CATEGORY VARCHAR(50),
    
    -- Data quality flags
    IS_DUPLICATE BOOLEAN DEFAULT FALSE,
    IS_VALID BOOLEAN DEFAULT TRUE,
    VALIDATION_ERRORS VARCHAR(500),
    
    -- Metadata
    INGESTION_ID VARCHAR(100),
    SOURCE_TABLE VARCHAR(50) DEFAULT 'BRONZE.CLICKS_RAW',
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0'
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_clicks_date ON CLICKS_CLEAN(DATE_PARTITION);
CREATE INDEX IF NOT EXISTS idx_clicks_user ON CLICKS_CLEAN(USER_ID);
CREATE INDEX IF NOT EXISTS idx_clicks_item ON CLICKS_CLEAN(ITEM_ID);
CREATE INDEX IF NOT EXISTS idx_clicks_session ON CLICKS_CLEAN(SESSION_ID);

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA SILVER TO ROLE PUBLIC;
GRANT INSERT ON ALL TABLES IN SCHEMA SILVER TO ROLE PUBLIC;
GRANT UPDATE ON ALL TABLES IN SCHEMA SILVER TO ROLE PUBLIC;

SELECT 'SILVER schema created successfully!' AS STATUS;
