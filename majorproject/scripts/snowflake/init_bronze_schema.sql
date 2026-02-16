-- Snowflake Bronze Layer Schema Initialization
-- Creates raw event tables with VARIANT columns for schema flexibility

-- Create database and schema (using qualified names)
CREATE DATABASE IF NOT EXISTS FLOWGUARD_DB;
CREATE SCHEMA IF NOT EXISTS FLOWGUARD_DB.BRONZE;

-- Switch context
USE DATABASE FLOWGUARD_DB;
USE SCHEMA BRONZE;

-- Orders Raw Table: Stores all order events from Kafka
CREATE TABLE IF NOT EXISTS ORDERS_RAW (
    -- Primary key
    EVENT_ID VARCHAR(100) PRIMARY KEY,
    
    -- Indexed columns for fast queries (known v1 fields)
    ORDER_ID VARCHAR(100) NOT NULL,
    USER_ID INTEGER NOT NULL,
    ITEM_ID INTEGER NOT NULL,
    ITEM_NAME VARCHAR(255),
    PRICE DECIMAL(10, 2) NOT NULL,
    STATUS VARCHAR(50),
    EVENT_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    
    -- Raw event storage (VARIANT = schema-flexible JSON)
    -- ✅ Handles any future fields without schema changes!
    RAW_EVENT VARIANT NOT NULL,
    
    -- Production Metadata (for debugging and idempotency)
    EVENT_TYPE VARCHAR(50),                -- Event type for filtering
    SCHEMA_VERSION VARCHAR(20),            -- Track v1/v2 schema evolution
    SOURCE_TOPIC VARCHAR(100),             -- Which Kafka topic produced this
    PRODUCER_SERVICE VARCHAR(100),         -- Which service emitted the event
    INGESTION_ID VARCHAR(200) UNIQUE,      -- Idempotency key: topic-partition-offset
    
    -- Ingestion Metadata
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PARTITION_DATE DATE AS (TO_DATE(EVENT_TIMESTAMP))
);

-- Clicks Raw Table: Stores all click/impression events from Kafka
CREATE TABLE IF NOT EXISTS CLICKS_RAW (
    -- Primary key
    EVENT_ID VARCHAR(100) PRIMARY KEY,
    
    -- Indexed columns for fast queries
    USER_ID INTEGER NOT NULL,
    EVENT_TYPE VARCHAR(50) NOT NULL,  -- 'click' or 'impression'
    ITEM_ID INTEGER,
    SESSION_ID VARCHAR(100),
    EVENT_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    
    -- Raw event storage (VARIANT = schema-flexible JSON)
    RAW_EVENT VARIANT NOT NULL,
    
    -- Production Metadata (for debugging and idempotency)
    SCHEMA_VERSION VARCHAR(20),            -- Track v1/v2 schema evolution
    SOURCE_TOPIC VARCHAR(100),             -- Which Kafka topic produced this
    PRODUCER_SERVICE VARCHAR(100),         -- Which service emitted the event
    INGESTION_ID VARCHAR(200) UNIQUE,      -- Idempotency key: topic-partition-offset
    
    -- Ingestion Metadata
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PARTITION_DATE DATE AS (TO_DATE(EVENT_TIMESTAMP))
);

-- Note: Snowflake doesn't use traditional indexes like PostgreSQL
-- Query performance is optimized through:
-- - Micro-partitions (automatic)
-- - Clustering keys (for very large tables)
-- - Search optimization service (optional)

-- Verify tables created
SHOW TABLES IN BRONZE;

-- Sample queries for validation
-- SELECT COUNT(*) FROM ORDERS_RAW;
-- SELECT COUNT(*) FROM CLICKS_RAW;
-- SELECT RAW_EVENT:delivery_address::STRING FROM ORDERS_RAW LIMIT 1;  -- Query new fields!
