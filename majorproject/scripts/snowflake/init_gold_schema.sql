-- ============================================================================
-- GOLD LAYER SCHEMA
-- Business metrics and aggregated analytics tables
-- ============================================================================

USE DATABASE FLOWGUARD_DB;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- ============================================================================
-- DAILY_GMV_METRICS: Daily revenue and order metrics
-- ============================================================================
CREATE TABLE IF NOT EXISTS DAILY_GMV_METRICS (
    -- Primary key
    DATE DATE PRIMARY KEY,
    
    -- Revenue metrics
    TOTAL_REVENUE FLOAT NOT NULL,
    AVG_ORDER_VALUE FLOAT,
    
    -- Order metrics
    ORDER_COUNT INT NOT NULL,
    TOTAL_ITEMS_SOLD INT,
    
    -- User metrics
    UNIQUE_USERS INT,
    NEW_USERS INT,
    RETURNING_USERS INT,
    
    -- Growth metrics (compared to previous day)
    REVENUE_GROWTH_PCT FLOAT,
    ORDER_GROWTH_PCT FLOAT,
    
    -- Metadata
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0'
);

CREATE INDEX IF NOT EXISTS idx_gmv_date ON DAILY_GMV_METRICS(DATE);


-- ============================================================================
-- FOOD_ITEM_PERFORMANCE: Item-level performance metrics
-- ============================================================================
CREATE TABLE IF NOT EXISTS FOOD_ITEM_PERFORMANCE (
    -- Composite primary key
    DATE DATE NOT NULL,
    ITEM_ID VARCHAR(50) NOT NULL,
    
    -- Item details
    ITEM_NAME VARCHAR(200),
    CATEGORY VARCHAR(50),
    
    -- Order metrics
    TOTAL_ORDERS INT NOT NULL,
    TOTAL_REVENUE FLOAT NOT NULL,
    UNIQUE_BUYERS INT,
    AVG_PRICE FLOAT,
    
    -- Click metrics
    CLICK_COUNT INT DEFAULT 0,
    IMPRESSION_COUNT INT DEFAULT 0,
    
    -- Conversion metrics
    CONVERSION_RATE FLOAT,  -- orders / clicks
    CLICK_THROUGH_RATE FLOAT,  -- clicks / impressions
    
    -- Rankings
    RANK_BY_REVENUE INT,
    RANK_BY_ORDERS INT,
    RANK_BY_CONVERSION INT,
    
    -- Performance indicators
    IS_TOP_SELLER BOOLEAN DEFAULT FALSE,  -- Top 10% by revenue
    IS_UNDERPERFORMING BOOLEAN DEFAULT FALSE,  -- Low conversion + high clicks
    
    -- Metadata
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0',
    
    PRIMARY KEY (DATE, ITEM_ID)
);

CREATE INDEX IF NOT EXISTS idx_item_perf_date ON FOOD_ITEM_PERFORMANCE(DATE);
CREATE INDEX IF NOT EXISTS idx_item_perf_revenue ON FOOD_ITEM_PERFORMANCE(TOTAL_REVENUE DESC);
CREATE INDEX IF NOT EXISTS idx_item_perf_conversion ON FOOD_ITEM_PERFORMANCE(CONVERSION_RATE DESC);


-- ============================================================================
-- USER_FUNNEL_METRICS: Daily conversion funnel analysis
-- ============================================================================
CREATE TABLE IF NOT EXISTS USER_FUNNEL_METRICS (
    -- Primary key
    DATE DATE PRIMARY KEY,
    
    -- Funnel stages
    TOTAL_SESSIONS INT NOT NULL,
    TOTAL_IMPRESSIONS INT,
    TOTAL_CLICKS INT,
    TOTAL_ORDERS INT,
    
    -- Conversion rates
    IMPRESSION_TO_CLICK_RATE FLOAT,  -- clicks / impressions
    CLICK_TO_ORDER_RATE FLOAT,  -- orders / clicks
    SESSION_TO_ORDER_RATE FLOAT,  -- orders / sessions
    
    -- Engagement metrics
    AVG_CLICKS_PER_SESSION FLOAT,
    AVG_ORDERS_PER_SESSION FLOAT,
    AVG_REVENUE_PER_SESSION FLOAT,
    
    -- User behavior
    ACTIVE_USERS INT,
    USERS_WITH_CLICKS INT,
    USERS_WITH_ORDERS INT,
    
    -- Metadata
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0'
);

CREATE INDEX IF NOT EXISTS idx_funnel_date ON USER_FUNNEL_METRICS(DATE);


-- ============================================================================
-- HOURLY_ORDER_PATTERNS: Hour-of-day ordering patterns
-- ============================================================================
CREATE TABLE IF NOT EXISTS HOURLY_ORDER_PATTERNS (
    -- Composite primary key
    DATE DATE NOT NULL,
    HOUR_OF_DAY INT NOT NULL,  -- 0-23
    
    -- Order metrics
    ORDER_COUNT INT NOT NULL,
    TOTAL_REVENUE FLOAT NOT NULL,
    AVG_ORDER_VALUE FLOAT,
    UNIQUE_USERS INT,
    
    -- Performance indicators
    IS_PEAK_HOUR BOOLEAN DEFAULT FALSE,
    PEAK_RANK INT,  -- 1 = busiest hour of the day
    
    -- Comparison metrics
    PCT_OF_DAILY_ORDERS FLOAT,
    PCT_OF_DAILY_REVENUE FLOAT,
    
    -- Metadata
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0',
    
    PRIMARY KEY (DATE, HOUR_OF_DAY)
);

CREATE INDEX IF NOT EXISTS idx_hourly_date ON HOURLY_ORDER_PATTERNS(DATE);
CREATE INDEX IF NOT EXISTS idx_hourly_hour ON HOURLY_ORDER_PATTERNS(HOUR_OF_DAY);


-- ============================================================================
-- USER_COHORT_METRICS: User segmentation and cohort analysis
-- ============================================================================
CREATE TABLE IF NOT EXISTS USER_COHORT_METRICS (
    -- Composite primary key
    DATE DATE NOT NULL,
    USER_ID VARCHAR(50) NOT NULL,
    
    -- User classification
    FIRST_ORDER_DATE DATE,
    DAYS_SINCE_FIRST_ORDER INT,
    COHORT_MONTH VARCHAR(7),  -- YYYY-MM format
    
    -- Lifetime metrics
    LIFETIME_ORDERS INT NOT NULL,
    LIFETIME_REVENUE FLOAT NOT NULL,
    LIFETIME_ITEMS INT,
    
    -- Current period metrics
    ORDERS_THIS_PERIOD INT,
    REVENUE_THIS_PERIOD FLOAT,
    
    -- Engagement metrics
    AVG_DAYS_BETWEEN_ORDERS FLOAT,
    DAYS_SINCE_LAST_ORDER INT,
    
    -- User segments
    USER_SEGMENT VARCHAR(20),  -- 'new', 'active', 'at_risk', 'churned', 'vip'
    IS_VIP BOOLEAN DEFAULT FALSE,  -- Top 10% by lifetime revenue
    
    -- Metadata
    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PROCESSING_VERSION VARCHAR(20) DEFAULT 'v1.0',
    
    PRIMARY KEY (DATE, USER_ID)
);

CREATE INDEX IF NOT EXISTS idx_cohort_date ON USER_COHORT_METRICS(DATE);
CREATE INDEX IF NOT EXISTS idx_cohort_segment ON USER_COHORT_METRICS(USER_SEGMENT);
CREATE INDEX IF NOT EXISTS idx_cohort_vip ON USER_COHORT_METRICS(IS_VIP);

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA GOLD TO ROLE PUBLIC;
GRANT INSERT ON ALL TABLES IN SCHEMA GOLD TO ROLE PUBLIC;
GRANT UPDATE ON ALL TABLES IN SCHEMA GOLD TO ROLE PUBLIC;

SELECT 'GOLD schema created successfully!' AS STATUS;
