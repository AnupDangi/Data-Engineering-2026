# Spark ETL Jobs

PySpark jobs for FlowGuard data pipeline: Bronze -> Silver -> Gold

## Architecture

```
BRONZE (Raw)          SILVER (Clean)           GOLD (Metrics)
─────────────         ──────────────           ──────────────
ORDERS_RAW      ->    ORDERS_CLEAN       ->    DAILY_GMV_METRICS
CLICKS_RAW      ->    CLICKS_CLEAN       ->    FOOD_ITEM_PERFORMANCE
                                               USER_FUNNEL_METRICS
                                               HOURLY_ORDER_PATTERNS
```

## Jobs

### Bronze -> Silver (Cleaning)

**`bronze_to_silver_orders.py`**

- Deduplicates orders using `INGESTION_ID`
- Validates data quality (nulls, negative prices)
- Enriches with food catalog from PostgreSQL
- Flags invalid/duplicate records
- Adds date partitions for query performance

**`bronze_to_silver_clicks.py`**

- Deduplicates clicks using `INGESTION_ID`
- Validates required fields
- Adds date partitions
- Flags invalid/duplicate records

### Silver -> Gold (Aggregation)

**`silver_to_gold_gmv.py`**

- Daily revenue metrics
- Order counts and averages
- User metrics (unique, new, returning)
- Growth vs previous day

**`silver_to_gold_item_performance.py`**

- Item-level revenue and orders
- Click and conversion metrics
- Rankings (by revenue, orders, conversion)
- Performance indicators (top sellers, underperforming)

**`silver_to_gold_funnel.py`**

- Conversion funnel (sessions -> clicks -> orders)
- Engagement metrics (clicks/session, orders/session)
- User activity tracking

## Running Locally

### Prerequisites

```bash
# Install dependencies
pip install pyspark==3.4.1 snowflake-connector-python psycopg2-binary

# Set environment variables
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="5432"
export POSTGRES_USER="flowguard"
export POSTGRES_PASSWORD="flowguard123"
```

### Run Single Job

```bash
# Process specific date
cd airflow/include/spark_jobs
python bronze_to_silver_orders.py "2026-02-16"

# Process all data
python bronze_to_silver_orders.py
```

### Run via Spark Submit

```bash
spark-submit \
  --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 \
  --conf spark.executor.memory=2g \
  airflow/include/spark_jobs/bronze_to_silver_orders.py "2026-02-16"
```

## Running in Airflow

Jobs are orchestrated by `flowguard_daily_etl` DAG:

- Runs daily at 1:00 AM UTC
- Processes previous day's data
- Supports backfill for historical dates

```bash
# Trigger manual run
astro dev run dags trigger flowguard_daily_etl

# Backfill for date range
astro dev run dags backfill flowguard_daily_etl \
  --start-date 2026-02-01 \
  --end-date 2026-02-15
```

## Data Flow

### Bronze to Silver

1. Read raw events from `BRONZE.ORDERS_RAW` / `BRONZE.CLICKS_RAW`
2. Deduplicate using `INGESTION_ID` (topic-partition-offset)
3. Validate data quality:
   - Required fields present
   - Valid data types (price > 0, timestamps not null)
   - Flag invalid records for investigation
4. Enrich with metadata (food catalog category, description)
5. Write to `SILVER.ORDERS_CLEAN` / `SILVER.CLICKS_CLEAN`

### Silver to Gold

1. Read clean data from `SILVER` layer
2. Apply business logic:
   - Group by date, item, user
   - Calculate aggregations (SUM, COUNT, AVG)
   - Compute derived metrics (conversion rates, rankings)
3. Write to `GOLD` analytics tables
4. Overwrite mode (idempotent - can rerun safely)

## Performance Optimization

- **Adaptive Query Execution (AQE)**: Enabled for dynamic partition coalescing
- **Partitioning**: Silver tables partitioned by `DATE_PARTITION`
- **Deduplication**: Window functions with `row_number()`
- **Incremental Processing**: Process only new dates (pass `process_date` arg)

## Data Quality

Each job validates data and tracks:

- `IS_VALID`: Boolean flag for data quality
- `IS_DUPLICATE`: Boolean flag for duplicate detection
- `VALIDATION_ERRORS`: String describing validation failures

Invalid records are kept in Silver for debugging but excluded from Gold metrics.

## Monitoring

Check logs for:

- Record counts at each stage
- Duplicate/invalid record counts
- Processing time
- Spark job metrics

## Troubleshooting

**Connection errors:**

```bash
# Test Snowflake connection
python -c "import snowflake.connector; print('OK')"

# Test PostgreSQL connection
python -c "import psycopg2; print('OK')"
```

**Missing data:**

- Check Bronze layer has data for the date
- Verify `DATE_PARTITION` matches execution date
- Check `IS_VALID = TRUE` filter

**Slow performance:**

- Increase Spark executor memory/cores
- Check Snowflake warehouse size
- Verify partitioning is working

## Schema Changes

When updating schemas:

1. Update DDL files in `scripts/snowflake/`
2. Update Spark job transformations
3. Test with sample data
4. Run schema migration script
5. Backfill historical data if needed
