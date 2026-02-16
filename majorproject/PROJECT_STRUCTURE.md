# FlowGuard Project Structure

This document explains the organization of the FlowGuard project.

## 📁 Project Structure

```
majorproject/
├── src/                          # Real-time ETL (Streaming)
│   ├── consumers/
│   │   └── bronze_consumer/      # Kafka → Snowflake Bronze Layer
│   ├── services/
│   │   ├── events_gateway/       # FastAPI event ingestion
│   │   └── food_catalog/         # Food items API
│   └── shared/                   # Shared schemas, Kafka configs
│
├── airflow/                      # Batch ETL (Astronomer)
│   ├── dags/                     # Airflow DAGs
│   │   └── flowguard_daily_etl.py
│   ├── include/
│   │   └── spark_jobs/           # PySpark transformations
│   ├── .astro/                   # Astro CLI configuration
│   └── requirements.txt          # Python dependencies
│
├── scripts/                      # Deployment & utility scripts
├── config/                       # Configuration files
├── data/                         # Static data (food items)
└── zomato-web-app/              # Next.js frontend
```

---

## 🔄 Real-time ETL (Streaming Pipeline)

**Location:** `src/`

**Purpose:** Ingest events in real-time from web application to Bronze layer

**Components:**

1. **Events Gateway** (`src/services/events_gateway/`)
   - FastAPI service on port 8000
   - Accepts order and click events via REST API
   - Produces to Kafka topics: `raw.orders.v1`, `raw.clicks.v1`
   - Stores orders in PostgreSQL

2. **Bronze Consumer** (`src/consumers/bronze_consumer/`)
   - Consumes from Kafka topics
   - Writes to Snowflake Bronze layer with idempotency
   - Uses bulk MERGE for performance
   - Tracks metadata (ingestion_id, source_topic, etc.)

**Data Flow:**
```
Web UI → Events Gateway → Kafka → Bronze Consumer → Snowflake BRONZE
```

**Technologies:**
- FastAPI (REST API)
- Kafka (message queue)
- PostgreSQL (operational DB)
- Snowflake (data warehouse)
- Python 3.10+

---

## 📊 Batch ETL (Astronomer + Spark)

**Location:** `airflow/`

**Purpose:** Transform data from Bronze → Silver → Gold for analytics

**Components:**

1. **Airflow DAGs** (`airflow/dags/`)
   - `flowguard_daily_etl.py`: Daily pipeline orchestration
   - Schedules: Daily at 1:00 AM UTC
   - Supports backfill for historical dates

2. **Spark Jobs** (`airflow/include/spark_jobs/`)
   - **Bronze → Silver (Cleaning):**
     - `bronze_to_silver_orders.py`: Deduplicate, validate, enrich orders
     - `bronze_to_silver_clicks.py`: Deduplicate, validate clicks
   
   - **Silver → Gold (Aggregation):**
     - `silver_to_gold_gmv.py`: Daily revenue metrics
     - `silver_to_gold_item_performance.py`: Item-level analytics
     - `silver_to_gold_funnel.py`: Conversion funnel metrics

**Data Flow:**
```
BRONZE (raw) → SILVER (clean) → GOLD (metrics)
```

**Technologies:**
- Apache Airflow (orchestration)
- PySpark (distributed processing)
- Snowflake (data warehouse)
- Astronomer (deployment platform)

---

## 🚀 Running the Project

### Real-time ETL (Always Running)

```bash
# Start all real-time services
./scripts/start_all.sh

# Services:
# - Events Gateway: http://localhost:8000
# - Food Catalog: http://localhost:8001
# - Bronze Consumer: Background process
# - Web UI: http://localhost:3000
```

### Batch ETL (Scheduled)

```bash
# Start Airflow (Astro)
cd airflow/
astro dev start

# Access:
# - Airflow UI: http://localhost:8080
# - Username: admin
# - Password: admin

# Trigger manual run
astro dev run dags trigger flowguard_daily_etl

# Backfill historical dates
astro dev run dags backfill flowguard_daily_etl \
  --start-date 2026-02-01 \
  --end-date 2026-02-15
```

---

## 📂 Snowflake Layer Architecture

### BRONZE (Raw Events)
- `ORDERS_RAW`: Raw order events from Kafka
- `CLICKS_RAW`: Raw click/impression events
- **Purpose:** Landing zone, immutable raw data
- **Managed by:** Real-time ETL (Bronze Consumer)

### SILVER (Cleaned Data)
- `ORDERS_CLEAN`: Deduplicated, validated, enriched orders
- `CLICKS_CLEAN`: Deduplicated, validated clicks
- **Purpose:** Clean, production-ready data
- **Managed by:** Batch ETL (Spark jobs)

### GOLD (Business Metrics)
- `DAILY_GMV_METRICS`: Daily revenue and order metrics
- `FOOD_ITEM_PERFORMANCE`: Item-level performance analytics
- `USER_FUNNEL_METRICS`: Conversion funnel analysis
- `HOURLY_ORDER_PATTERNS`: Time-based ordering patterns
- `USER_COHORT_METRICS`: User segmentation
- **Purpose:** Pre-aggregated analytics for dashboards
- **Managed by:** Batch ETL (Spark jobs)

---

## 🔧 Configuration

### Real-time ETL
- **Config:** `majorproject/.env`
- **Contains:** Kafka, PostgreSQL, Snowflake credentials

### Batch ETL
- **Config:** `airflow/.env`
- **Contains:** Snowflake, Spark, Airflow settings
- **Note:** Must sync Snowflake credentials from parent `.env`

---

## 📝 Development Workflow

### Adding a New Event Type

1. **Real-time ETL:**
   - Add schema in `src/shared/schemas/events.py`
   - Add router in `src/services/events_gateway/routers/`
   - Add Kafka topic in `config/kafka/topics.yaml`
   - Update Bronze Consumer to handle new event

2. **Batch ETL:**
   - Add DDL for Silver table in `scripts/snowflake/`
   - Create Spark job in `airflow/include/spark_jobs/`
   - Update Airflow DAG to run new job

### Testing

```bash
# Real-time ETL: Send test event
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{...}'

# Batch ETL: Run Spark job locally
cd airflow/include/spark_jobs
python bronze_to_silver_orders.py "2026-02-16"
```

---

## 🐛 Debugging

### Real-time ETL Issues
- **Logs:** Check service logs in `/tmp/events_gateway.log`, `/tmp/bronze_consumer.log`
- **Kafka:** Use `kafka-console-consumer` to inspect topics
- **Snowflake:** Query `BRONZE` layer to verify ingestion

### Batch ETL Issues
- **Logs:** Check Airflow UI → DAG Runs → Task Logs
- **Spark:** Check executor logs for detailed errors
- **Snowflake:** Verify `SILVER` and `GOLD` table counts

---

## 📦 Dependencies

### Real-time ETL
- Python 3.10+
- FastAPI, uvicorn
- confluent-kafka
- psycopg2-binary
- snowflake-connector-python

### Batch ETL
- Astronomer Astro CLI
- PySpark 3.4+
- Airflow 2.x
- Snowflake Spark connector

---

## 🎯 Best Practices

1. **Separation of Concerns:**
   - Real-time ETL handles streaming ingestion (low latency)
   - Batch ETL handles transformations (high throughput)

2. **Idempotency:**
   - Bronze Consumer uses MERGE with `INGESTION_ID`
   - Spark jobs use `overwrite` mode for Gold tables

3. **Data Quality:**
   - Silver layer validates and flags invalid records
   - Gold layer only uses `IS_VALID = TRUE` data

4. **Monitoring:**
   - Real-time: Check consumer lag, ingestion rates
   - Batch: Check DAG success rates, job durations

---

## 📚 Further Reading

- [Real-time ETL README](src/consumers/bronze_consumer/README.md)
- [Batch ETL README](airflow/include/spark_jobs/README.md)
- [Snowflake Schema Docs](scripts/snowflake/)
- [Airflow DAG Documentation](airflow/dags/flowguard_daily_etl.py)
