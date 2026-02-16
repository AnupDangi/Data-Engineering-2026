# Batch ETL Quick Start Guide

## ✅ What's Been Implemented

### 1. **Snowflake Schema Layers**
- ✅ BRONZE: Raw events (already working)
- ✅ SILVER: Cleaned, validated data (just created)
- ✅ GOLD: Business metrics (just created)

### 2. **PySpark ETL Jobs**
- ✅ Bronze → Silver (data cleaning)
- ✅ Silver → Gold (metric aggregation)
- ✅ 6 Spark jobs covering all transformations

### 3. **Airflow Orchestration**
- ✅ Astronomer project setup
- ✅ Daily ETL DAG with scheduling
- ✅ Data quality validation
- ✅ Backfill support

---

## 🚀 How to Run

### Option 1: Initialize Schemas (Already Done!)

```bash
cd majorproject/scripts
./init_silver_gold_schemas.sh
```

**Result:** 
- ✅ Created SILVER.ORDERS_CLEAN
- ✅ Created SILVER.CLICKS_CLEAN
- ✅ Created 5 GOLD metric tables

---

### Option 2: Run Spark Jobs Locally (Test Individual Jobs)

```bash
# Activate venv
cd majorproject
source ../venv/bin/activate

# Install Spark dependencies
pip install pyspark==3.4.1

# Run Bronze → Silver (Orders)
cd airflow/include/spark_jobs
python bronze_to_silver_orders.py "2026-02-16"

# Run Bronze → Silver (Clicks)
python bronze_to_silver_clicks.py "2026-02-16"

# Run Silver → Gold (GMV)
python silver_to_gold_gmv.py "2026-02-16"

# Run Silver → Gold (Item Performance)
python silver_to_gold_item_performance.py "2026-02-16"

# Run Silver → Gold (Funnel)
python silver_to_gold_funnel.py "2026-02-16"
```

**What this does:**
- Reads from BRONZE layer
- Cleans and validates data
- Writes to SILVER layer
- Aggregates metrics
- Writes to GOLD layer

---

### Option 3: Run with Airflow (Full Pipeline)

```bash
# 1. Start Astro Airflow
cd majorproject/airflow
astro dev start

# Wait for services to start (~30 seconds)

# 2. Access Airflow UI
# Open: http://localhost:8080
# Username: admin
# Password: admin

# 3. Trigger DAG manually
astro dev run dags trigger flowguard_daily_etl

# OR use Airflow UI:
# - Go to DAGs page
# - Find "flowguard_daily_etl"
# - Click "Play" button

# 4. Monitor progress
# - Click on DAG name
# - View Graph/Grid to see task status
# - Click tasks to see logs
```

**What this does:**
- Runs all 6 Spark jobs in sequence
- Bronze → Silver (orders + clicks in parallel)
- Validates Silver data
- Silver → Gold (3 jobs in parallel)
- Validates Gold metrics
- Sends success notification

---

## 📊 Verify Results in Snowflake

```sql
-- Check SILVER layer
USE DATABASE FLOWGUARD_DB;
USE SCHEMA SILVER;

SELECT COUNT(*) FROM ORDERS_CLEAN;
SELECT COUNT(*) FROM CLICKS_CLEAN;

SELECT * FROM ORDERS_CLEAN LIMIT 10;

-- Check GOLD layer
USE SCHEMA GOLD;

SELECT * FROM DAILY_GMV_METRICS ORDER BY DATE DESC;
SELECT * FROM FOOD_ITEM_PERFORMANCE ORDER BY TOTAL_REVENUE DESC LIMIT 10;
SELECT * FROM USER_FUNNEL_METRICS ORDER BY DATE DESC;
```

---

## 🔄 Current Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     REAL-TIME ETL                           │
│  Web UI → Events Gateway → Kafka → Bronze Consumer         │
│                           ↓                                  │
│                  Snowflake BRONZE Layer                      │
│                   (249 orders, 242 clicks)                   │
└─────────────────────────────────────────────────────────────┘
                           ↓
                           ↓
┌─────────────────────────────────────────────────────────────┐
│                      BATCH ETL                              │
│                                                             │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   BRONZE    │ →  │    SILVER    │ →  │     GOLD     │  │
│  │  (Raw)      │    │  (Cleaned)   │    │  (Metrics)   │  │
│  │             │    │              │    │              │  │
│  │ ORDERS_RAW  │    │ ORDERS_CLEAN │    │ GMV_METRICS  │  │
│  │ CLICKS_RAW  │    │ CLICKS_CLEAN │    │ ITEM_PERF    │  │
│  │             │    │              │    │ FUNNEL       │  │
│  └─────────────┘    └──────────────┘    └──────────────┘  │
│                                                             │
│         PySpark Jobs (Airflow Orchestrated)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 📝 Next Steps

### 1. **Backfill Historical Data**
```bash
# Process all historical dates
cd airflow
astro dev run dags backfill flowguard_daily_etl \
  --start-date 2026-02-01 \
  --end-date 2026-02-15
```

### 2. **Schedule Daily Runs**
- Already configured to run daily at 1:00 AM UTC
- Will automatically process previous day's data
- No manual intervention needed

### 3. **Add More Metrics (Future)**
- User retention cohorts
- Hourly order patterns
- Geographic analysis
- A/B test metrics

### 4. **Build Dashboard (Next Phase)**
- Connect Tableau/PowerBI to GOLD layer
- Pre-aggregated metrics = fast queries
- Or build custom dashboard with React + Snowflake API

---

## 🐛 Troubleshooting

### Problem: Airflow shows "Import Error"
**Solution:**
```bash
cd airflow
astro dev restart
```

### Problem: Spark job fails with Snowflake connection error
**Solution:** Check `airflow/.env` has correct Snowflake credentials

### Problem: No data in SILVER/GOLD
**Solution:** 
1. Verify BRONZE has data: `SELECT COUNT(*) FROM BRONZE.ORDERS_RAW`
2. Check Spark job logs for errors
3. Verify date filter matches data: `WHERE DATE_PARTITION = '2026-02-16'`

### Problem: Slow Spark performance
**Solution:**
- Increase executor memory in DAG: `"spark.executor.memory": "4g"`
- Add more partitions: `df.repartition(10)`
- Check Snowflake warehouse size: `ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = MEDIUM`

---

## 📚 Documentation

- [Project Structure](../PROJECT_STRUCTURE.md) - Full architecture overview
- [Spark Jobs README](../airflow/include/spark_jobs/README.md) - Job details
- [Airflow DAG](../airflow/dags/flowguard_daily_etl.py) - Pipeline logic

---

## ✅ Summary

**Status:**
- ✅ SILVER schema created
- ✅ GOLD schema created  
- ✅ 6 Spark jobs implemented
- ✅ Airflow DAG configured
- ✅ Ready to process data!

**What You Can Do Now:**
1. Run Spark jobs locally to test
2. Start Airflow and trigger DAG manually
3. Query SILVER/GOLD tables in Snowflake
4. Let Airflow run daily automatically

**Current Data:**
- BRONZE: 249 orders, 242 clicks (real-time)
- SILVER: Ready to process (run Spark jobs)
- GOLD: Ready to aggregate (run Spark jobs)

🚀 **Your FlowGuard batch ETL pipeline is ready!**
