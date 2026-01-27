# Zomato Delivery Analytics Pipeline

**Real-time Data Ingestion + Batch Processing ETL**

A production-ready data engineering pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for processing Zomato delivery data at scale.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring](#monitoring)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project demonstrates a complete data engineering pipeline that:

1. **Streams** 45,584 delivery records continuously at 1000 events/sec
2. **Ingests** data into Bronze layer (raw JSONL files, 128MB per file)
3. **Transforms** Bronze → Silver (cleaning, deduplication, validation)
4. **Aggregates** Silver → Gold (business metrics by city & vehicle type)
5. **Orchestrates** ETL with Apache Airflow (every 30 minutes)

### Use Case

Analyze Zomato food delivery metrics including:

- Delivery time patterns by city
- Vehicle performance analysis
- Delivery person ratings
- Weather & traffic impact on delivery times

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PIPELINE FLOW                          │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│  ZomatoData.csv │  (6MB, 45,584 records)
│  (Static File)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PRODUCER (Python Streaming Ingestion)                              │
│  • Rate: 1000 events/sec                                            │
│  • Strategy: Infinite CSV replay                                    │
│  • Buffer: 128MB in-memory, flush on size limit                     │
│  • Output: JSONL (newline-delimited JSON)                           │
└────────┬────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Data Lake)                                       │
│  Location: ./src/data/data_storage/bronze/                          │
│  Format: JSONL                                                       │
│  Partitioning: year=YYYY/month=MM/day=DD/hour=HH/                   │
│  File Size: ~128MB per file                                         │
│  Behavior: Append to same file until 128MB, then create new file    │
└────────┬────────────────────────────────────────────────────────────┘
         │
         │ Docker Volume Mount
         │ Host: ./src/data/data_storage
         │ Container: /opt/airflow/data_lake
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AIRFLOW ORCHESTRATION (Astronomer/Docker)                          │
│  • Schedule: Every 30 minutes                                       │
│  • Catchup: Enabled (processes historical partitions)               │
│  • Max Active Runs: 1 (prevents concurrent runs)                    │
└────────┬────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ETL TASK 1: bronze_to_silver                                       │
│  • Read: Bronze JSONL partition                                     │
│  • Transform: Flatten JSON, type casting, null handling             │
│  • Deduplicate: By event_id (keep latest)                           │
│  • Validate: delivery_time_minutes > 0 and < 300                    │
│  • Write: Silver Parquet (Snappy compression)                       │
│  • Partitioning: year=YYYY/month=MM/day=DD/hour=HH/                 │
└────────┬────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleaned & Validated Data)                            │
│  Location: ./src/data/data_storage/silver/                          │
│  Format: Parquet                                                     │
│  Schema: Strongly typed (Int, Double, Timestamp, String)            │
└────────┬────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ETL TASK 2: silver_to_gold                                         │
│  • Read: Silver Parquet partition                                   │
│  • Filter: Only valid records (is_valid = True)                     │
│  • Aggregate 1: Group by city → delivery metrics                    │
│  • Aggregate 2: Group by vehicle_type → performance metrics         │
│  • Write: Gold Parquet (append mode)                                │
└────────┬────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Business Metrics)                                      │
│  Location: ./src/data/data_storage/gold/                            │
│  Format: Parquet                                                     │
│  Tables:                                                             │
│    • by_city/ - City-wise delivery metrics                          │
│    • by_vehicle/ - Vehicle performance metrics                      │
│  Metrics: order_count, avg/min/max delivery_time, avg_rating        │
└─────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture Layers

| Layer      | Purpose             | Format  | Size        | Retention  |
| ---------- | ------------------- | ------- | ----------- | ---------- |
| **Bronze** | Raw ingestion       | JSONL   | ~128MB/file | Indefinite |
| **Silver** | Cleaned & validated | Parquet | Compressed  | 90 days    |
| **Gold**   | Business metrics    | Parquet | Aggregated  | 1 year     |

---

## ✨ Features

### Producer (Bronze Ingestion)

- ✅ **High throughput**: 1000 events/sec
- ✅ **Intelligent buffering**: 128MB in-memory before flush
- ✅ **File size management**: Append to single file until 128MB, then create new
- ✅ **Infinite replay**: CSV loops continuously with unique event IDs
- ✅ **Idempotency**: Duplicate detection within session
- ✅ **Hive-style partitioning**: year/month/day/hour
- ✅ **Production logging**: Detailed metrics in `producer_pipeline.log`

### ETL Pipeline (Airflow + Spark)

- ✅ **Bronze → Silver**: Deduplication, type safety, validation
- ✅ **Silver → Gold**: City & vehicle aggregations
- ✅ **Incremental processing**: Hourly partitions
- ✅ **Graceful skipping**: Handles missing partitions
- ✅ **Local Spark**: Embedded sessions (no cluster needed)
- ✅ **TaskFlow API**: Modern Airflow 2.x patterns
- ✅ **Error handling**: Retries with exponential backoff

### Data Quality

- ✅ **Schema enforcement**: Strong typing (Int, Double, Timestamp)
- ✅ **Null handling**: Safe casting with fallback to NULL
- ✅ **Deduplication**: Latest record wins by ingestion_time
- ✅ **Validation**: Business rules (delivery_time: 0-300 minutes)
- ✅ **Audit trail**: processed_at timestamps

---

## 🛠️ Technology Stack

| Component         | Technology            | Version | Purpose                         |
| ----------------- | --------------------- | ------- | ------------------------------- |
| **Orchestration** | Apache Airflow        | 2.x     | Workflow management             |
| **Container**     | Docker + Astro CLI    | Latest  | Local Airflow environment       |
| **Processing**    | PySpark               | 3.x     | Distributed data transformation |
| **Storage**       | File System (Parquet) | -       | Data lake storage               |
| **Language**      | Python                | 3.10+   | Producer & ETL logic            |
| **Monitoring**    | Airflow UI            | -       | Pipeline observability          |

---

## 📦 Prerequisites

### Required Software

1. **Docker Desktop** (macOS/Windows) or **Docker Engine** (Linux)
   - Version: 20.10+
   - Memory: Minimum 4GB allocated
   - Download: https://www.docker.com/products/docker-desktop

2. **Astro CLI** (Astronomer's Airflow dev tool)

   ```bash
   # macOS (Homebrew)
   brew install astro

   # Or manual installation
   curl -sSL install.astronomer.io | sudo bash
   ```

3. **Python 3.10+**

   ```bash
   python3 --version  # Should be 3.10 or higher
   ```

4. **Git** (for cloning repository)
   ```bash
   git --version
   ```

### System Requirements

- **RAM**: 8GB minimum (16GB recommended)
- **Storage**: 5GB free space
- **CPU**: 2+ cores
- **OS**: macOS, Linux, or Windows with WSL2

---

## 🚀 Installation & Setup

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd FlowGuard/learntools/miniproject2
```

### Step 2: Install Python Dependencies (Producer)

```bash
cd src/producer
pip install -r requirements.txt  # If requirements file exists
# Or manually:
# pip install pandas (for CSV reading)
```

### Step 3: Configure Docker Volume Mount

The Airflow container needs to access the data lake. Verify the mount configuration:

```bash
cd ../airflow
cat docker-compose.override.yml
```

**Expected content:**

```yaml
services:
  scheduler:
    volumes:
      - /Users/YOUR_USERNAME/path/to/miniproject2/src/data/data_storage:/opt/airflow/data_lake:rw
```

⚠️ **IMPORTANT**: Update the path to match your local system!

### Step 4: Start Airflow

```bash
cd src/airflow
astro dev start
```

**Wait for:**

```
✔ Project started
➤ Airflow UI: http://localhost:8080
```

**Default credentials:**

- Username: `admin`
- Password: `admin`

### Step 5: Verify Data Lake Access

```bash
# Get scheduler container ID
SCHED=$(docker ps --filter "name=scheduler" --format "{{.ID}}")

# Verify mount
docker exec $SCHED ls -lh /opt/airflow/data_lake/bronze/
```

Expected: Directory should exist (may be empty initially)

---

## 🏃 Running the Pipeline

### Start Producer (Bronze Ingestion)

```bash
cd src/producer
python3 main.py
```

**Expected output:**

```
🚀 Bronze Data Ingestion Pipeline Started
Source: ../data/ZomatoDataset.csv
Destination: ../data/data_storage/bronze
Throughput: 1000 events/sec
File size: 128MB

📖 Starting CSV read cycle #1
📊 Cycle #1 | Batch 10 | Events: 10,000 | Files: 0 | Buffer: 7.09MB
📄 Creating NEW file: events_20260127105322660871.json
✅ Appended 45,584 events (32.33MB) → events_*.json | Total file size: 32.33MB / 128MB
```

**Monitor logs:**

```bash
tail -f producer_pipeline.log
```

**Stop producer:**

- Press `Ctrl+C` (graceful shutdown)

### Enable Airflow DAG

1. Open Airflow UI: http://localhost:8080
2. Find DAG: `zomato_hourly_etl`
3. Toggle the switch to **ON** (blue)
4. Click **"Trigger DAG"** for manual run

### Verify Pipeline Execution

**Check Bronze files:**

```bash
ls -lh src/data/data_storage/bronze/year=2026/month=01/day=27/hour=*/
```

**Check Silver output:**

```bash
ls -lh src/data/data_storage/silver/year=2026/month=01/day=27/hour=*/
```

**Check Gold metrics:**

```bash
ls -lh src/data/data_storage/gold/year=2026/month=01/day=27/hour=*/by_city/
ls -lh src/data/data_storage/gold/year=2026/month=01/day=27/hour=*/by_vehicle/
```

---

## 📊 Monitoring

### Producer Metrics

**Log file:** `src/producer/producer_pipeline.log`

**Key metrics:**

- **Events/sec**: Should maintain 1000 events/sec
- **Buffer size**: Grows to ~32MB per CSV cycle
- **File count**: Increments when file exceeds 128MB
- **Cycles completed**: Increases indefinitely

**Sample log:**

```
📊 Cycle #5 | Batch 20 | Events: 65,584 | Files: 1 | Buffer: 14.19MB
✅ Appended 45,584 events (32.33MB) → events_*.json | Total file size: 97.32MB / 128MB
```

### Airflow Monitoring

**Airflow UI:** http://localhost:8080

**DAG Status:**

- **Green**: Task succeeded ✅
- **Yellow**: Task running or queued
- **Red**: Task failed ❌

**Task Logs:**
Click on any task → "Log" button → View detailed execution logs

**Key metrics in logs:**

```
✅ Loaded 136752 records
✅ Deduplicated: 136752 → 136752
✅ Written 136752 records to silver
✅ Gold metrics: 5 city rows, 3 vehicle rows
```

### Data Quality Checks

```bash
# Count records in Silver
find src/data/data_storage/silver -name "*.parquet" -exec ls -lh {} \;

# Count records in Gold
find src/data/data_storage/gold -name "*.parquet" -exec ls -lh {} \;
```

---

## 📁 Project Structure

```
miniproject2/
├── README.md                     # This file
├── PIPELINE_STATUS.md            # Current pipeline status & fixes
├── src/
│   ├── producer/                 # Bronze layer ingestion
│   │   ├── main.py              # Entry point (infinite CSV replay)
│   │   ├── csv_reader.py        # CSV streaming logic
│   │   ├── rate_controller.py   # 1000 events/sec rate limiting
│   │   ├── event_builder.py     # JSON event construction
│   │   ├── bronze_writer.py     # Buffered JSONL writer (128MB files)
│   │   └── producer_pipeline.log # Runtime logs
│   │
│   ├── airflow/                  # Orchestration & ETL
│   │   ├── dags/
│   │   │   └── hourly_etl.py    # Bronze → Silver → Gold DAG
│   │   ├── include/
│   │   │   └── spark/           # Spark job modules (optional)
│   │   ├── docker-compose.override.yml  # Volume mount config
│   │   ├── .astro/
│   │   │   └── config.yaml      # Astro CLI settings
│   │   └── requirements.txt     # Airflow dependencies
│   │
│   └── data/
│       ├── ZomatoDataset.csv    # Source data (45,584 rows)
│       └── data_storage/        # Data lake
│           ├── bronze/          # Raw JSONL (128MB files)
│           ├── silver/          # Cleaned Parquet
│           └── gold/            # Aggregated metrics
│               ├── by_city/
│               └── by_vehicle/
```

---

## ⚙️ Configuration

### Producer Settings

**File:** `src/producer/main.py`

```python
CSV_PATH = "../data/ZomatoDataset.csv"
BATCH_SIZE = 1000                # Events per second
INTERVAL_SECONDS = 1             # Batch interval
BRONZE_BASE_PATH = "../data/data_storage/bronze"
MAX_FILE_SIZE_MB = 128           # File size before new file creation
```

### Airflow DAG Settings

**File:** `src/airflow/dags/hourly_etl.py`

```python
BRONZE_BASE = "/opt/airflow/data_lake"
SILVER_BASE = "/opt/airflow/data_lake"
GOLD_BASE = "/opt/airflow/data_lake"

# DAG schedule
schedule="*/30 * * * *"  # Every 30 minutes
catchup=True             # Process historical partitions
max_active_runs=1        # Prevent concurrent runs
```

### Docker Volume Mount

**File:** `src/airflow/docker-compose.override.yml`

```yaml
services:
  scheduler:
    volumes:
      - /absolute/path/to/src/data/data_storage:/opt/airflow/data_lake:rw
```

---

## 🐛 Troubleshooting

### Producer Issues

**Problem:** Producer not creating files

```bash
# Check if producer is running
ps aux | grep "python.*main.py"

# Check logs for errors
tail -50 producer_pipeline.log | grep -i error

# Verify CSV file exists
ls -lh src/data/ZomatoDataset.csv
```

**Problem:** Buffer not flushing

- **Check:** Event IDs are unique (includes timestamp)
- **Check:** `seen_event_ids` is cleared after each flush

### Airflow Issues

**Problem:** DAG tasks retry infinitely (yellow)

```bash
# Check Airflow container logs
docker logs <scheduler_container_id> | tail -100

# Common causes:
# 1. Bronze partition doesn't exist → Wait for producer
# 2. Mount path incorrect → Verify docker-compose.override.yml
# 3. Spark dependencies missing → Check requirements.txt
```

**Problem:** "File not found" errors

```bash
# Verify mount inside container
SCHED=$(docker ps --filter "name=scheduler" --format "{{.ID}}")
docker exec $SCHED ls -lh /opt/airflow/data_lake/bronze/

# If empty, check host path
ls -lh src/data/data_storage/bronze/
```

**Problem:** VOID type error (Parquet)

- **Fixed in:** `hourly_etl.py` line 243
- **Solution:** `.withColumn("city", lit(None).cast("string"))`

### Data Quality Issues

**Problem:** Duplicate records in Silver

- **Check:** Deduplication window in `bronze_to_silver` task
- **Verify:** `event_id` uniqueness

**Problem:** No Gold metrics generated

- **Check:** Silver data contains valid records (`is_valid = True`)
- **Verify:** delivery_time_minutes is between 0-300

### Performance Issues

**Problem:** Slow ETL execution

```bash
# Increase Spark parallelism
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Allocate more memory
spark.conf.set("spark.driver.memory", "2g")
```

**Problem:** Docker running slow

- **Check:** Docker Desktop memory allocation (min 4GB)
- **Check:** Disk space available

---

## 🎓 Learning Outcomes

This project demonstrates:

1. **Streaming ingestion** with rate limiting and buffering
2. **Medallion architecture** (Bronze/Silver/Gold layers)
3. **Airflow orchestration** with TaskFlow API
4. **PySpark transformations** (deduplication, aggregation, partitioning)
5. **Docker containerization** with volume mounts
6. **Production patterns** (idempotency, error handling, logging)
7. **Data quality** (validation, type safety, schema enforcement)

---

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Astro CLI Guide](https://docs.astronomer.io/astro/cli/overview)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

## 📄 License

This project is for educational purposes.

---

## 👤 Author

Data Engineering Learning Project - 2026
