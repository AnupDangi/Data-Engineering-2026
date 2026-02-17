# FlowGuard - What's Next?

## 📊 Current Status (As of Feb 16, 2026)

### ✅ COMPLETED (Stages 1-3)

1. **Events Gateway + Kafka** - Fully working
2. **Web UI + Food Catalog** - Fully working
3. **Bronze Layer (Kafka → Snowflake)** - Fully working with:
   - ✅ Idempotency (MERGE-based)
   - ✅ Metadata tracking
   - ✅ Auto-reconnect
   - ✅ ~249 orders, ~242 clicks in Snowflake

---

## 🎯 NEXT PHASE: Choose Your Path

You have **3 main options** for what to build next:

---

### Option 1: Flink Real-Time Attribution (HIGH VALUE) 🔥

**What it does:** Match clicks to orders in real-time to calculate conversion rates

**Why important:**

- Answers: "Which food items get the most clicks but don't convert?"
- Answers: "What's our click-to-order conversion rate per category?"
- Real-time business insights

**What to build:**

```
Clicks + Orders → Flink Job → Match in 30-min window → attributed.events.v1 topic
```

**Core logic:**

```java
// Flink pseudo-code
clicks.join(orders)
  .where(click.user_id == order.user_id)
  .within(30 minutes)
  .select(click, order, timeDiff)
  → emit attributed_event
```

**Deliverables:**

1. Flink Docker container setup
2. Attribution job (click-order matching)
3. New Kafka topic: `attributed.events.v1`
4. Attribution consumer → write to Snowflake SILVER layer
5. Simple dashboard showing conversion rates

**Time estimate:** 2-3 days
**Complexity:** Medium-High (new technology: Flink)
**Value:** High (business insights)

---

### Option 2: Spark ETL + Airflow Batch Jobs (SOLID FOUNDATION) 📊

**What it does:** Daily/hourly aggregations for business metrics

**Why important:**

- Calculate daily GMV (Gross Merchandise Value)
- Food item performance rankings
- User cohort analysis
- Scheduled reports

**What to build:**

```
Bronze → Spark Jobs → SILVER/GOLD layers → Business dashboards
```

**Example jobs:**

1. **Daily Order Aggregation**

   ```sql
   SELECT
     DATE(event_timestamp) as date,
     COUNT(*) as orders,
     SUM(price) as gmv,
     COUNT(DISTINCT user_id) as unique_users
   FROM BRONZE.ORDERS_RAW
   GROUP BY date
   → GOLD.daily_metrics
   ```

2. **Food Performance**

   ```sql
   SELECT
     item_name,
     COUNT(*) as total_orders,
     SUM(price) as revenue,
     AVG(price) as avg_price
   FROM BRONZE.ORDERS_RAW
   GROUP BY item_name
   ORDER BY revenue DESC
   → GOLD.food_performance
   ```

3. **Click Funnel Analysis**
   ```sql
   SELECT
     item_id,
     COUNT(CASE WHEN event_type='impression' THEN 1 END) as impressions,
     COUNT(CASE WHEN event_type='click' THEN 1 END) as clicks,
     clicks / impressions * 100 as ctr
   FROM BRONZE.CLICKS_RAW
   GROUP BY item_id
   → GOLD.funnel_metrics
   ```

**Deliverables:**

1. Airflow Docker setup + DAGs
2. 3-5 Spark transformation jobs
3. SILVER layer (cleaned data)
4. GOLD layer (aggregated metrics)
5. Schedule: Run daily at midnight

**Time estimate:** 2-3 days
**Complexity:** Medium (familiar tech: SQL + Python)
**Value:** High (foundation for analytics)

---

### Option 3: Simple Analytics Dashboard (QUICK WIN) 📈

**What it does:** Visualize existing Bronze data immediately

**Why important:**

- See your data NOW (no waiting for complex processing)
- Validate data quality
- Show stakeholders what you have

**What to build:**

```
Snowflake BRONZE → Streamlit/Metabase/Superset → Dashboard
```

**Dashboard features:**

1. **Real-time Metrics**
   - Orders per hour/day
   - Revenue trends
   - Top food items

2. **Click Analysis**
   - Click heatmap by food category
   - Impression vs click rates
   - User engagement metrics

3. **Data Quality**
   - Events ingested per minute
   - Consumer lag monitoring
   - Error rates

**Tools options:**

- **Streamlit** (Python, fastest) - 4 hours
- **Metabase** (Open source) - 6 hours
- **Superset** (Apache) - 8 hours
- **Grafana** (Advanced) - 1 day

**Time estimate:** 4 hours - 1 day
**Complexity:** Low
**Value:** Medium (visibility, but no new insights)

---

## 🤔 My Recommendation: **Option 2 (Spark + Airflow)**

**Rationale:**

1. **Foundation first** - SILVER/GOLD layers needed for any analytics
2. **Known technology** - SQL + Spark = easier than Flink
3. **Immediate value** - Daily metrics answer business questions
4. **Flexible** - Easy to add more jobs later

**After Option 2, you can:**

- Add Flink for real-time (Option 1)
- Build dashboards on top of GOLD layer (Option 3)
- Add ML models for predictions

---

## 📋 If You Choose Option 2 (Spark + Airflow)

### Step 1: Setup Infrastructure (1 hour)

```bash
# Add to docker-compose.yml
- Apache Airflow (web UI on port 8080)
- Spark master + workers
```

### Step 2: Create SILVER Schema (30 min)

```sql
-- Snowflake
CREATE SCHEMA SILVER;

CREATE TABLE SILVER.orders_cleaned AS
SELECT
  order_id,
  user_id,
  item_id,
  item_name,
  price,
  event_timestamp,
  DATE(event_timestamp) as order_date
FROM BRONZE.ORDERS_RAW
WHERE price > 0 AND item_id IS NOT NULL;
```

### Step 3: Create GOLD Schema (30 min)

```sql
CREATE SCHEMA GOLD;

CREATE TABLE GOLD.daily_order_metrics (
  date DATE,
  total_orders INT,
  total_revenue DECIMAL(10,2),
  unique_users INT,
  avg_order_value DECIMAL(10,2)
);
```

### Step 4: Write Airflow DAG (2 hours)

```python
# dags/daily_etl.py
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

dag = DAG('daily_etl', schedule_interval='0 0 * * *')

# Task 1: Clean orders
clean_orders = SnowflakeOperator(
    task_id='clean_orders',
    sql='INSERT INTO SILVER.orders_cleaned ...',
    dag=dag
)

# Task 2: Aggregate daily metrics
daily_metrics = SnowflakeOperator(
    task_id='daily_metrics',
    sql='INSERT INTO GOLD.daily_order_metrics ...',
    dag=dag
)

clean_orders >> daily_metrics
```

### Step 5: Test & Monitor (1 hour)

```bash
# Trigger manually
airflow dags trigger daily_etl

# Check results in Snowflake
SELECT * FROM GOLD.daily_order_metrics ORDER BY date DESC;
```

---

## 🚀 Quick Decision Matrix

| Criteria           | Flink (Option 1) | Spark+Airflow (Option 2) | Dashboard (Option 3)   |
| ------------------ | ---------------- | ------------------------ | ---------------------- |
| Time to complete   | 2-3 days         | 2-3 days                 | 4 hours - 1 day        |
| Learning curve     | High             | Low-Medium               | Low                    |
| Business value     | High             | High                     | Medium                 |
| Technical debt     | Low              | Low                      | High (if skipping ETL) |
| Flexibility        | Medium           | High                     | Low                    |
| **Recommendation** | ⭐⭐⭐           | ⭐⭐⭐⭐⭐               | ⭐⭐                   |

---

## 💡 Alternative: Hybrid Approach (BEST FOR LEARNING)

**Phase A: Quick wins (1 day)**

1. Create simple GOLD tables in Snowflake (just SQL, no Airflow yet)
2. Build basic Streamlit dashboard
3. Show metrics to "stakeholders"

**Phase B: Production setup (2 days)** 4. Add Airflow for scheduling 5. Add Spark for complex transformations 6. Migrate manual queries to DAGs

**Phase C: Real-time (later)** 7. Add Flink for attribution 8. Add Redis for billing

---

## 📝 What Do You Want?

Tell me:

1. **What questions** do you want to answer with your data?
   - "Which food items are most popular?"
   - "What's my revenue trend?"
   - "Do clicks actually lead to orders?"

2. **What's your timeline?**
   - Need something quick (< 1 day)?
   - Can invest 2-3 days for solid foundation?

3. **What's your goal?**
   - Learning new tech (Flink)?
   - Building production pipeline (Airflow)?
   - Quick demo/portfolio piece (Dashboard)?

**My suggestion:** Start with **Option 2 (Spark + Airflow)** - it gives you:

- Immediate value (daily metrics)
- Clean foundation (SILVER/GOLD layers)
- Easy to extend later (add Flink, ML, etc.)
- Portfolio-worthy (shows you understand data engineering)

Let me know which path you want to take and I'll help you build it! 🚀
