# Apache Airflow: Orchestration for Data Engineers

Airflow is a platform that lets you build and run workflows. A workflow is represented as a DAG (a Directed Acyclic Graph), and contains individual pieces of work called Tasks, arranged with dependencies and data flows taken into account.

---

## 🧠 How Airflow Works

Apache Airflow reduces to one core idea:

> **Describe workflows as code, run them on a schedule, and never lose visibility.**

### The Mental Model

Think of Airflow as a **smart traffic controller** for your data pipelines:

1. **You define** → What tasks need to run and in what order (DAG)
2. **Scheduler decides** → When tasks should execute based on time/dependencies
3. **Executor runs** → Tasks on available workers (local or distributed)
4. **Metadata DB tracks** → Every execution, every retry, every failure
5. **UI shows** → Real-time visibility into pipeline health

### Architecture Components

```
┌─────────────────────────────────────────────────────────┐
│                    AIRFLOW SYSTEM                        │
│                                                          │
│  ┌─────────────┐                                        │
│  │  DAG Files  │  ← You write these (Python code)       │
│  └──────┬──────┘                                        │
│         │                                                │
│         ↓                                                │
│  ┌─────────────┐         ┌─────────────┐               │
│  │  Scheduler  │ ←─────→ │ Metadata DB │               │
│  └──────┬──────┘         └─────────────┘               │
│         │                       ↑                        │
│         │                       │                        │
│         ↓                       │                        │
│  ┌─────────────┐                │                        │
│  │   Executor  │ ───────────────┘                        │
│  └──────┬──────┘                                        │
│         │                                                │
│         ↓                                                │
│  ┌─────────────────────────────┐                        │
│  │         Workers              │                        │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ │                        │
│  │  │Task 1│ │Task 2│ │Task 3│ │                        │
│  │  └──────┘ └──────┘ └──────┘ │                        │
│  └─────────────────────────────┘                        │
│                                                          │
│  ┌─────────────────────────────┐                        │
│  │  Webserver (UI: port 8080)  │                        │
│  └─────────────────────────────┘                        │
│                                                          │
└─────────────────────────────────────────────────────────┘

         External Systems (via Hooks)
              │
              ↓
  ┌──────────┬──────────┬──────────┐
  │ Database │  Kafka   │  Spark   │
  └──────────┴──────────┴──────────┘
```

**Component Flow**:

1. **DAG Files** → Define workflows
2. **Scheduler** → Parses DAGs, creates task instances, writes to DB
3. **Metadata DB** → Central state store (PostgreSQL/MySQL)
4. **Executor** → Queues tasks for execution
5. **Workers** → Execute actual task logic
6. **Webserver** → Provides UI for monitoring/triggering

### Execution Flow

**Step 1: DAG Definition**

```python
# You write this in ~/airflow/dags/
with DAG('my_pipeline', schedule_interval='@daily') as dag:
    task_a >> task_b >> task_c  # Define dependencies
```

**Step 2: Scheduler Parses**

- Scheduler reads your DAG file
- Checks if it's time to run (based on `schedule_interval`)
- Creates `DagRun` (one execution instance)
- Creates `TaskInstance` for each task

**Step 3: Executor Queues**

- Executor picks up task instances that are ready
- Checks dependencies (did upstream tasks succeed?)
- Queues task for execution

**Step 4: Worker Executes**

- Worker receives task
- Runs the actual logic (Python function, Bash command, etc.)
- Returns result: success/failure/retry

**Step 5: Metadata Updates**

- Every state change logged to database
- UI reflects real-time status
- XCom stores inter-task data

**Step 6: Scheduler Continues**

- On success → trigger downstream tasks
- On failure → retry (if configured) or mark failed
- On completion → close DagRun

---

## 📖 Essential Jargon (No Mysticism)

### 1. DAG (Directed Acyclic Graph)

**What**: The blueprint of your workflow.

**Decoded**:

- **Directed** → Tasks have order (`A → B`, not random)
- **Acyclic** → No infinite loops (can't go `A → B → A`)
- **Graph** → Visual representation of dependencies

**Key Point**: A DAG is _code_, not execution. It defines _what should happen_, not _when it happened_.

**Example**:

```python
# This IS a DAG
fetch_data >> clean_data >> save_data

# This is NOT allowed (cycle)
task_a >> task_b >> task_a  ❌
```

**Visual Example**: Complex DAG with branching and trigger rules

![Airflow DAG with Trigger](https://airflow.apache.org/docs/apache-airflow/2.5.2/_images/branch_with_trigger.png)

_This shows how tasks can branch and converge based on conditions - a real workflow pattern you'll use._

---

### 2. Task

**What**: A single unit of work.

**Rules**:

- Does **one thing** (fetch data, NOT fetch + clean + save)
- Atomic (succeeds or fails completely)
- Idempotent (running twice = same result)

**Example**:

```python
fetch_user = PythonOperator(
    task_id='fetch_user',
    python_callable=fetch_user_function
)
```

**Task Dependencies Visual**:

```
Simple Linear:     task_a >> task_b >> task_c

Parallel:          task_a >> [task_b, task_c] >> task_d

Branching:         task_a >> branch_task
                           ├─> task_b >> join
                           └─> task_c >> join
```

**Task Dependencies Visual**:

```
Simple Linear:     task_a >> task_b >> task_c

Parallel:          task_a >> [task_b, task_c] >> task_d

Branching:         task_a >> branch_task
                           ├─> task_b >> join
                           └─> task_c >> join
```

---

### 3. Operator

**What**: A template that defines _how_ a task executes.

**Key Operators**:

| Operator               | Purpose             |
| ---------------------- | ------------------- |
| `PythonOperator`       | Run Python function |
| `BashOperator`         | Run shell command   |
| `EmailOperator`        | Send email          |
| `HttpOperator`         | Make API call       |
| `SparkSubmitOperator`  | Submit Spark job    |
| `KafkaProduceOperator` | Send to Kafka topic |

**Important**: Operator ≠ Task

- Operator = **class/template**
- Task = **instance of operator**

---

### 4. Executor

**What**: Determines **where and how** tasks run.

**Types**:

| Executor             | Use Case                         |
| -------------------- | -------------------------------- |
| `SequentialExecutor` | One task at a time (default/dev) |
| `LocalExecutor`      | Parallel on one machine          |
| `CeleryExecutor`     | Distributed across many workers  |
| `KubernetesExecutor` | Each task = Kubernetes pod       |

**Key Point**: Executor is **infrastructure**, not DAG logic. You change executor in `airflow.cfg`, not in your DAG code.

---

### 5. Scheduler

**What**: The brain that decides **when** DAGs run.

**Responsibilities**:

- Parse DAG files (every N seconds)
- Check if it's time to run (based on `schedule_interval`)
- Create DagRun instances
- Monitor task dependencies
- Handle retries
- Trigger downstream tasks

**Cron vs Airflow Scheduler**:

```
Cron:     "Run at 9 AM" → fires, doesn't care if previous run failed
Airflow:  "Run at 9 AM" → checks dependencies, retries, backfills, logs everything
```

---

### 6. DagRun

**What**: One execution instance of a DAG.

**Example**:

- DAG: `daily_sales_pipeline`
- DagRun 1: 2026-01-20 run
- DagRun 2: 2026-01-21 run

Each DagRun has:

- `execution_date` (logical timestamp)
- `state` (running/success/failed)
- `run_id` (unique identifier)

---

### 7. TaskInstance

**What**: One execution instance of a task within a DagRun.

**States**:

- `queued` → waiting for executor
- `running` → currently executing
- `success` → completed successfully
- `failed` → error occurred
- `up_for_retry` → will retry
- `skipped` → dependency failed

**State Transition Flow**:

```
     ┌──────────┐
     │  queued  │
     └────┬─────┘
          │
          ↓
     ┌──────────┐
     │ running  │
     └────┬─────┘
          │
          ├─────────────┬──────────────┐
          ↓             ↓              ↓
    ┌─────────┐   ┌──────────┐   ┌────────┐
    │ success │   │  failed  │   │skipped │
    └─────────┘   └────┬─────┘   └────────┘
                       │
                       ↓
                 ┌──────────────┐
                 │up_for_retry  │
                 └──────┬───────┘
                        │
                        ↓
                   (back to queued)
```

---

### 8. XCom (Cross-Communication)

**What**: A mechanism to pass small data between tasks.

**Usage**:

```python
# Task 1: Push data
def fetch_user(**context):
    data = {"name": "John"}
    context["ti"].xcom_push(key="user_data", value=data)

# Task 2: Pull data
def process_user(**context):
    data = context["ti"].xcom_pull(
        key="user_data",
        task_ids="fetch_user"
    )
```

**Visual Flow**:

```
┌───────────┐       XCom        ┌──────────────┐
│  Task A   │ ─────────────────>│ Metadata DB  │
│ (push)    │  {key: "result"}  │              │
└───────────┘                    └──────┬───────┘
                                        │
                                        │ pull
                                        ↓
                                 ┌──────────────┐
                                 │   Task B     │
                                 │  (pull)      │
                                 └──────────────┘
```

**Important**: XCom is for **metadata**, not large datasets.

- ✅ File path, record count, status flag
- ❌ 10GB DataFrame

---

### 9. Sensor

**What**: A special operator that **waits** for something to happen.

**Common Sensors**:

- `FileSensor` → Wait for file to exist
- `HttpSensor` → Wait for API to be healthy
- `TimeDeltaSensor` → Wait for time duration
- `ExternalTaskSensor` → Wait for another DAG

**Example**:

```python
wait_for_file = FileSensor(
    task_id='wait_for_data',
    filepath='/data/input.csv',
    poke_interval=30,  # Check every 30 seconds
    timeout=3600       # Give up after 1 hour
)
```

**How Sensors Work**:

```
Sensor Task Lifecycle:
┌──────────────────────────────────────────────┐
│                                              │
│  ┌──────┐   Check    ┌─────────┐            │
│  │Start │───────────>│ Exists? │            │
│  └──────┘            └────┬────┘            │
│                           │                 │
│               ┌───────No──┴────Yes───┐      │
│               │                      │      │
│               ↓                      ↓      │
│         ┌──────────┐           ┌─────────┐ │
│         │  Wait    │           │ Success │ │
│         │30 sec... │           └─────────┘ │
│         └────┬─────┘                        │
│              │                              │
│              │ (loop back)                  │
│              └───────────┐                  │
│                          │                  │
│                Timeout?  ↓                  │
│              Yes ───> ┌────────┐            │
│                       │ Failed │            │
│                       └────────┘            │
└──────────────────────────────────────────────┘
```

---

### 10. Hook

**What**: A reusable interface to external systems.

**Purpose**: Abstracts connection logic (credentials, retries, etc.)

**Example**:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

pg_hook = PostgresHook(postgres_conn_id='my_postgres')
records = pg_hook.get_records("SELECT * FROM users")
```

**Connection stored in Airflow UI** → Security best practice.

---

### 11. Connection

**What**: Stored credentials for external systems.

**Stored in**: Airflow metadata DB or secrets backend

**Created via**: UI or CLI

```bash
airflow connections add my_postgres \
  --conn-type postgres \
  --conn-host localhost \
  --conn-login user \
  --conn-password pass
```

---

### 12. Variables

**What**: Key-value store for dynamic configuration.

**Use Cases**:

- API keys
- Environment flags
- Thresholds

**Example**:

```python
from airflow.models import Variable

batch_size = Variable.get("batch_size", default_var=100)
```

---

### 13. Backfill

**What**: Run DAG for past date ranges.

**Command**:

```bash
airflow dags backfill \
  --start-date 2026-01-01 \
  --end-date 2026-01-15 \
  my_pipeline
```

**Use Case**: You deployed a new pipeline, but need historical data processed.

---

### 14. Catchup

**What**: Config that determines if Airflow runs missed schedules.

**Example**:

```python
with DAG(
    dag_id='my_dag',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False  # Don't run all missed dates since Jan 1
):
```

- `catchup=True` → Backfill automatically
- `catchup=False` → Only run from now onwards

---

### 15. Trigger Rule

**What**: Determines when a task should run based on upstream task states.

**Options**:

- `all_success` (default) → All upstream tasks succeeded
- `all_failed` → All upstream tasks failed
- `one_success` → At least one upstream succeeded
- `one_failed` → At least one upstream failed
- `none_failed` → No upstream tasks failed (skipped OK)

**Example**:

```python
cleanup = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_function,
    trigger_rule='all_done'  # Run regardless of success/failure
)
```

**Visual Comparison**:

**With Trigger (all_done)**:
![Branch with Trigger](https://airflow.apache.org/docs/apache-airflow/2.5.2/_images/branch_with_trigger.png)

**Without Trigger (default all_success)**:
![Branch without Trigger](https://airflow.apache.org/docs/apache-airflow/2.6.0/_images/branch_without_trigger.png)

_Notice how trigger rules affect which downstream tasks execute when branches have different outcomes._

---

### 16. Pool

**What**: Limit concurrent task execution.

**Use Case**: Prevent overloading external systems.

**Example**:

```bash
# Create pool with max 5 concurrent tasks
airflow pools set postgres_pool 5 "PostgreSQL connections"
```

```python
query_task = PythonOperator(
    task_id='query_db',
    python_callable=query_function,
    pool='postgres_pool'  # Max 5 of these tasks run concurrently
)
```

---

### 17. SLA (Service Level Agreement)

**What**: Expected task completion time.

**Example**:

```python
fetch_data = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_function,
    sla=timedelta(minutes=30)  # Should complete in 30 min
)
```

If SLA missed → Alert triggered.

---

## 🧩 How Airflow Fits Your Data Stack

| Tool                | Role                       |
| ------------------- | -------------------------- |
| **Kafka**           | Real-time event transport  |
| **Spark Streaming** | Continuous data processing |
| **Airflow**         | Orchestration & scheduling |

**Airflow coordinates**:

- Triggering Spark batch jobs
- Validating Bronze → Silver → Gold
- Scheduling ML training
- Monitoring pipeline health
- Backfilling historical data

**Visual: Complete Data Pipeline**

```
┌───────────────────────────────────────────────────────────────┐
│                    DATA PIPELINE ECOSYSTEM                     │
└───────────────────────────────────────────────────────────────┘

  REAL-TIME LAYER
  ┌─────────┐        ┌──────────────────┐       ┌────────────┐
  │ Kafka   │───────>│ Spark Streaming  │──────>│   Gold     │
  │ Events  │        │  (continuous)    │       │  Storage   │
  └─────────┘        └──────────────────┘       └────────────┘
                              ↑
                              │ trigger & monitor
                              │
                     ┌────────┴────────┐
                     │    AIRFLOW      │
                     │  (orchestrator) │
                     └────────┬────────┘
                              │
                              │ schedule & coordinate
                              ↓
  BATCH LAYER
  ┌─────────┐        ┌──────────────────┐       ┌────────────┐
  │ Bronze  │───────>│   Spark Batch    │──────>│   Silver   │
  │ Raw Data│        │ (hourly/daily)   │       │  Cleaned   │
  └─────────┘        └──────────────────┘       └────────────┘

                     AIRFLOW ORCHESTRATES:
                     • When batches run
                     • Data validation
                     • Retry logic
                     • Monitoring & alerts
```

**One sentence to remember**:

> **Kafka moves data, Spark thinks about data, Airflow decides _when_ things happen.**

---

## 🎯 Key Takeaways

1. **DAG = Blueprint** → Defines workflow logic
2. **Scheduler = Brain** → Decides when to run
3. **Executor = Hands** → Runs tasks (local/distributed)
4. **Metadata DB = Memory** → Tracks every execution
5. **UI = Eyes** → Visibility into pipeline health
6. **XCom = Messenger** → Pass data between tasks
7. **Operators = Templates** → Reusable task patterns

**Airflow never replaces Kafka or Spark. It coordinates them.**
