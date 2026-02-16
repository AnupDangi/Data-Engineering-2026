"""
FlowGuard Daily ETL Pipeline

This DAG orchestrates the Bronze -> Silver -> Gold ETL pipeline:

Bronze -> Silver (Cleaning):
- Orders: Deduplicate, validate, enrich with catalog
- Clicks: Deduplicate, validate

Silver -> Gold (Aggregation):
- Daily GMV metrics
- Food item performance
- User funnel analysis

Schedule: Daily at 1:00 AM UTC
Backfill: Supported via execution_date
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime as pendulum_datetime
import os


# Default arguments for all tasks
default_args = {
    "owner": "FlowGuard",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="flowguard_daily_etl",
    description="Daily ETL pipeline: Bronze -> Silver -> Gold",
    start_date=pendulum_datetime(2026, 2, 1, tz="UTC"),
    schedule="0 1 * * *",  # Daily at 1 AM UTC
    catchup=True,  # Enable backfill
    max_active_runs=1,
    default_args=default_args,
    tags=["flowguard", "etl", "spark", "daily"],
    doc_md=__doc__,
)
def flowguard_daily_etl():
    
    # ========================================================================
    # BRONZE -> SILVER: Data Cleaning
    # ========================================================================
    
    bronze_to_silver_orders = SparkSubmitOperator(
        task_id="bronze_to_silver_orders",
        application="/usr/local/airflow/include/spark_jobs/bronze_to_silver_orders.py",
        name="bronze_to_silver_orders",
        conn_id="spark_default",
        application_args=["{{ ds }}"],  # Pass execution date (YYYY-MM-DD)
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.sql.adaptive.enabled": "true",
        },
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "host.docker.internal"),
            "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "food_catalog"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "flowguard"),
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "flowguard123"),
        },
        verbose=True,
    )
    
    bronze_to_silver_clicks = SparkSubmitOperator(
        task_id="bronze_to_silver_clicks",
        application="/usr/local/airflow/include/spark_jobs/bronze_to_silver_clicks.py",
        name="bronze_to_silver_clicks",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
        },
        verbose=True,
    )
    
    # ========================================================================
    # SILVER -> GOLD: Business Metrics
    # ========================================================================
    
    silver_to_gold_gmv = SparkSubmitOperator(
        task_id="silver_to_gold_gmv",
        application="/usr/local/airflow/include/spark_jobs/silver_to_gold_gmv.py",
        name="silver_to_gold_gmv",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
        },
        verbose=True,
    )
    
    silver_to_gold_item_performance = SparkSubmitOperator(
        task_id="silver_to_gold_item_performance",
        application="/usr/local/airflow/include/spark_jobs/silver_to_gold_item_performance.py",
        name="silver_to_gold_item_performance",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
        },
        verbose=True,
    )
    
    silver_to_gold_funnel = SparkSubmitOperator(
        task_id="silver_to_gold_funnel",
        application="/usr/local/airflow/include/spark_jobs/silver_to_gold_funnel.py",
        name="silver_to_gold_funnel",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
        },
        env_vars={
            "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", ""),
        },
        verbose=True,
    )
    
    # ========================================================================
    # Data Quality Checks
    # ========================================================================
    
    @task
    def validate_silver_data(**context):
        """Validate that Silver data was created successfully"""
        import snowflake.connector
        
        ds = context["ds"]
        
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse="COMPUTE_WH",
            database="FLOWGUARD_DB",
            schema="SILVER"
        )
        
        cursor = conn.cursor()
        
        # Check orders count
        cursor.execute(f"SELECT COUNT(*) FROM ORDERS_CLEAN WHERE DATE_PARTITION = '{ds}'")
        orders_count = cursor.fetchone()[0]
        
        # Check clicks count
        cursor.execute(f"SELECT COUNT(*) FROM CLICKS_CLEAN WHERE DATE_PARTITION = '{ds}'")
        clicks_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Silver validation passed:")
        print(f"   Orders: {orders_count}")
        print(f"   Clicks: {clicks_count}")
        
        if orders_count == 0 and clicks_count == 0:
            raise ValueError("No data found in Silver layer for this date")
        
        return {"orders": orders_count, "clicks": clicks_count}
    
    @task
    def validate_gold_data(**context):
        """Validate that Gold metrics were calculated"""
        import snowflake.connector
        
        ds = context["ds"]
        
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse="COMPUTE_WH",
            database="FLOWGUARD_DB",
            schema="GOLD"
        )
        
        cursor = conn.cursor()
        
        # Check GMV metrics
        cursor.execute(f"SELECT TOTAL_REVENUE, ORDER_COUNT FROM DAILY_GMV_METRICS WHERE DATE = '{ds}'")
        gmv_data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if gmv_data:
            print(f"✅ Gold validation passed:")
            print(f"   Daily Revenue: ${gmv_data[0]:,.2f}")
            print(f"   Order Count: {gmv_data[1]}")
        else:
            raise ValueError("No GMV metrics found for this date")
        
        return {"revenue": gmv_data[0], "orders": gmv_data[1]}
    
    @task
    def send_success_notification(**context):
        """Send success notification (placeholder for email/Slack)"""
        ds = context["ds"]
        print(f"🎉 ETL pipeline completed successfully for {ds}")
        print(f"📊 Check Snowflake GOLD schema for metrics")
        return True
    
    # ========================================================================
    # Define Task Dependencies
    # ========================================================================
    
    # Bronze -> Silver (can run in parallel)
    validate_silver = validate_silver_data()
    [bronze_to_silver_orders, bronze_to_silver_clicks] >> validate_silver
    
    # Silver -> Gold (can run in parallel after validation)
    validate_gold = validate_gold_data()
    validate_silver >> [silver_to_gold_gmv, silver_to_gold_item_performance, silver_to_gold_funnel] >> validate_gold
    
    # Final notification
    notification = send_success_notification()
    validate_gold >> notification


# Instantiate the DAG
flowguard_etl_dag = flowguard_daily_etl()
