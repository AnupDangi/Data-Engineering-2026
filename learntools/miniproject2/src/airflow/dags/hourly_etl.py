"""
Zomato Delivery ETL Pipeline
----------------------------
Hourly batch processing: Bronze → Silver → Gold

Using TaskFlow API with local Spark sessions.

Schedule: Every hour at :00
Catchup: Enabled (processes historical partitions)
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

# ============================================================================
# Configuration
# ============================================================================

# Data paths (via Docker mount at /opt/airflow/data_lake)
BRONZE_BASE = "/opt/airflow/data_lake"
SILVER_BASE = "/opt/airflow/data_lake"
GOLD_BASE = "/opt/airflow/data_lake"


def get_spark_session(app_name: str = "etl"):
    """Create a local Spark session."""
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()


# ============================================================================
# DAG Definition
# ============================================================================

@dag(
    dag_id="zomato_hourly_etl",
    description="30-minute ETL: Bronze → Silver → Gold",
    start_date=pendulum.datetime(2026, 1, 26, 14, tz="UTC"),
    schedule="*/30 * * * *",
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    tags=["zomato", "etl", "spark", "hourly"],
)

def zomato_etl():
    """
    Hourly ETL pipeline processing delivery events.
    
    Bronze → Silver: Deduplicate, clean, validate
    Silver → Gold: Aggregate metrics for analytics
    """
    
    @task
    def bronze_to_silver(**context) -> dict:
        """Transform raw bronze JSONL to clean silver Parquet."""
        from pyspark.sql.functions import (
            col, to_timestamp, when, lit,
            row_number, current_timestamp, trim
        )
        from pyspark.sql.window import Window
        from pyspark.sql.types import DoubleType, IntegerType
        import os
        
        # Get partition info
        data_interval_start = context['data_interval_start']
        year = data_interval_start.year
        month = data_interval_start.month
        day = data_interval_start.day
        hour = data_interval_start.hour
        
        bronze_path = f"{BRONZE_BASE}/bronze/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        silver_path = f"{SILVER_BASE}/silver/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        
        print(f"🚀 Bronze → Silver ETL for {year}-{month:02d}-{day:02d} {hour:02d}:00")
        print(f"📖 Reading from: {bronze_path}")
        
        # Check if bronze partition exists (producer might not have written data yet)
        if not os.path.exists(bronze_path):
            print(f"⚠️  Bronze partition not found: {bronze_path}")
            print("This is normal if producer hasn't written data for this time window yet.")
            return {
                "status": "skipped",
                "reason": "no_bronze_data",
                "bronze_count": 0,
                "silver_count": 0,
                "duplicates_removed": 0
            }
        
        # Create Spark session
        spark = get_spark_session("bronze_to_silver")
        
        try:
            # Safe cast helpers
            def safe_cast_int(column_name: str):
                return when(
                    (col(column_name).isNull()) | 
                    (trim(col(column_name)) == "") | 
                    (trim(col(column_name)).isin("NaN", "nan", "NULL", "null")),
                    lit(None)
                ).otherwise(col(column_name)).cast(IntegerType())
            
            def safe_cast_double(column_name: str):
                return when(
                    (col(column_name).isNull()) | 
                    (trim(col(column_name)) == "") | 
                    (trim(col(column_name)).isin("NaN", "nan", "NULL", "null")),
                    lit(None)
                ).otherwise(col(column_name)).cast(DoubleType())
            
            # Read bronze JSONL
            df = spark.read.json(bronze_path)
            original_count = df.count()
            print(f"✅ Loaded {original_count} records")
            
            # Deduplicate by event_id (keep latest)
            window = Window.partitionBy("event_id").orderBy(col("ingestion_time").desc())
            df_deduped = df.withColumn("row_num", row_number().over(window)) \
                .filter(col("row_num") == 1) \
                .drop("row_num")
            
            deduped_count = df_deduped.count()
            print(f"✅ Deduplicated: {original_count} → {deduped_count}")
            
            # Flatten and clean
            df_silver = df_deduped.select(
                col("event_id"),
                to_timestamp("event_time").alias("event_time"),
                to_timestamp("ingestion_time").alias("ingestion_time"),
                
                col("payload.ID").alias("order_id"),
                col("payload.Delivery_person_ID").alias("delivery_person_id"),
                safe_cast_int("payload.Delivery_person_Age").alias("delivery_person_age"),
                safe_cast_double("payload.Delivery_person_Ratings").alias("delivery_person_rating"),
                
                safe_cast_double("payload.Restaurant_latitude").alias("restaurant_lat"),
                safe_cast_double("payload.Restaurant_longitude").alias("restaurant_lon"),
                safe_cast_double("payload.Delivery_location_latitude").alias("delivery_lat"),
                safe_cast_double("payload.Delivery_location_longitude").alias("delivery_lon"),
                
                col("payload.Weather_conditions").alias("weather_conditions"),
                col("payload.Road_traffic_density").alias("road_traffic_density"),
                safe_cast_int("payload.Vehicle_condition").alias("vehicle_condition"),
                col("payload.Type_of_order").alias("order_type"),
                col("payload.Type_of_vehicle").alias("vehicle_type"),
                safe_cast_int("payload.multiple_deliveries").alias("multiple_deliveries"),
                col("payload.Festival").alias("is_festival"),
                col("payload.City").alias("city"),
                safe_cast_int("payload.Time_taken (min)").alias("delivery_time_minutes")
            ).withColumn(
                "is_valid",
                when(
                    (col("delivery_time_minutes").isNotNull()) &
                    (col("delivery_time_minutes") > 0) &
                    (col("delivery_time_minutes") < 300),
                    True
                ).otherwise(False)
            ).withColumn("processed_at", current_timestamp())
            
            # Write to silver
            df_silver.write.mode("overwrite").parquet(silver_path)
            
            final_count = df_silver.count()
            print(f"✅ Written {final_count} records to silver: {silver_path}")
            
            return {
                "bronze_count": original_count,
                "silver_count": final_count,
                "duplicates_removed": original_count - deduped_count
            }
        finally:
            spark.stop()
    
    @task
    def silver_to_gold(bronze_result: dict, **context) -> dict:
        """Aggregate silver data into gold metrics."""
        from pyspark.sql.functions import (
            col, avg, count, min as spark_min, max as spark_max,
            lit, current_timestamp
        )
        
        # Skip if bronze_to_silver was skipped
        if bronze_result.get('status') == 'skipped':
            print("⚠️  Skipping silver_to_gold (no bronze data was processed)")
            return {
                "status": "skipped",
                "reason": bronze_result.get('reason'),
                "gold_city_metrics": 0,
                "gold_vehicle_metrics": 0,
                "source_silver_count": 0
            }
        
        # Get partition info
        data_interval_start = context['data_interval_start']
        year = data_interval_start.year
        month = data_interval_start.month
        day = data_interval_start.day
        hour = data_interval_start.hour
        
        silver_path = f"{SILVER_BASE}/silver/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        gold_path = f"{GOLD_BASE}/gold/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"
        
        print(f"🚀 Silver → Gold ETL for {year}-{month:02d}-{day:02d} {hour:02d}:00")
        
        # Create Spark session
        spark = get_spark_session("silver_to_gold")
        
        try:
            # Read silver Parquet
            df = spark.read.parquet(silver_path)
            df_valid = df.filter(col("is_valid") == True)
            
            # Aggregate by city
            df_city = df_valid.groupBy("city").agg(
                count("*").alias("order_count"),
                avg("delivery_time_minutes").alias("avg_delivery_time"),
                spark_min("delivery_time_minutes").alias("min_delivery_time"),
                spark_max("delivery_time_minutes").alias("max_delivery_time"),
                avg("delivery_person_rating").alias("avg_rating")
            ).withColumn("metric_type", lit("by_city")) \
             .withColumn("processed_at", current_timestamp())
            
            # Aggregate by vehicle type
            df_vehicle = df_valid.groupBy("vehicle_type").agg(
                count("*").alias("order_count"),
                avg("delivery_time_minutes").alias("avg_delivery_time"),
                spark_min("delivery_time_minutes").alias("min_delivery_time"),
                spark_max("delivery_time_minutes").alias("max_delivery_time"),
                avg("delivery_person_rating").alias("avg_rating")
            ).withColumn("metric_type", lit("by_vehicle")) \
             .withColumn("city", lit(None).cast("string")) \
             .withColumn("processed_at", current_timestamp())
            
            # Write gold metrics (append to preserve historical data)
            df_city.write.mode("append").parquet(f"{gold_path}/by_city")
            df_vehicle.write.mode("append").parquet(f"{gold_path}/by_vehicle")
            
            city_count = df_city.count()
            vehicle_count = df_vehicle.count()
            
            print(f"✅ Gold metrics: {city_count} city rows, {vehicle_count} vehicle rows")
            print(f"✅ Written to: {gold_path}")
            
            return {
                "gold_city_metrics": city_count,
                "gold_vehicle_metrics": vehicle_count,
                "source_silver_count": bronze_result.get("silver_count", 0)
            }
        finally:
            spark.stop()
    
    @task
    def log_summary(gold_result: dict):
        """Print ETL summary."""
        print("=" * 60)
        print("📊 ETL Pipeline Complete!")
        print(f"   Gold city metrics:    {gold_result.get('gold_city_metrics', 0)}")
        print(f"   Gold vehicle metrics: {gold_result.get('gold_vehicle_metrics', 0)}")
        print(f"   Source records:       {gold_result.get('source_silver_count', 0)}")
        print("=" * 60)
    
    # Task dependencies
    bronze_result = bronze_to_silver()
    gold_result = silver_to_gold(bronze_result)
    log_summary(gold_result)


# Instantiate the DAG
dag = zomato_etl()
