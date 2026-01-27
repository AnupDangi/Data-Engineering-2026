"""
Bronze to Silver ETL Job
------------------------
Reads raw JSONL events from bronze layer, cleans and transforms data,
and writes to silver layer in Parquet format.

Transformations:
- Deduplicate by event_id
- Parse and validate timestamps
- Cast numeric fields to proper types
- Handle nulls and invalid values
- Flatten nested payload structure
- Add processing metadata

Partitioning: By ingestion time (year/month/day/hour)
Format: Parquet (columnar, compressed)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit,
    row_number, concat_ws, current_timestamp, trim
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType


def safe_cast_int(column_name: str) -> col:
    """Safely cast to integer, handling 'NaN' strings and nulls."""
    return when(
        (col(column_name).isNull()) | 
        (trim(col(column_name)) == "") | 
        (trim(col(column_name)).isin("NaN", "nan", "NULL", "null")),
        lit(None)
    ).otherwise(col(column_name)).cast(IntegerType())


def safe_cast_double(column_name: str) -> col:
    """Safely cast to double, handling 'NaN' strings and nulls."""
    return when(
        (col(column_name).isNull()) | 
        (trim(col(column_name)) == "") | 
        (trim(col(column_name)).isin("NaN", "nan", "NULL", "null")),
        lit(None)
    ).otherwise(col(column_name)).cast(DoubleType())


def create_spark_session(app_name: str = "bronze_to_silver") -> SparkSession:
    """Create Spark session optimized for local processing."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def read_bronze_data(spark: SparkSession, bronze_path: str):
    """
    Read JSONL files from bronze partition.
    
    Args:
        spark: SparkSession
        bronze_path: Full path to bronze partition (includes year/month/day/hour)
    
    Returns:
        DataFrame with bronze data
    """
    print(f"📖 Reading bronze data from: {bronze_path}")
    
    # Read JSONL (newline-delimited JSON)
    df = spark.read.json(bronze_path)
    
    record_count = df.count()
    print(f"✅ Loaded {record_count} records from bronze")
    
    return df


def deduplicate_events(df):
    """
    Deduplicate by event_id, keeping the latest ingestion_time.
    
    This handles at-least-once delivery from bronze writer.
    """
    print("🔧 Deduplicating events by event_id...")
    
    # Window partitioned by event_id, ordered by ingestion_time desc
    window = Window.partitionBy("event_id").orderBy(col("ingestion_time").desc())
    
    # Keep only the first (latest) occurrence
    df_deduped = df.withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    original_count = df.count()
    deduped_count = df_deduped.count()
    duplicates = original_count - deduped_count
    
    print(f"✅ Removed {duplicates} duplicate events ({original_count} → {deduped_count})")
    
    return df_deduped


def transform_and_clean(df):
    """
    Apply transformations and data quality rules.
    
    Transformations:
    - Flatten payload into top-level columns
    - Cast numeric fields to proper types
    - Parse timestamps
    - Handle missing/invalid values
    - Add derived fields
    """
    print("🔧 Transforming and cleaning data...")
    
    # Flatten payload structure with safe casting
    df_flat = df.select(
        col("event_id"),
        to_timestamp("event_time").alias("event_time"),
        to_timestamp("ingestion_time").alias("ingestion_time"),
        
        # Flatten payload
        col("payload.ID").alias("order_id"),
        col("payload.Delivery_person_ID").alias("delivery_person_id"),
        safe_cast_int("payload.Delivery_person_Age").alias("delivery_person_age"),
        safe_cast_double("payload.Delivery_person_Ratings").alias("delivery_person_rating"),
        
        # Coordinates
        safe_cast_double("payload.Restaurant_latitude").alias("restaurant_lat"),
        safe_cast_double("payload.Restaurant_longitude").alias("restaurant_lon"),
        safe_cast_double("payload.Delivery_location_latitude").alias("delivery_lat"),
        safe_cast_double("payload.Delivery_location_longitude").alias("delivery_lon"),
        
        # Order details
        col("payload.Order_Date").alias("order_date_raw"),
        col("payload.Time_Orderd").alias("time_ordered_raw"),
        col("payload.Time_Order_picked").alias("time_picked_raw"),
        
        # Contextual data
        col("payload.Weather_conditions").alias("weather_conditions"),
        col("payload.Road_traffic_density").alias("road_traffic_density"),
        safe_cast_int("payload.Vehicle_condition").alias("vehicle_condition"),
        col("payload.Type_of_order").alias("order_type"),
        col("payload.Type_of_vehicle").alias("vehicle_type"),
        safe_cast_int("payload.multiple_deliveries").alias("multiple_deliveries"),
        col("payload.Festival").alias("is_festival"),
        col("payload.City").alias("city"),
        
        # Target variable
        safe_cast_int("payload.Time_taken (min)").alias("delivery_time_minutes")
    )
    
    # Add data quality flags
    df_cleaned = df_flat.withColumn(
        "is_valid_coordinates",
        when(
            (col("restaurant_lat").isNotNull()) &
            (col("restaurant_lon").isNotNull()) &
            (col("delivery_lat").isNotNull()) &
            (col("delivery_lon").isNotNull()),
            True
        ).otherwise(False)
    ).withColumn(
        "is_valid_delivery_time",
        when(
            (col("delivery_time_minutes").isNotNull()) &
            (col("delivery_time_minutes") > 0) &
            (col("delivery_time_minutes") < 300),  # < 5 hours seems reasonable
            True
        ).otherwise(False)
    )
    
    # Add processing metadata
    df_final = df_cleaned.withColumn(
        "processed_at",
        current_timestamp()
    ).withColumn(
        "data_quality_score",
        when(col("is_valid_coordinates") & col("is_valid_delivery_time"), 1.0)
        .when(col("is_valid_coordinates") | col("is_valid_delivery_time"), 0.5)
        .otherwise(0.0)
    )
    
    print(f"✅ Transformation complete")
    
    return df_final


def write_silver_data(df, silver_path: str):
    """
    Write transformed data to silver layer.
    
    Format: Parquet (columnar, compressed)
    Mode: Overwrite (idempotent)
    Partitioning: Already partitioned by execution time
    """
    print(f"💾 Writing silver data to: {silver_path}")
    
    # Ensure parent directory exists
    Path(silver_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Write as Parquet with compression
    df.write \
        .mode("overwrite") \
        .parquet(silver_path)
    
    record_count = df.count()
    print(f"✅ Wrote {record_count} records to silver layer")


def main():
    """
    Main ETL execution.
    
    Args (from Airflow):
        --year: Partition year
        --month: Partition month
        --day: Partition day
        --hour: Partition hour
        --bronze-base: Base path for bronze layer
        --silver-base: Base path for silver layer
    """
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument("--bronze-base", type=str, default="/opt/airflow/data/data_storage")
    parser.add_argument("--silver-base", type=str, default="/opt/airflow/data/data_storage")
    
    args = parser.parse_args()
    
    # Build partition paths
    partition = f"year={args.year}/month={args.month:02d}/day={args.day:02d}/hour={args.hour:02d}"
    bronze_path = f"{args.bronze_base}/bronze/{partition}"
    silver_path = f"{args.silver_base}/silver/{partition}"
    
    print("=" * 80)
    print(f"🚀 Bronze to Silver ETL Started")
    print("=" * 80)
    print(f"Partition: {partition}")
    print(f"Bronze: {bronze_path}")
    print(f"Silver: {silver_path}")
    print("=" * 80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # ETL Pipeline
        df_bronze = read_bronze_data(spark, bronze_path)
        df_deduped = deduplicate_events(df_bronze)
        df_silver = transform_and_clean(df_deduped)
        write_silver_data(df_silver, silver_path)
        
        # Summary stats
        print("\n" + "=" * 80)
        print("📊 Data Quality Summary")
        print("=" * 80)
        df_silver.groupBy("data_quality_score").count().show()
        
        print("=" * 80)
        print("✅ Bronze to Silver ETL Completed Successfully")
        print("=" * 80)
        
        spark.stop()
        sys.exit(0)
        
    except Exception as e:
        print(f"\n❌ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
