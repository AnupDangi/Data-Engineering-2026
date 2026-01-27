"""
Silver to Gold ETL Job
---------------------
Reads clean data from silver layer, performs aggregations and business logic,
and writes to gold layer for analytics.

Transformations:
- Aggregate delivery metrics by city, vehicle type, weather
- Calculate KPIs (avg delivery time, success rate, etc.)
- Create summary tables for dashboards
- Time-series rollups

Partitioning: By business dimension (city, metric type)
Format: Parquet (optimized for analytics queries)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum as spark_sum, min as spark_min, max as spark_max,
    when, lit, round as spark_round, current_timestamp, concat_ws
)


def create_spark_session(app_name: str = "silver_to_gold") -> SparkSession:
    """Create Spark session optimized for aggregations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def read_silver_data(spark: SparkSession, silver_path: str):
    """Read Parquet data from silver partition."""
    print(f"📖 Reading silver data from: {silver_path}")
    
    df = spark.read.parquet(silver_path)
    
    record_count = df.count()
    print(f"✅ Loaded {record_count} records from silver")
    
    return df


def calculate_delivery_metrics(df):
    """
    Calculate core delivery KPIs aggregated by dimensions.
    
    Dimensions: city, vehicle_type, weather_conditions, road_traffic_density
    Metrics:
    - Total deliveries
    - Avg delivery time
    - Min/Max delivery time
    - Avg delivery person rating
    - Success rate (valid deliveries)
    """
    print("🔧 Calculating delivery metrics...")
    
    # Filter to high-quality records only
    df_valid = df.filter(col("data_quality_score") >= 0.5)
    
    # Aggregate by city
    city_metrics = df_valid.groupBy("city").agg(
        count("*").alias("total_deliveries"),
        spark_round(avg("delivery_time_minutes"), 2).alias("avg_delivery_time"),
        spark_min("delivery_time_minutes").alias("min_delivery_time"),
        spark_max("delivery_time_minutes").alias("max_delivery_time"),
        spark_round(avg("delivery_person_rating"), 2).alias("avg_rating"),
        spark_round(
            (spark_sum(when(col("is_valid_delivery_time"), 1).otherwise(0)) / count("*") * 100),
            2
        ).alias("success_rate_pct")
    ).withColumn("metric_type", lit("city"))
    
    # Aggregate by vehicle type
    vehicle_metrics = df_valid.groupBy("vehicle_type").agg(
        count("*").alias("total_deliveries"),
        spark_round(avg("delivery_time_minutes"), 2).alias("avg_delivery_time"),
        spark_min("delivery_time_minutes").alias("min_delivery_time"),
        spark_max("delivery_time_minutes").alias("max_delivery_time"),
        spark_round(avg("delivery_person_rating"), 2).alias("avg_rating"),
        spark_round(
            (spark_sum(when(col("is_valid_delivery_time"), 1).otherwise(0)) / count("*") * 100),
            2
        ).alias("success_rate_pct")
    ).withColumnRenamed("vehicle_type", "city") \
     .withColumn("metric_type", lit("vehicle_type"))
    
    # Aggregate by weather conditions
    weather_metrics = df_valid.groupBy("weather_conditions").agg(
        count("*").alias("total_deliveries"),
        spark_round(avg("delivery_time_minutes"), 2).alias("avg_delivery_time"),
        spark_min("delivery_time_minutes").alias("min_delivery_time"),
        spark_max("delivery_time_minutes").alias("max_delivery_time"),
        spark_round(avg("delivery_person_rating"), 2).alias("avg_rating"),
        spark_round(
            (spark_sum(when(col("is_valid_delivery_time"), 1).otherwise(0)) / count("*") * 100),
            2
        ).alias("success_rate_pct")
    ).withColumnRenamed("weather_conditions", "city") \
     .withColumn("metric_type", lit("weather"))
    
    # Union all metrics
    all_metrics = city_metrics.union(vehicle_metrics).union(weather_metrics)
    
    # Add metadata
    all_metrics = all_metrics.withColumn(
        "calculated_at",
        current_timestamp()
    )
    
    print("✅ Metrics calculated")
    
    return all_metrics


def calculate_hourly_summary(df, year: int, month: int, day: int, hour: int):
    """
    Calculate hourly summary statistics.
    
    This provides a time-series view for trend analysis.
    """
    print("🔧 Calculating hourly summary...")
    
    summary = df.filter(col("data_quality_score") >= 0.5).agg(
        lit(year).alias("year"),
        lit(month).alias("month"),
        lit(day).alias("day"),
        lit(hour).alias("hour"),
        count("*").alias("total_deliveries"),
        spark_round(avg("delivery_time_minutes"), 2).alias("avg_delivery_time"),
        spark_round(avg("delivery_person_rating"), 2).alias("avg_rating"),
        count(when(col("is_festival") == "Yes", 1)).alias("festival_deliveries"),
        count(when(col("multiple_deliveries") > 1, 1)).alias("multi_delivery_count")
    ).withColumn("calculated_at", current_timestamp())
    
    print("✅ Hourly summary calculated")
    
    return summary


def write_gold_data(df, gold_path: str, partition_by: str = None):
    """
    Write aggregated data to gold layer.
    
    Format: Parquet
    Mode: Append (accumulate metrics over time)
    """
    print(f"💾 Writing gold data to: {gold_path}")
    
    # Ensure parent directory exists
    Path(gold_path).parent.mkdir(parents=True, exist_ok=True)
    
    writer = df.write.mode("append")
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.parquet(gold_path)
    
    record_count = df.count()
    print(f"✅ Wrote {record_count} records to gold layer")


def main():
    """
    Main ETL execution.
    
    Args (from Airflow):
        --year: Partition year
        --month: Partition month
        --day: Partition day
        --hour: Partition hour
        --silver-base: Base path for silver layer
        --gold-base: Base path for gold layer
    """
    parser = argparse.ArgumentParser(description="Silver to Gold ETL")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--day", type=int, required=True)
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument("--silver-base", type=str, default="/opt/airflow/data/data_storage")
    parser.add_argument("--gold-base", type=str, default="/opt/airflow/data/data_storage")
    
    args = parser.parse_args()
    
    # Build paths
    partition = f"year={args.year}/month={args.month:02d}/day={args.day:02d}/hour={args.hour:02d}"
    silver_path = f"{args.silver_base}/silver/{partition}"
    gold_base = f"{args.gold_base}/gold"
    
    print("=" * 80)
    print(f"🚀 Silver to Gold ETL Started")
    print("=" * 80)
    print(f"Partition: {partition}")
    print(f"Silver: {silver_path}")
    print(f"Gold: {gold_base}")
    print("=" * 80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Read silver data
        df_silver = read_silver_data(spark, silver_path)
        
        # Calculate metrics
        df_metrics = calculate_delivery_metrics(df_silver)
        df_hourly = calculate_hourly_summary(df_silver, args.year, args.month, args.day, args.hour)
        
        # Write to gold layer
        write_gold_data(df_metrics, f"{gold_base}/delivery_metrics", partition_by="metric_type")
        write_gold_data(df_hourly, f"{gold_base}/hourly_summary")
        
        # Show sample
        print("\n" + "=" * 80)
        print("📊 Sample Metrics")
        print("=" * 80)
        df_metrics.show(10, truncate=False)
        
        print("\n" + "=" * 80)
        print("✅ Silver to Gold ETL Completed Successfully")
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
