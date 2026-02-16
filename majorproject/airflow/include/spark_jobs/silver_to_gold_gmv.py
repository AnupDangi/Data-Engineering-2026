"""
Silver to Gold: Daily GMV Metrics

This job calculates daily revenue and order metrics:
- Total revenue, order count
- Unique users, avg order value
- Growth metrics vs previous day
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg, 
    lag, round as spark_round, current_timestamp, lit
)
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "Silver_to_Gold_GMV"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.30") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def get_snowflake_options(env_vars: dict) -> dict:
    """Get Snowflake connection options"""
    return {
        "sfURL": f"{env_vars['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": env_vars["SNOWFLAKE_USER"],
        "sfPassword": env_vars["SNOWFLAKE_PASSWORD"],
        "sfDatabase": "FLOWGUARD_DB",
        "sfWarehouse": "COMPUTE_WH",
    }


def read_silver_orders(spark: SparkSession, sf_options: dict, process_date: str = None):
    """Read cleaned orders from Silver layer"""
    query = "(SELECT * FROM SILVER.ORDERS_CLEAN WHERE IS_VALID = TRUE"
    
    if process_date:
        query += f" AND DATE_PARTITION = '{process_date}'"
    
    query += ") as silver_orders"
    
    return spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load()


def calculate_daily_gmv(df):
    """Calculate daily GMV metrics"""
    # Aggregate by date
    daily_metrics = df.groupBy("DATE_PARTITION").agg(
        spark_sum("PRICE").alias("TOTAL_REVENUE"),
        count("ORDER_ID").alias("ORDER_COUNT"),
        count("ITEM_ID").alias("TOTAL_ITEMS_SOLD"),
        countDistinct("USER_ID").alias("UNIQUE_USERS"),
        avg("PRICE").alias("AVG_ORDER_VALUE")
    )
    
    # Round monetary values
    daily_metrics = daily_metrics.withColumn(
        "TOTAL_REVENUE", spark_round(col("TOTAL_REVENUE"), 2)
    ).withColumn(
        "AVG_ORDER_VALUE", spark_round(col("AVG_ORDER_VALUE"), 2)
    )
    
    # Calculate growth metrics (vs previous day)
    window_spec = Window.orderBy("DATE_PARTITION")
    
    daily_metrics = daily_metrics.withColumn(
        "prev_revenue", lag("TOTAL_REVENUE", 1).over(window_spec)
    ).withColumn(
        "prev_orders", lag("ORDER_COUNT", 1).over(window_spec)
    )
    
    daily_metrics = daily_metrics.withColumn(
        "REVENUE_GROWTH_PCT",
        spark_round(
            when(
                col("prev_revenue").isNotNull() & (col("prev_revenue") > 0),
                ((col("TOTAL_REVENUE") - col("prev_revenue")) / col("prev_revenue")) * 100
            ).otherwise(None),
            2
        )
    ).withColumn(
        "ORDER_GROWTH_PCT",
        spark_round(
            when(
                col("prev_orders").isNotNull() & (col("prev_orders") > 0),
                ((col("ORDER_COUNT") - col("prev_orders")) / col("prev_orders")) * 100
            ).otherwise(None),
            2
        )
    )
    
    # Drop temporary columns
    daily_metrics = daily_metrics.drop("prev_revenue", "prev_orders")
    
    # Rename DATE_PARTITION to DATE
    daily_metrics = daily_metrics.withColumnRenamed("DATE_PARTITION", "DATE")
    
    # Add metadata
    daily_metrics = daily_metrics.withColumn(
        "LOAD_TIMESTAMP", current_timestamp()
    ).withColumn(
        "PROCESSING_VERSION", lit("v1.0")
    )
    
    # Add placeholder columns for user segmentation (will be calculated in user cohort job)
    daily_metrics = daily_metrics.withColumn("NEW_USERS", lit(None).cast("int")) \
        .withColumn("RETURNING_USERS", lit(None).cast("int"))
    
    return daily_metrics


def write_to_gold(df, sf_options: dict, table_name: str):
    """Write to Gold layer"""
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", f"GOLD.{table_name}") \
        .mode("overwrite") \
        .save()


def main(process_date: str = None):
    """Main ETL job"""
    print(f"Starting Silver to Gold GMV ETL for date: {process_date or 'ALL'}")
    
    # Get environment variables
    import os
    env_vars = {
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    }
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        sf_options = get_snowflake_options(env_vars)
        
        # Read Silver orders
        print("Reading SILVER.ORDERS_CLEAN...")
        orders_df = read_silver_orders(spark, sf_options, process_date)
        print(f"Read {orders_df.count()} valid orders")
        
        # Calculate daily GMV metrics
        print("Calculating daily GMV metrics...")
        gmv_df = calculate_daily_gmv(orders_df)
        
        # Write to Gold
        print("Writing to GOLD.DAILY_GMV_METRICS...")
        write_to_gold(gmv_df, sf_options, "DAILY_GMV_METRICS")
        
        # Print summary
        gmv_df.show(10, truncate=False)
        print(f"\n✅ ETL Complete! Wrote {gmv_df.count()} daily records")
        
        return 0
        
    except Exception as e:
        print(f"❌ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    process_date = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(main(process_date))
