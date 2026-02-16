"""
Silver to Gold: Food Item Performance

This job calculates item-level performance metrics:
- Order and revenue metrics per item
- Click and conversion metrics
- Rankings and performance indicators
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct, avg,
    round as spark_round, current_timestamp, lit, when,
    row_number, percent_rank
)
from pyspark.sql.window import Window


def create_spark_session(app_name: str = "Silver_to_Gold_Item_Performance"):
    """Create Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.30") \
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


def read_silver_data(spark: SparkSession, sf_options: dict, process_date: str = None):
    """Read orders and clicks from Silver"""
    # Read orders
    orders_query = "(SELECT * FROM SILVER.ORDERS_CLEAN WHERE IS_VALID = TRUE"
    if process_date:
        orders_query += f" AND DATE_PARTITION = '{process_date}'"
    orders_query += ") as orders"
    
    orders_df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", orders_query) \
        .load()
    
    # Read clicks
    clicks_query = "(SELECT * FROM SILVER.CLICKS_CLEAN WHERE IS_VALID = TRUE"
    if process_date:
        clicks_query += f" AND DATE_PARTITION = '{process_date}'"
    clicks_query += ") as clicks"
    
    clicks_df = spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", clicks_query) \
        .load()
    
    return orders_df, clicks_df


def calculate_item_performance(orders_df, clicks_df):
    """Calculate item-level performance metrics"""
    # Aggregate orders by date and item
    order_metrics = orders_df.groupBy("DATE_PARTITION", "ITEM_ID").agg(
        count("ORDER_ID").alias("TOTAL_ORDERS"),
        spark_sum("PRICE").alias("TOTAL_REVENUE"),
        countDistinct("USER_ID").alias("UNIQUE_BUYERS"),
        avg("PRICE").alias("AVG_PRICE"),
        # Get item name and category (take first non-null)
        first("ITEM_NAME", ignorenulls=True).alias("ITEM_NAME"),
        first("ENRICHED_CATEGORY", ignorenulls=True).alias("CATEGORY")
    )
    
    # Aggregate clicks by date and item
    click_metrics = clicks_df.groupBy("DATE_PARTITION", "ITEM_ID").agg(
        count(when(col("EVENT_TYPE") == "click", 1)).alias("CLICK_COUNT"),
        count(when(col("EVENT_TYPE") == "impression", 1)).alias("IMPRESSION_COUNT")
    )
    
    # Join orders with clicks
    item_perf = order_metrics.join(
        click_metrics,
        on=["DATE_PARTITION", "ITEM_ID"],
        how="left"
    )
    
    # Fill nulls for items with no clicks
    item_perf = item_perf.fillna({"CLICK_COUNT": 0, "IMPRESSION_COUNT": 0})
    
    # Calculate conversion rates
    item_perf = item_perf.withColumn(
        "CONVERSION_RATE",
        spark_round(
            when(col("CLICK_COUNT") > 0, col("TOTAL_ORDERS") / col("CLICK_COUNT") * 100)
            .otherwise(None),
            2
        )
    ).withColumn(
        "CLICK_THROUGH_RATE",
        spark_round(
            when(col("IMPRESSION_COUNT") > 0, col("CLICK_COUNT") / col("IMPRESSION_COUNT") * 100)
            .otherwise(None),
            2
        )
    )
    
    # Round monetary values
    item_perf = item_perf.withColumn("TOTAL_REVENUE", spark_round(col("TOTAL_REVENUE"), 2)) \
        .withColumn("AVG_PRICE", spark_round(col("AVG_PRICE"), 2))
    
    # Add rankings (per date)
    window_by_date = Window.partitionBy("DATE_PARTITION").orderBy(col("TOTAL_REVENUE").desc())
    
    item_perf = item_perf.withColumn(
        "RANK_BY_REVENUE", row_number().over(window_by_date)
    )
    
    window_by_orders = Window.partitionBy("DATE_PARTITION").orderBy(col("TOTAL_ORDERS").desc())
    item_perf = item_perf.withColumn(
        "RANK_BY_ORDERS", row_number().over(window_by_orders)
    )
    
    window_by_conversion = Window.partitionBy("DATE_PARTITION").orderBy(col("CONVERSION_RATE").desc_nulls_last())
    item_perf = item_perf.withColumn(
        "RANK_BY_CONVERSION", row_number().over(window_by_conversion)
    )
    
    # Add performance indicators
    # Top seller = top 10% by revenue
    revenue_percentile_window = Window.partitionBy("DATE_PARTITION")
    item_perf = item_perf.withColumn(
        "revenue_percentile", percent_rank().over(
            Window.partitionBy("DATE_PARTITION").orderBy(col("TOTAL_REVENUE"))
        )
    )
    
    item_perf = item_perf.withColumn(
        "IS_TOP_SELLER", (col("revenue_percentile") >= 0.9).cast("boolean")
    )
    
    # Underperforming = high clicks but low conversion
    item_perf = item_perf.withColumn(
        "IS_UNDERPERFORMING",
        ((col("CLICK_COUNT") > 10) & (col("CONVERSION_RATE") < 5)).cast("boolean")
    )
    
    item_perf = item_perf.drop("revenue_percentile")
    
    # Rename DATE_PARTITION to DATE
    item_perf = item_perf.withColumnRenamed("DATE_PARTITION", "DATE")
    
    # Add metadata
    item_perf = item_perf.withColumn("LOAD_TIMESTAMP", current_timestamp()) \
        .withColumn("PROCESSING_VERSION", lit("v1.0"))
    
    return item_perf


def write_to_gold(df, sf_options: dict):
    """Write to Gold layer"""
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "GOLD.FOOD_ITEM_PERFORMANCE") \
        .mode("overwrite") \
        .save()


def main(process_date: str = None):
    """Main ETL job"""
    print(f"Starting Silver to Gold Item Performance ETL for date: {process_date or 'ALL'}")
    
    # Get environment variables
    import os
    from pyspark.sql.functions import first
    
    env_vars = {
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
    }
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        sf_options = get_snowflake_options(env_vars)
        
        # Read Silver data
        print("Reading SILVER.ORDERS_CLEAN and SILVER.CLICKS_CLEAN...")
        orders_df, clicks_df = read_silver_data(spark, sf_options, process_date)
        print(f"Read {orders_df.count()} orders and {clicks_df.count()} clicks")
        
        # Calculate item performance
        print("Calculating item performance metrics...")
        perf_df = calculate_item_performance(orders_df, clicks_df)
        
        # Write to Gold
        print("Writing to GOLD.FOOD_ITEM_PERFORMANCE...")
        write_to_gold(perf_df, sf_options)
        
        # Print summary
        print("\n📊 Top 10 Items by Revenue:")
        perf_df.orderBy(col("TOTAL_REVENUE").desc()).select(
            "DATE", "ITEM_NAME", "TOTAL_REVENUE", "TOTAL_ORDERS", "CONVERSION_RATE"
        ).show(10, truncate=False)
        
        print(f"\n✅ ETL Complete! Wrote {perf_df.count()} item records")
        
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
