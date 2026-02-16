"""
Silver to Gold: User Funnel Metrics

This job calculates daily conversion funnel:
- Sessions, impressions, clicks, orders
- Conversion rates at each stage
- User engagement metrics
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, round as spark_round,
    current_timestamp, lit, when, sum as spark_sum
)


def create_spark_session(app_name: str = "Silver_to_Gold_Funnel"):
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


def calculate_funnel_metrics(orders_df, clicks_df):
    """Calculate daily funnel metrics"""
    # Aggregate clicks by date
    click_metrics = clicks_df.groupBy("DATE_PARTITION").agg(
        countDistinct("SESSION_ID").alias("TOTAL_SESSIONS"),
        count(when(col("EVENT_TYPE") == "impression", 1)).alias("TOTAL_IMPRESSIONS"),
        count(when(col("EVENT_TYPE") == "click", 1)).alias("TOTAL_CLICKS"),
        countDistinct("USER_ID").alias("USERS_WITH_CLICKS")
    )
    
    # Aggregate orders by date
    order_metrics = orders_df.groupBy("DATE_PARTITION").agg(
        count("ORDER_ID").alias("TOTAL_ORDERS"),
        countDistinct("USER_ID").alias("USERS_WITH_ORDERS"),
        spark_sum("PRICE").alias("TOTAL_REVENUE")
    )
    
    # Join click and order metrics
    funnel = click_metrics.join(order_metrics, on="DATE_PARTITION", how="full_outer")
    
    # Fill nulls
    funnel = funnel.fillna({
        "TOTAL_SESSIONS": 0,
        "TOTAL_IMPRESSIONS": 0,
        "TOTAL_CLICKS": 0,
        "TOTAL_ORDERS": 0,
        "USERS_WITH_CLICKS": 0,
        "USERS_WITH_ORDERS": 0,
        "TOTAL_REVENUE": 0.0
    })
    
    # Calculate conversion rates
    funnel = funnel.withColumn(
        "IMPRESSION_TO_CLICK_RATE",
        spark_round(
            when(col("TOTAL_IMPRESSIONS") > 0, col("TOTAL_CLICKS") / col("TOTAL_IMPRESSIONS") * 100)
            .otherwise(0),
            2
        )
    ).withColumn(
        "CLICK_TO_ORDER_RATE",
        spark_round(
            when(col("TOTAL_CLICKS") > 0, col("TOTAL_ORDERS") / col("TOTAL_CLICKS") * 100)
            .otherwise(0),
            2
        )
    ).withColumn(
        "SESSION_TO_ORDER_RATE",
        spark_round(
            when(col("TOTAL_SESSIONS") > 0, col("TOTAL_ORDERS") / col("TOTAL_SESSIONS") * 100)
            .otherwise(0),
            2
        )
    )
    
    # Calculate engagement metrics
    funnel = funnel.withColumn(
        "AVG_CLICKS_PER_SESSION",
        spark_round(
            when(col("TOTAL_SESSIONS") > 0, col("TOTAL_CLICKS") / col("TOTAL_SESSIONS"))
            .otherwise(0),
            2
        )
    ).withColumn(
        "AVG_ORDERS_PER_SESSION",
        spark_round(
            when(col("TOTAL_SESSIONS") > 0, col("TOTAL_ORDERS") / col("TOTAL_SESSIONS"))
            .otherwise(0),
            2
        )
    ).withColumn(
        "AVG_REVENUE_PER_SESSION",
        spark_round(
            when(col("TOTAL_SESSIONS") > 0, col("TOTAL_REVENUE") / col("TOTAL_SESSIONS"))
            .otherwise(0),
            2
        )
    )
    
    # Calculate active users (users who clicked OR ordered)
    funnel = funnel.withColumn(
        "ACTIVE_USERS",
        col("USERS_WITH_CLICKS") + col("USERS_WITH_ORDERS")
    )
    
    # Rename DATE_PARTITION to DATE
    funnel = funnel.withColumnRenamed("DATE_PARTITION", "DATE")
    
    # Add metadata
    funnel = funnel.withColumn("LOAD_TIMESTAMP", current_timestamp()) \
        .withColumn("PROCESSING_VERSION", lit("v1.0"))
    
    # Select final columns
    funnel = funnel.select(
        "DATE",
        "TOTAL_SESSIONS",
        "TOTAL_IMPRESSIONS",
        "TOTAL_CLICKS",
        "TOTAL_ORDERS",
        "IMPRESSION_TO_CLICK_RATE",
        "CLICK_TO_ORDER_RATE",
        "SESSION_TO_ORDER_RATE",
        "AVG_CLICKS_PER_SESSION",
        "AVG_ORDERS_PER_SESSION",
        "AVG_REVENUE_PER_SESSION",
        "ACTIVE_USERS",
        "USERS_WITH_CLICKS",
        "USERS_WITH_ORDERS",
        "LOAD_TIMESTAMP",
        "PROCESSING_VERSION"
    )
    
    return funnel


def write_to_gold(df, sf_options: dict):
    """Write to Gold layer"""
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "GOLD.USER_FUNNEL_METRICS") \
        .mode("overwrite") \
        .save()


def main(process_date: str = None):
    """Main ETL job"""
    print(f"Starting Silver to Gold Funnel ETL for date: {process_date or 'ALL'}")
    
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
        
        # Read Silver data
        print("Reading SILVER data...")
        orders_df, clicks_df = read_silver_data(spark, sf_options, process_date)
        print(f"Read {orders_df.count()} orders and {clicks_df.count()} clicks")
        
        # Calculate funnel metrics
        print("Calculating funnel metrics...")
        funnel_df = calculate_funnel_metrics(orders_df, clicks_df)
        
        # Write to Gold
        print("Writing to GOLD.USER_FUNNEL_METRICS...")
        write_to_gold(funnel_df, sf_options)
        
        # Print summary
        print("\n📊 Funnel Metrics:")
        funnel_df.select(
            "DATE", "TOTAL_CLICKS", "TOTAL_ORDERS", 
            "CLICK_TO_ORDER_RATE", "AVG_REVENUE_PER_SESSION"
        ).show(10, truncate=False)
        
        print(f"\n✅ ETL Complete! Wrote {funnel_df.count()} daily funnel records")
        
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
