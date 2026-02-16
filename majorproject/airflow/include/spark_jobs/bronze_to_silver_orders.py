"""
Bronze to Silver: Orders Cleaning and Enrichment

This job:
1. Reads from BRONZE.ORDERS_RAW
2. Deduplicates using INGESTION_ID
3. Validates data quality (nulls, negative prices, etc.)
4. Enriches with food catalog metadata from PostgreSQL
5. Writes to SILVER.ORDERS_CLEAN
"""

import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, when, count, sum as spark_sum,
    row_number, current_timestamp, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType


def create_spark_session(app_name: str = "Bronze_to_Silver_Orders"):
    """Create Spark session with Snowflake connector"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.30") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
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


def get_postgres_options(env_vars: dict) -> dict:
    """Get PostgreSQL connection options for food catalog enrichment"""
    return {
        "url": f"jdbc:postgresql://{env_vars.get('POSTGRES_HOST', 'localhost')}:{env_vars.get('POSTGRES_PORT', '5432')}/{env_vars.get('POSTGRES_DB', 'food_catalog')}",
        "user": env_vars.get("POSTGRES_USER", "flowguard"),
        "password": env_vars.get("POSTGRES_PASSWORD", "flowguard123"),
        "driver": "org.postgresql.Driver"
    }


def read_bronze_orders(spark: SparkSession, sf_options: dict, process_date: str = None):
    """Read orders from Bronze layer"""
    query = "(SELECT * FROM BRONZE.ORDERS_RAW"
    
    if process_date:
        # Incremental load - only process today's data
        query += f" WHERE DATE(EVENT_TIMESTAMP) = '{process_date}'"
    
    query += ") as bronze_orders"
    
    return spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load()


def read_food_catalog(spark: SparkSession, pg_options: dict):
    """Read food items from PostgreSQL for enrichment"""
    try:
        return spark.read \
            .format("jdbc") \
            .options(**pg_options) \
            .option("dbtable", "food_items") \
            .load() \
            .select(
                col("item_id"),
                col("category").alias("enriched_category"),
                col("description").alias("enriched_description"),
                col("preparation_time").alias("enriched_preparation_time")
            )
    except Exception as e:
        print(f"Warning: Could not read food catalog: {e}")
        # Return empty DataFrame with expected schema if PostgreSQL is unavailable
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("item_id", StringType(), True),
            StructField("enriched_category", StringType(), True),
            StructField("enriched_description", StringType(), True),
            StructField("enriched_preparation_time", IntegerType(), True)
        ])
        return spark.createDataFrame([], schema)


def deduplicate_orders(df):
    """Remove duplicate orders based on INGESTION_ID"""
    # Use row_number() to keep the first occurrence
    window_spec = Window.partitionBy("INGESTION_ID").orderBy(col("EVENT_TIMESTAMP").desc())
    
    return df.withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("IS_DUPLICATE", (col("row_num") > 1).cast(BooleanType())) \
        .drop("row_num")


def validate_orders(df):
    """Validate data quality and flag invalid records"""
    validation_conditions = []
    
    # Check for required fields
    df = df.withColumn(
        "has_required_fields",
        col("ORDER_ID").isNotNull() & 
        col("USER_ID").isNotNull() & 
        col("ITEM_ID").isNotNull()
    )
    validation_conditions.append(
        when(~col("has_required_fields"), "missing_required_fields")
    )
    
    # Check price validity
    df = df.withColumn(
        "has_valid_price",
        col("PRICE").isNotNull() & (col("PRICE") > 0) & (col("PRICE") < 10000)
    )
    validation_conditions.append(
        when(~col("has_valid_price"), "invalid_price")
    )
    
    # Check timestamp validity
    df = df.withColumn(
        "has_valid_timestamp",
        col("EVENT_TIMESTAMP").isNotNull()
    )
    validation_conditions.append(
        when(~col("has_valid_timestamp"), "missing_timestamp")
    )
    
    # Combine all validation errors
    validation_expr = validation_conditions[0]
    for condition in validation_conditions[1:]:
        validation_expr = when(
            col("VALIDATION_ERRORS").isNotNull(),
            col("VALIDATION_ERRORS") + ", " + condition.otherwise("")
        ).otherwise(condition.otherwise(""))
    
    df = df.withColumn("VALIDATION_ERRORS", validation_expr)
    
    # Mark as valid if no errors
    df = df.withColumn(
        "IS_VALID",
        (col("VALIDATION_ERRORS") == "") & col("has_required_fields") & col("has_valid_price") & col("has_valid_timestamp")
    )
    
    return df.drop("has_required_fields", "has_valid_price", "has_valid_timestamp")


def transform_to_silver(df, catalog_df):
    """Transform and enrich orders for Silver layer"""
    # Add date partition
    df = df.withColumn("DATE_PARTITION", to_date(col("EVENT_TIMESTAMP")))
    
    # Rename and select columns
    df = df.select(
        col("ORDER_ID"),
        col("EVENT_ID"),
        col("USER_ID"),
        col("ITEM_ID"),
        col("ITEM_NAME"),
        col("PRICE"),
        col("STATUS"),
        col("EVENT_TIMESTAMP").alias("ORDER_TIMESTAMP"),
        col("DATE_PARTITION"),
        col("INGESTION_ID"),
        col("IS_DUPLICATE"),
        col("IS_VALID"),
        col("VALIDATION_ERRORS")
    )
    
    # Enrich with food catalog (left join to keep all orders)
    df = df.join(catalog_df, on="ITEM_ID", how="left")
    
    # Add metadata
    df = df.withColumn("SOURCE_TABLE", lit("BRONZE.ORDERS_RAW")) \
        .withColumn("LOAD_TIMESTAMP", current_timestamp()) \
        .withColumn("PROCESSING_VERSION", lit("v1.0"))
    
    return df


def write_to_silver(df, sf_options: dict):
    """Write cleaned orders to Silver layer"""
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "SILVER.ORDERS_CLEAN") \
        .mode("append") \
        .save()


def main(process_date: str = None):
    """Main ETL job"""
    print(f"Starting Bronze to Silver Orders ETL for date: {process_date or 'ALL'}")
    
    # Get environment variables
    import os
    env_vars = {
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "food_catalog"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "flowguard"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "flowguard123"),
    }
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Get connection options
        sf_options = get_snowflake_options(env_vars)
        pg_options = get_postgres_options(env_vars)
        
        # Read source data
        print("Reading Bronze orders...")
        bronze_df = read_bronze_orders(spark, sf_options, process_date)
        print(f"Read {bronze_df.count()} orders from Bronze")
        
        # Read food catalog for enrichment
        print("Reading food catalog...")
        catalog_df = read_food_catalog(spark, pg_options)
        print(f"Read {catalog_df.count()} items from catalog")
        
        # Deduplicate
        print("Deduplicating orders...")
        deduped_df = deduplicate_orders(bronze_df)
        duplicate_count = deduped_df.filter(col("IS_DUPLICATE") == True).count()
        print(f"Found {duplicate_count} duplicate orders")
        
        # Validate
        print("Validating data quality...")
        validated_df = validate_orders(deduped_df)
        invalid_count = validated_df.filter(col("IS_VALID") == False).count()
        print(f"Found {invalid_count} invalid orders")
        
        # Transform and enrich
        print("Transforming to Silver schema...")
        silver_df = transform_to_silver(validated_df, catalog_df)
        
        # Write to Silver
        print("Writing to SILVER.ORDERS_CLEAN...")
        write_to_silver(silver_df, sf_options)
        
        # Print summary statistics
        total_written = silver_df.count()
        valid_written = silver_df.filter(col("IS_VALID") == True).count()
        print(f"\n✅ ETL Complete!")
        print(f"Total orders written: {total_written}")
        print(f"Valid orders: {valid_written}")
        print(f"Invalid orders: {total_written - valid_written}")
        print(f"Duplicate orders flagged: {duplicate_count}")
        
        return 0
        
    except Exception as e:
        print(f"❌ ETL Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # Get process_date from command line args if provided
    process_date = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(main(process_date))
