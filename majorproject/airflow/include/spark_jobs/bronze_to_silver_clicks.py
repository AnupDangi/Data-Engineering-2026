"""
Bronze to Silver: Clicks Cleaning and Enrichment

This job:
1. Reads from BRONZE.CLICKS_RAW
2. Deduplicates using INGESTION_ID
3. Validates data quality
4. Enriches with food catalog metadata
5. Writes to SILVER.CLICKS_CLEAN
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, when, row_number, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import BooleanType


def create_spark_session(app_name: str = "Bronze_to_Silver_Clicks"):
    """Create Spark session with Snowflake connector"""
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


def read_bronze_clicks(spark: SparkSession, sf_options: dict, process_date: str = None):
    """Read clicks from Bronze layer"""
    query = "(SELECT * FROM BRONZE.CLICKS_RAW"
    
    if process_date:
        query += f" WHERE DATE(EVENT_TIMESTAMP) = '{process_date}'"
    
    query += ") as bronze_clicks"
    
    return spark.read \
        .format("snowflake") \
        .options(**sf_options) \
        .option("query", query) \
        .load()


def deduplicate_clicks(df):
    """Remove duplicate clicks based on INGESTION_ID"""
    window_spec = Window.partitionBy("INGESTION_ID").orderBy(col("EVENT_TIMESTAMP").desc())
    
    return df.withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("IS_DUPLICATE", (col("row_num") > 1).cast(BooleanType())) \
        .drop("row_num")


def validate_clicks(df):
    """Validate data quality and flag invalid records"""
    # Check for required fields
    df = df.withColumn(
        "has_required_fields",
        col("EVENT_ID").isNotNull() & 
        col("USER_ID").isNotNull() &
        col("EVENT_TYPE").isNotNull()
    )
    
    # Check timestamp validity
    df = df.withColumn(
        "has_valid_timestamp",
        col("EVENT_TIMESTAMP").isNotNull()
    )
    
    # Build validation errors
    df = df.withColumn(
        "VALIDATION_ERRORS",
        when(~col("has_required_fields"), "missing_required_fields")
        .when(~col("has_valid_timestamp"), "missing_timestamp")
        .otherwise("")
    )
    
    # Mark as valid
    df = df.withColumn(
        "IS_VALID",
        (col("VALIDATION_ERRORS") == "") & col("has_required_fields") & col("has_valid_timestamp")
    )
    
    return df.drop("has_required_fields", "has_valid_timestamp")


def transform_to_silver(df):
    """Transform clicks for Silver layer"""
    # Add date partition
    df = df.withColumn("DATE_PARTITION", to_date(col("EVENT_TIMESTAMP")))
    
    # Select and rename columns
    df = df.select(
        col("EVENT_ID"),
        col("USER_ID"),
        col("SESSION_ID"),
        col("ITEM_ID"),
        col("EVENT_TYPE"),
        col("EVENT_TIMESTAMP").alias("CLICK_TIMESTAMP"),
        col("DATE_PARTITION"),
        col("INGESTION_ID"),
        col("IS_DUPLICATE"),
        col("IS_VALID"),
        col("VALIDATION_ERRORS")
    )
    
    # Add metadata
    df = df.withColumn("SOURCE_TABLE", lit("BRONZE.CLICKS_RAW")) \
        .withColumn("LOAD_TIMESTAMP", current_timestamp()) \
        .withColumn("PROCESSING_VERSION", lit("v1.0"))
    
    # Note: Enrichment with item_name and category can be done in a subsequent job
    # by joining with SILVER.ORDERS_CLEAN or food catalog
    df = df.withColumn("ENRICHED_ITEM_NAME", lit(None).cast("string")) \
        .withColumn("ENRICHED_CATEGORY", lit(None).cast("string"))
    
    return df


def write_to_silver(df, sf_options: dict):
    """Write cleaned clicks to Silver layer"""
    df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "SILVER.CLICKS_CLEAN") \
        .mode("append") \
        .save()


def main(process_date: str = None):
    """Main ETL job"""
    print(f"Starting Bronze to Silver Clicks ETL for date: {process_date or 'ALL'}")
    
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
        # Get connection options
        sf_options = get_snowflake_options(env_vars)
        
        # Read source data
        print("Reading Bronze clicks...")
        bronze_df = read_bronze_clicks(spark, sf_options, process_date)
        print(f"Read {bronze_df.count()} clicks from Bronze")
        
        # Deduplicate
        print("Deduplicating clicks...")
        deduped_df = deduplicate_clicks(bronze_df)
        duplicate_count = deduped_df.filter(col("IS_DUPLICATE") == True).count()
        print(f"Found {duplicate_count} duplicate clicks")
        
        # Validate
        print("Validating data quality...")
        validated_df = validate_clicks(deduped_df)
        invalid_count = validated_df.filter(col("IS_VALID") == False).count()
        print(f"Found {invalid_count} invalid clicks")
        
        # Transform
        print("Transforming to Silver schema...")
        silver_df = transform_to_silver(validated_df)
        
        # Write to Silver
        print("Writing to SILVER.CLICKS_CLEAN...")
        write_to_silver(silver_df, sf_options)
        
        # Print summary
        total_written = silver_df.count()
        valid_written = silver_df.filter(col("IS_VALID") == True).count()
        print(f"\n✅ ETL Complete!")
        print(f"Total clicks written: {total_written}")
        print(f"Valid clicks: {valid_written}")
        print(f"Invalid clicks: {total_written - valid_written}")
        print(f"Duplicate clicks flagged: {duplicate_count}")
        
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
