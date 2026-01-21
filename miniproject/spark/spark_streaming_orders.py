from pyspark.sql import SparkSession


## Create Spark Session with Kafka package included 
spark = SparkSession.builder \
    .appName("ZomatoRealTimeBrain") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

## kafka topic =unbounded table
## offset =row number 
## spark pulls incrementaly

# Read streaming data from Kafka topic 'zomato_orders'
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "zomato_orders") \
    .option("startingOffsets", "earliest") \
    .load()


### now raw data lake which is streamed.
## can we directly keep the raw data which is streamed into delta lake as bronze layer ??
## yes,
## but we have to make sure the data is should be in proper format


from pyspark.sql.functions import col, from_json,to_timestamp
from pyspark.sql.types import * 


schema = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("restaurant", StringType()),
    StructField("event_time", StringType())
])


## Now remember ETL pipeline
## Extract 

## parse the data into proper columns and data types with timestamp conversion
bronze = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select(
    col("data.*"),
    to_timestamp("event_time").alias("event_ts")
)

## bronze data lake write stream we require checkpointing for fault tolerance
## this bronze data can be kept to any storage system like S3,ADLS,HDFS,Local FS etc
bronze_query = bronze.writeStream \
    .format("json") \
    .option("path", "./data_lake/bronze/zomato_orders") \
    .option("checkpointLocation", "./checkpoints/bronze_orders") \
    .outputMode("append") \
    .start()


print("Spark Streaming Started - Reading from Kafka topic: zomato_orders")
print(" Writing Bronze layer to: ./data_lake/bronze/zomato_orders")

# Keep the streaming query running
try:
    bronze_query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping Spark Streaming...")
    bronze_query.stop()
    spark.stop()
    print("✅ Streaming stopped successfully")


## now lets do some transformations for silver layer 
## Add watermark + deduplication

# from pyspark.sql.functions import sum

## silver layer with watermark and deduplication
# silver=bronze \
#     .withWatermark("event_ts", "10 minutes") \
#     .dropDuplicates(["event_id"])


## let's write silver layer to ssilver data lake 
# silver_query = silver.writeStream \
#     .format("parquet") \
#     .option("path", "./data_lake/silver/zomato_orders") \
#     .option("checkpointLocation", "./checkpoints/silver_orders") \
#     .outputMode("append") \
#     .start()


# ## now gold layer aggregation for business analytics
# gold = silver \
#     .groupBy("restaurant") \
#     .agg(sum("amount").alias("total_revenue"))


# ## write the gold layer to console for demo purpose
# gold_query=gold.writeStream \
#     .format("console") \
#     .outputMode("complete")\
#     .start()