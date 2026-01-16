## Basics
import csv
import os
from pyspark.sql import SparkSession



## we create a spark session which is entrypoint with session name,master to use all the cores of cpu.
spark = SparkSession.builder\
    .appName("Basics")\
    .master("local[*]")\
    .getOrCreate()

## create the dataframe 
data = [
    (1, "anup", 23),
    (2, "Bishal", 24),
    (3, "Ram", 25)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# Show the dataframe
print("DataFrame created successfully:")
df.show()

# Print schema
print("\nDataFrame schema:")
df.printSchema()


## Transformations(lazy ,always lazy which means it will not execute until action is called)
adults_df=df.filter(df.age>=21)

names_df=adults_df.select("name")


## now we will perform action to see the result
names_df.show()

file_path=os.path.abspath("test.csv")
## testing by reading a actual csv 

df_csv=spark.read.option("header","true")\
    .option("inferSchema","true")\
    .csv(file_path)

## think you have to create a partiion which creates units of parallelism,units of failure recovery units of perfomance scoring
## by default spark creates 200 partitions
## this is used in big data when data is huge we can create more partitions to handle the data mostly think in distributed systems.
print(f"\nNumber of partitions: {df_csv.rdd.getNumPartitions()}")

## this creates a shuffle data moves across machnines networks and disks are involved.
print("\nGroupBy age count:")
df_csv.groupBy("age").count().show()

print("\nFull CSV data (first 20 rows):")
df_csv.show()

## lets use parquet format which is columnar storage format
## it is used for faster read and write operations it is compressed ,schema-aware,and industry-standard

df.write\
    .mode("overwrite")\
    .parquet("output/users_parquet")



## level 2 Spark on how it works
"""
    - Driver -plans the job
    - Executors - muscle (do the work)
    - Partitions - slices of data
    - Tasks - work units per partition
    """

## When we run 
df.groupBy("age").count()
## the driver creates a DAG of stages and tasks
## then it schedules tasks to executors
## executors process data in partitions and return results from all the partions it can be anwhere in the cluster 



## Rule 1: Transformationsn are cheap,Shuffles are expensive
"""
    Narrow Transformations 
    - select,filter,map,withColumn
    - data is not shuffled
    - executed in a single stage and fast
"""

"""
    Wide Transformations
    - groupBy,join,distinct,orderBy
    - shuffles=network + disk + path
    - multiple stages and slow

    To increase spark performance minimize shuffles
"""

## Rule 2: PARITIONS CONTROL PARALLELISM
"""
        df.rdd.getNumPartitions()
        - default is 200 partitions
        1 partition-> 1 task-> 1 CPU Core
"""

"""
    df.repartition(200)   # full shuffle (expensive)
    df.coalesce(20)       # reduce partitions (cheap)

    Increase partitions for large datasets to improve parallelism
    Decrease partitions for small datasets to reduce overhead
    Increases partions-> repartition (full shuffle)
    Reduce partions-> coalesce (no shuffle)
"""

## Rule 3: Joins Decide your fate
""" 
    Default join=shuffle join(slow)

    df1.join(df2,"user_id)

    Spark must: 
     - hash ,shuffle,match across machines

    Broadcast Join(fast)
    - small df broadcasted to all executors

    eg 
    from pyspark.sql.functions import broadcast
    df1.join(broadcast(df2),"user_id")
    
"""

## Rule 4: Caching is your friend,always use with your intent,not hope
"""
    df.cache()  # lazy caching
    df.persist() # different storage levels

    Cache when:
    - reusing datasets multiple times
    - iterative algorithms (ML,GraphX)

    Avoid caching when:
    - one-time use datasets
    - memory is limited

    - df.count() # action triggers caching

    Blind Caching equals to executor OOM
"""

## Rule 5: Spark SQL is not optional
"""
    Spark is secretly a SQL engine 

df.createOrReplaceTempView("users")

    spark.sql(query).show()

    Under the hood:
        - Catatyst Optimizer
        - Logical Plan -> Optimized Logical Plan -> Physical Plan   

    Real pipelines mix: 
    Pyspark DataFrame 
    Spark SQL
    Optimized joins
"""


## Rule 6: Storage Format == performance multiplier
""" 
    - Bronze: Raw ingestion,kafka dumps
    - Silver: cleaned,typed,dedeuplicated
    - Gold: aggregated,business-ready
    Use Parquet/ORC over CSV/JSON for speed and compression
    Partition data by frequently filtered columns
    Use bucketing for large joins
"""

""""
eg of datastorage
    Bronze: 
    df.write.mode("append").parquet("path/to/bronzedbmaybes3")
""" 


## Rule 7: Spark uses Structured Streaming for real-time data
"""
    Streaming is inifinite table
    Micro-batches process
    Same dataframe api

    eg:
    spark.readStream\
        .format("kafka")\
        .option("subscribe","events")\
        .load()

    in this way we can read from kafka topics
"""






# Stop the session
spark.stop()


