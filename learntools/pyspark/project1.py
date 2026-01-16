## Goal: Learn execution plans, shuffles, partitions, skew, caching

"""
Problem 1: 
    A user-event analytics pipeline that looks simple but hides expensive operations.
    
    Raw Events (CSV) file for now 
    ↓
    Filter
    ↓
    GroupBy (⚠ shuffle)
    ↓
    Join with Users (⚠ shuffle / broadcast)
    ↓
    Aggregation
    ↓
    Parquet Output
"""
from pyspark.sql import SparkSession



##initalize spark session with config shuffle as 200 partitions as well test it later on 
spark = (
    SparkSession.builder
    .appName("Spark-Project-1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "200")  # default, we will change this later
    .getOrCreate()
)

## read the data from csv file think we are in broze level
## in practice we will read from data lake or blob storage which is stored as raw logs
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/ZomatoDataset.csv")
)
## lets checkthe shape and schema  
df.printSchema()
df.count()


## first transformation (cheap-> narrow)
## clean data by filtering the null value think basic ETL operation

clean_df = df.filter(
    df["Delivery_person_Age"].isNotNull() &
    df["Time_taken (min)"].isNotNull()
)

# nothing is expensive only narrow transformation


## first expensive operation
## but how??
## when we use groupBy causes shuffle operation across the cluster
## think you are grouping city data but from different partitions
## spark need to move data across the cluster to group them together
## this is expensive operation
city_avg=(
    clean_df.groupBy("City")
    .avg("Time_taken (min)")
)

## lets check the execution plan
## explain method shows how the query will be executed
## shows logical plan which is aggregate in this case
## shows physical plan which is shuffle hash aggregate
## shows the stages of execution
## before running any shuffle operation we can see the plan so that we dont write bad query
city_avg.explain(True)

## here we trigger the action means the operation of city_avg is executed 
## Steps of execution : 
## spark created shuffle files
## network + disk involved
## stage boundary created
## this is expensive operation you paid task laamo
city_avg.show()


clean_df.groupBy("City").count().orderBy("count", ascending=False).show()

## Second Expensive operation
## huge numbers of unique users
## 
person_avg = (
    clean_df
    .groupBy("Delivery_person_ID")
    .avg("Time_taken (min)")
)

person_avg.explain(True)


person_avg.count()

## observe partitions (silent performance killer)
get_no_partitions=clean_df.rdd.getNumPartitions()

print(f"Number of partitions: {get_no_partitions}") ## here you get 2 

## here you get 200 partitions remeber the partitions is created during shuffle operations on that dataframe
## we can set partitions per dataframe during shuffle operations

## here i got 1 during exection 

## we got to know Key point
""""
    AQE (Adaptive Query Execution)
    - dynamically coalesce shuffle partitions based on data size
    - avoids small tasks
    - improves performance
"""

print(f"city_avg partitions: {city_avg.rdd.getNumPartitions()}") 


## now set right-size of partitions (manually based on requirement or data size)

spark.conf.set("spark.sql.shuffle.partitions", "8")


city_avg_fixed = (
    clean_df
    .groupBy("City")
    .avg("Time_taken (min)")
)

city_avg_fixed.explain()
city_avg_fixed.show()

## here i got 1 during exection as per data size if your data is huge during execution it will create more partitions but not max than set value
print(f"city_avg_fixed partitions: {city_avg_fixed.rdd.getNumPartitions()}")


## Cache with intent 
## we reuse clean_df multiple times

## without cache every time it will recompute the dataframe from scratch
clean_df.cache()
print(f"Clean count: {clean_df.count()}")  ## action to materialize the cache


## if you ever need to optimize the output use parquet format but what it does??
## parquet is columnar storage format
## it is used for faster read and write operations it is compressed ,schema-aware,and industry-standard
## check the folder it created sql files and data files which is used for faster read and write operations
(
    city_avg_fixed
    .write
    .mode("overwrite")
    .parquet("output/city_delivery_metrics")
)

# phase 2: join-induced shuffles
# why spark jobs "randomly" OOM
# how broadcast joins actually work

print("-----------------------------Phase2 spark jobs--------------")
    
from pyspark.sql.functions import col

users_df = (
    clean_df
    .select(
        "Delivery_person_ID",
        "Delivery_person_Age",
        "Delivery_person_Ratings",
        "Vehicle_condition"
    )
    .dropDuplicates(["Delivery_person_ID"])
)

## checks for count
users_df.count()


## do the do the join without helping spark

joined_df=clean_df.join(
    users_df,
    on='Delivery_person_ID',
    how='inner'
)


#3 read teh execution plan
joined_df.explain(True)

joined_df.count()

## few key points here
"""
    - how to know your join is bad??
    - fact table: many rows dont try to join two fact tables
    - dimension table: small rows
    - default startegy is shuffle both sides

    Note: Never shuffle a small table if you can ship it. 
"""

## lets use broadcast join
## lets assume users_df is small or big

from pyspark.sql.functions import broadcast

optimized_join=clean_df.join(
    broadcast(users_df),
    on="Delivery_person_ID",
    how="inner"
)

optimized_join.explain(True)


## trigger the execution

count_optimized=optimized_join.count()

print(f"Optimized Join Count: {count_optimized}")

## if you want you can cache the users_df if you are reusing it multiple times

"""
| Situation       | Correct Strategy      |
| --------------- | --------------------- |
| Small dimension | Broadcast join        |
| Large ↔ large   | Partition + shuffle   |
| Skewed key      | Salting / repartition |
| Unknown sizes   | Inspect first         |
"""


## final aggregation on optimized_join table
final_metrics = (
    optimized_join
    .groupBy("City", clean_df["Type_of_vehicle"])
    .avg("Time_taken (min)")
)

final_metrics.explain()

final_metrics.show()


## now write the final output to parquet    
# (
#     final_metrics
#     .write
#     .mode("overwrite")
#     .partitionBy("City")
#     .parquet("output/delivery_metrics")
# )


## Phase3: Data Skew 

## Spark parallelism assumes work is evenly distributed across partitions 
## but here is a catch 

"""
    Data Skew: Uneven distribution of data across partitions
    - some partitions have significantly more data than others
    - leads to stragglers (slow tasks)
    - increases job completion time
"""

"""
    Problems Caused by Data Skew:
    - one key gets 70-80% of the data 
    - other tasks finish early 
    - cluster sits idle
    - job looks "Stuck"
"""


## Step1: Prove skew exists in your data
print("-----------------------------Phase3 Data Skew--------------")
(
    clean_df
    .groupBy("City")
    .count()
    .orderBy("count", ascending=False)
    .show(truncate=False)
)

city_metrics = (
    optimized_join
    .groupBy("City")
    .avg("Time_taken (min)")
)

city_metrics.explain(True)

## if the data was too big we can see the skew here
## active tasks shows some tasks taking too long to complete
city_metrics.show()


## if skew exists we have to manually fix it though we have small data we dont have any skew.
## if one key is dominant 
## all of its hash to the same reducer
## no parallelism 

## Note: Skew is a data problem,not a spark problem


from pyspark.sql.functions import spark_partition_id

(
    optimized_join
    .withColumn("pid", spark_partition_id())
    .groupBy("pid")
    .count()
    .orderBy("count", ascending=False)
    .show(20)
)

## we try to repartition based on city to fix lets see what happens
## this will still send to same one partition so it is expensive
optimized_join.repartition(50, "City")

## Salting technique to fix skew
from pyspark.sql.functions import rand,floor

salted_df = (
    optimized_join
    .withColumn("salt", floor(rand()*5))  # adding salt column with random number between 0-4
)

print("------------------Salted DF------------------")
salted_df.show(5)


partial_agg = (
    salted_df
    .groupBy("City", "salt")
    .avg("Time_taken (min)")
)

partial_agg.explain(True)

## final aggregation after merge salts

final_city_metrics = (
    partial_agg
    .groupBy("City")
    .avg("avg(Time_taken (min))")
)

final_city_metrics.explain(True)
final_city_metrics.show()


## why salting works??
"""     
    -Before salting, one key-> one reducer-> no parallelism
    -After salting, one key -> N sub-keys -> N reducers
    -Final merge is tiny
"""

spark.stop()

## project end
## Rule 1: Shuffles are expensive,avoid them
"""
    Shuffle: Data movement across the cluster
    - network + disk I/O
    - stage boundary
    - expensive operation

    Triggers for shuffle:
    - wide transformations (groupBy,join,distinct)
    - repartition
"""
## Rule 2: Partitions control parallelism
"""
    - 1 partition -> 1 task -> 1 CPU core
    - default shuffle partitions = 200
    - increase partitions for large datasets (improve parallelism)
    - decrease partitions for small datasets (reduce overhead)

    Methods:
    - repartition(n)   # full shuffle (expensive)
    - coalesce(n)     # reduce partitions (cheap)
"""
## Rule 3: Joins decide your fate
"""
    - default join = shuffle join (slow)
    - broadcast join for small dimension tables (fast)
    - avoid joining two large fact tables
    - use salting for skewed keys
"""
## Rule 4: Cache with intent
""" 
    - df.cache()  # lazy caching
    - df.persist() # different storage levels ## didnt learn this but also works 
"""
