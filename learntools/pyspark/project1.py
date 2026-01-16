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

## nothing is expensive only narrow transformation


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


