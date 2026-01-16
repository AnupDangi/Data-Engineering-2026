"""
Project 2: The Join Wars
========================
Core Question: Why do joins that worked yesterday suddenly OOM today?

Scenario:
deliveries   (100M rows)
   ⨝
drivers      (5M rows)
   ⨝
vehicles     (3M rows)

Problem: All joins on keys, all tables "reasonable" - but everything breaks.
Why? Because Spark must decide where data lives in memory.
"""

## Wait read this article first  to understand join strategies visually and deeply 
## https://medium.com/@amarkrgupta96/join-strategies-in-apache-spark-a-hands-on-approach-d0696fc0a6c9
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count, sum as _sum

## Initialize Spark with memory monitoring
spark = (
    SparkSession.builder
    .appName("Project-2-Join-Wars")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB default
    .config("spark.sql.cbo.enabled", "true")  # Cost-based optimizer
    .getOrCreate()
)

## =============================================================================
## 1️⃣ THREE JOIN STRATEGIES (Physical Execution)
## =============================================================================

"""
a) BroadcastHashJoin
   - One table fully copied to EVERY executor
   - Fast: no shuffle
   - Memory hungry: table × num_executors
   - Safe only if table is small AND stays small

b) ShuffleHashJoin
   - Both tables shuffled by join key
   - One side hashed per partition
   - Moderate memory pressure
   - Good for medium-sized tables

c) SortMergeJoin
   - Both sides shuffled
   - Both sides sorted
   - Disk + network heavy
   - Safest for very large tables
   - Spark's default when unsure
"""

## Let's simulate with Zomato data
deliveries_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/ZomatoDataset.csv")
)

## Create dimension tables (simulating drivers and vehicles)
drivers_df = (
    deliveries_df
    .select("Delivery_person_ID", "Delivery_person_Age", "Delivery_person_Ratings")
    .dropDuplicates(["Delivery_person_ID"])
)

vehicles_df = (
    deliveries_df
    .select("Delivery_person_ID", "Type_of_vehicle", "Vehicle_condition")
    .dropDuplicates(["Delivery_person_ID"])
)

print(f"Deliveries: {deliveries_df.count()} rows")
print(f"Drivers: {drivers_df.count()} rows")
print(f"Vehicles: {vehicles_df.count()} rows")


## =============================================================================
## 2️⃣ THE SILENT KILLER: Multiple Broadcasts Composing Badly
## =============================================================================

"""
Key Level-3 Truth: Broadcast joins compose badly.

What happens:
    deliveries ⨝ drivers  → broadcast drivers (8MB in memory)
    result     ⨝ vehicles → broadcast vehicles (5MB in memory)

Each executor holds:
    - drivers hash table (8MB)
    - vehicles hash table (5MB)
    - JVM overhead (~30%)
    - Task buffers
    
Total memory per executor: 8 + 5 + overhead = 17MB+ per executor
Multiply by concurrent tasks = BOOM 💥
"""

## Example: Double broadcast (dangerous in production)
print("\n=== DANGEROUS: Double Broadcast Pattern ===")

result_double_broadcast = (
    deliveries_df
    .join(broadcast(drivers_df), "Delivery_person_ID")
    .join(broadcast(vehicles_df), "Delivery_person_ID")
)

## Check the plan - you'll see TWO BroadcastExchange nodes
result_double_broadcast.explain()

## This works on small data but explodes with scale
# result_double_broadcast.count()


## =============================================================================
## 3️⃣ BROADCAST FAILURE MODES (The Three Silent Killers)
## =============================================================================

"""
❌ Failure Mode 1: Table Grows Silently
    Yesterday: 8 MB → broadcast works
    Today: 25 MB → crosses threshold, Spark switches to shuffle
    You never knew, job slows down 10x

❌ Failure Mode 2: Data Skew Inside Broadcast
    Hash table uneven distribution
    One executor gets more collisions
    Memory spikes locally, others fine
    
❌ Failure Mode 3: Many Concurrent Tasks
    Each task references broadcast
    JVM pressure multiplies
    GC storms (garbage collection chaos)
    Job doesn't fail fast - it crawls, then dies
"""

## Example: Monitoring broadcast size
print("\n=== Monitoring Broadcast Safety ===")

## Check actual size before broadcasting
drivers_size_mb = (
    drivers_df.rdd.map(lambda x: len(str(x))).sum() / 1024 / 1024
)
print(f"Estimated drivers_df size: {drivers_size_mb:.2f} MB")

## Safe broadcast: explicit and monitored
if drivers_size_mb < 10:
    print("✅ Safe to broadcast drivers_df")
else:
    print("⚠️  Too large for broadcast, use shuffle join")


## =============================================================================
## 4️⃣ JOIN ORDER: The Hidden Superpower
## =============================================================================

"""
Spark is NOT guaranteed to choose best join order.

Example:
    A.join(B).join(C)
    
Spark may execute: (A ⨝ B) ⨝ C

But if:
    - B is selective (filters 90% of rows)
    - C is huge
    
Better plan: A ⨝ (B ⨝ C)

Why? Intermediate result size changes drastically!

Problem: Spark can only reorder if:
    - Cost-based optimizer (CBO) enabled
    - Statistics exist (ANALYZE TABLE)
    - No explicit hints block it
    
Without stats → Spark guesses conservatively → bigger shuffles
"""

## Example: Bad join order
print("\n=== Bad Join Order (Large Intermediate Result) ===")

## First join creates huge intermediate table
bad_order = (
    deliveries_df
    .join(drivers_df, "Delivery_person_ID")  # Creates 45K rows
    .join(vehicles_df, "Delivery_person_ID")  # Joins 45K rows again
)

bad_order.explain()


## Example: Better join order (reduce early)
print("\n=== Better Join Order (Reduce Early) ===")

## Join smaller dimension tables first, THEN join to fact
better_order = (
    deliveries_df
    .join(
        drivers_df.join(vehicles_df, "Delivery_person_ID"),  # Join dims first (smaller)
        "Delivery_person_ID"
    )
)

better_order.explain()


## =============================================================================
## 5️⃣ THE PROFESSIONAL PATTERN: Reduce Early
## =============================================================================

"""
Rule: Shrink data BEFORE you join it.

❌ Bad Pattern:
    fact ⨝ dim1 ⨝ dim2 ⨝ dim3 → filter → aggregate

✅ Good Pattern:
    fact → filter → aggregate → ⨝ dim1 → ⨝ dim2

Why?
    - Smaller rows
    - Fewer columns
    - Lower shuffle volume
    - Lower memory pressure
    
Spark rewards early reduction!
"""

## Example: Reduce early pattern
print("\n=== Professional Pattern: Reduce Early ===")

## ❌ Bad: Join first, filter later
bad_pattern = (
    deliveries_df
    .join(drivers_df, "Delivery_person_ID")
    .join(vehicles_df, "Delivery_person_ID")
    .filter(col("City") == "Urban")  # Filter AFTER expensive joins
    .groupBy("City")
    .agg(avg("Time_taken (min)"))
)

print("Bad pattern (filter after join):")
bad_pattern.explain()


## ✅ Good: Filter first, then join
good_pattern = (
    deliveries_df
    .filter(col("City") == "Urban")  # Filter FIRST (reduces 70% of data)
    .select("Delivery_person_ID", "City", "Time_taken (min)")  # Select only needed columns
    .join(broadcast(drivers_df), "Delivery_person_ID")  # Now safe to broadcast
    .groupBy("City")
    .agg(avg("Time_taken (min)"))
)

print("\nGood pattern (filter early):")
good_pattern.explain()

print("\nResults comparison:")
bad_pattern.show()
good_pattern.show()


## =============================================================================
## 6️⃣ DEFENSIVE JOIN ENGINEERING: The Four Rules
## =============================================================================

"""
Rule 1: Only broadcast ONE table per stage
    - Pick the smallest
    - Assert it explicitly with broadcast()
    - Monitor its size over time

Rule 2: Never broadcast after a big join
    - Intermediate results are NOT dimension tables
    - Broadcasting them kills clusters

Rule 3: Partition before large joins
    - If both sides are large: repartition by join key first
    - Converts chaos into predictable cost

Rule 4: Prefer SortMergeJoin for very large tables
    - Slower per row but safer at scale
    - Disable auto-broadcast if needed
"""

## Example: Rule 1 - Single broadcast only
print("\n=== Rule 1: Single Broadcast Only ===")

## ✅ Good: One broadcast, one shuffle
safe_join = (
    deliveries_df
    .join(broadcast(drivers_df), "Delivery_person_ID")  # Broadcast small table
    .join(vehicles_df, "Delivery_person_ID")  # Let Spark shuffle this
)

safe_join.explain()


## Example: Rule 3 - Pre-partition large tables
print("\n=== Rule 3: Pre-partition Before Large Join ===")

## When both tables are large, partition by join key first
partitioned_join = (
    deliveries_df
    .repartition(8, "Delivery_person_ID")  # Partition by join key
    .join(
        drivers_df.repartition(8, "Delivery_person_ID"),  # Same partitioning
        "Delivery_person_ID"
    )
)

partitioned_join.explain()


## Example: Rule 4 - Force SortMergeJoin for safety
print("\n=== Rule 4: Force SortMergeJoin (Disable Broadcast) ===")

## Disable auto-broadcast to force SortMergeJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

sort_merge_join = (
    deliveries_df
    .join(drivers_df, "Delivery_person_ID")
)

print("SortMergeJoin (safest for large tables):")
sort_merge_join.explain()

## Reset threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")


## =============================================================================
## 7️⃣ READING JOIN WARS in explain()
## =============================================================================

"""
When reading explain(), look for WARNING SIGNS:

🚨 Multiple BroadcastExchange nodes
   → Memory pressure multiplies

🚨 Broadcasts chained together
   → Executors hold multiple hash tables

🚨 Huge intermediate projections
   → Too many columns carried through

🚨 Missing filters before joins
   → Unnecessary data volume

🚨 Exchange with high row count estimates
   → Expensive shuffles
"""

## Example: Spotting problems in explain()
print("\n=== Reading Explain Plans (Warning Signs) ===")

## Bad plan - multiple broadcasts
bad_plan = (
    deliveries_df
    .join(broadcast(drivers_df), "Delivery_person_ID")
    .join(broadcast(vehicles_df), "Delivery_person_ID")
)

print("🚨 BAD PLAN - Look for multiple BroadcastExchange:")
bad_plan.explain()


## =============================================================================
## 8️⃣ MENTAL UPGRADE: The Three Levels
## =============================================================================

"""
Beginner:
    "Spark failed with OOM."

Intermediate:
    "A broadcast join caused memory pressure."

Level-3 Engineer:
    "Two broadcasts composed badly; join order inflated the intermediate 
     result before reduction."

That sentence alone is the difference between user and engineer.
"""

## =============================================================================
## 9️⃣ REAL-WORLD SURVIVAL PATTERN
## =============================================================================

"""
Your future pipeline:
    events ⨝ users ⨝ sessions ⨝ devices ⨝ geo ⨝ products

This is Project 2 in disguise.

Without this knowledge:
    - Pipelines work on dev
    - Explode on prod  
    - Fail only under peak load

With this knowledge:
    - You design joins intentionally
    - You control memory
    - You sleep at night
"""

## Example: Production-grade multi-join pattern
print("\n=== Production Pattern: Safe Multi-Join ===")

## Step 1: Filter and select early
fact_reduced = (
    deliveries_df
    .filter(col("City").isNotNull())
    .select("Delivery_person_ID", "City", "Time_taken (min)", "Order_Date")
)

## Step 2: Single broadcast of smallest dim
with_drivers = fact_reduced.join(
    broadcast(
        drivers_df.select("Delivery_person_ID", "Delivery_person_Ratings")
    ),
    "Delivery_person_ID"
)

## Step 3: Shuffle join for remaining dims
final_result = with_drivers.join(
    vehicles_df.select("Delivery_person_ID", "Type_of_vehicle"),
    "Delivery_person_ID"
)

## Step 4: Aggregate after joins
production_metrics = (
    final_result
    .groupBy("City", "Type_of_vehicle")
    .agg(
        avg("Time_taken (min)").alias("avg_time"),
        count("*").alias("total_deliveries"),
        avg("Delivery_person_Ratings").alias("avg_rating")
    )
)

print("Production-safe plan:")
production_metrics.explain()
production_metrics.show()


## =============================================================================
## 🎯 KEY TAKEAWAYS
## =============================================================================

"""
1. Broadcast joins compose badly - avoid multiple broadcasts
2. Monitor table sizes - what's small today may be large tomorrow
3. Reduce early - filter and select before joining
4. Join order matters - let CBO help or control it manually
5. Pre-partition large tables before joining
6. Read explain() plans - spot warning signs early
7. SortMergeJoin is slower but safer at scale
"""

spark.stop()