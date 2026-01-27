from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)

delivery_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("ingestion_time", TimestampType(), False),
    StructField("payload", StructType([
        StructField("ID", StringType(), True),
        StructField("Delivery_person_ID", StringType(), True),
        StructField("Delivery_person_Age", StringType(), True),
        StructField("Delivery_person_Ratings", StringType(), True),
        StructField("Restaurant_latitude", StringType(), True),
        StructField("Restaurant_longitude", StringType(), True),
        StructField("Delivery_location_latitude", StringType(), True),
        StructField("Delivery_location_longitude", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Time_Orderd", StringType(), True),
        StructField("Time_Order_picked", StringType(), True),
        StructField("Weather_conditions", StringType(), True),
        StructField("Road_traffic_density", StringType(), True),
        StructField("Vehicle_condition", StringType(), True),
        StructField("Type_of_order", StringType(), True),
        StructField("Type_of_vehicle", StringType(), True),
        StructField("multiple_deliveries", StringType(), True),
        StructField("Festival", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Time_taken (min)", StringType(), True),
    ]), True)
])
