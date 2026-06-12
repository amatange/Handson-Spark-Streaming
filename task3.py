# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType and add a watermark
processed_stream = parsed_stream.withColumn("event_time", col("timestamp").cast(TimestampType())).withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_stream = processed_stream .groupBy(window(col("event_time"), "5 minutes", "1 minute")).agg(sum("fare_amount").alias("total_fare"))

# Extract window start and end times as separate columns
final_stream = windowed_stream.withColumn("window_start", col("window").getField("start")).withColumn("window_end", col("window").getField("end")).drop("window")

# Define a function to write each batch to a CSV file with column names
def write_window_batch(df, batch_id):
    # Save the batch DataFrame as a CSV file with headers included
    if df.count() > 0:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"outputs/task3/batch_{batch_id}")
    
# Use foreachBatch to apply the function to each micro-batch
query = final_stream.writeStream.foreachBatch(write_window_batch).outputMode("update").option("checkpointLocation", "outputs/task3/.checkpoint").start()

query.awaitTermination()
