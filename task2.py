from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
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

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_stream = processed_stream.groupBy("driver_id").agg(sum("fare_amount").alias("total_fare"), avg("distance_km").alias("avg_distance"))

# Define a function to write each batch to a CSV file
def write_batch_to_csv(df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    if df.count() > 0:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"outputs/task2/batch_{batch_id}")

# Use foreachBatch to apply the function to each micro-batch
query = aggregated_stream.writeStream.foreachBatch(write_batch_to_csv).outputMode("update").option("checkpointLocation", "outputs/task2/.checkpoint").start()

query.awaitTermination()
