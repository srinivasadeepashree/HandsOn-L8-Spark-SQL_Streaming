from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- CONFIGURATION ---
HOST = "localhost"
PORT = 9999
CHECKPOINT_BASE_DIR = "/tmp/checkpoints_task2"

# 1.1 Create a Spark session
spark = SparkSession.builder \
    .appName("RideSharingAnalytics") \
    .getOrCreate()

# Set log level to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# 1.2 Define the schema for incoming JSON data
# 'timestamp' is read as StringType initially, as instructed
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True) # Read as String for parsing
])

# 1.3 Ingest streaming data from socket
# The raw data comes in a 'value' column as a string
raw_stream_df = spark.readStream \
    .format("socket") \
    .option("host", HOST) \
    .option("port", PORT) \
    .load()

# Step 4: Parse JSON
parsed_stream = raw_stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Step 5: Convert timestamp column
parsed_stream = parsed_stream.withColumn("event_time", col("timestamp").cast(TimestampType()))

# Step 6: Aggregations per driver
aggregated_stream = parsed_stream.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Step 7: Write each batch to CSV using foreachBatch
def write_to_csv(batch_df, batch_id):
    batch_df.coalesce(1).write.mode("append").csv(f"outputs/task2", header=True)

query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", CHECKPOINT_BASE_DIR) \
    .start()

query.awaitTermination()