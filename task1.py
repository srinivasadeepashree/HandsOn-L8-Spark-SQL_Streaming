from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1.1 Create a Spark session [cite: 36]
spark = SparkSession.builder \
    .appName("RideSharingAnalytics") \
    .getOrCreate()

# Set log level to reduce console noise
spark.sparkContext.setLogLevel("WARN")

# 1.2 Define the schema for incoming JSON data 
# Note: 'timestamp' is read as StringType initially, as instructed, for later conversion.
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 1.3 Read streaming data from socket (e.g., localhost:9999) 
# The raw data comes in a 'value' column as a string
raw_stream_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 1.4 Parse JSON data into columns using the defined schema [cite: 34, 38]
parsed_stream_df = raw_stream_df.select(
    from_json(col("value"), schema).alias("data")
).select(
    "data.*" # Flattens the nested 'data' structure into columns
)

# Display the Schema and Data for verification
print("--- Streaming DataFrame Schema ---")
parsed_stream_df.printSchema()

# 1.5 Print the parsed data to the console (for verification of Task 1) 
# Use 'append' mode since we are not performing aggregations yet
query = parsed_stream_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/task1") \
    .option("checkpointLocation", "/tmp/checkpoints_task1") \
    .start()

# Wait for the termination signal
query.awaitTermination()

# To stop the Spark Session after termination (optional but good practice)
# spark.stop()