from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StringType, StructType, StructField

# Define schema for emoji data
schema = StructType([
    StructField("emoji", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

# Create Spark session
spark = SparkSession.builder \
    .appName("EmojiWindowedCounts") \
    .getOrCreate()

# Kafka broker for the single cluster (port 9092)
KAFKA_BROKER = 'localhost:9092'  # Single Kafka broker
KAFKA_TOPIC_CLUSTER1 = "aggregated_emoji_topic_cluster1"
KAFKA_TOPIC_CLUSTER2 = "aggregated_emoji_topic_cluster2"

# Checkpoint directory (this can be any directory where Spark can store its checkpoint data)
CHECKPOINT_DIR = "/tmp/spark_checkpoint_dir"  # Change this to your desired path

# Read from Kafka topic (source cluster)
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "emoji_topic") \
    .option("startingOffsets", "latest") \
    .load()


# Parse Kafka messages
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp column to a timestamp type
parsed_df = parsed_df.withColumn("event_time", col("timestamp").cast("timestamp"))

# Apply a 2-second tumbling window to count emojis
windowed_counts = parsed_df.groupBy(
    window(col("event_time"), "2 seconds"),
    col("emoji")
).count()

# Transform count by applying floor(count/50)
windowed_counts = windowed_counts.withColumn(
    "transformed_count", (col("count") / 200).cast("int")
)

# Filter the DataFrame to send only records where transformed_count > 0
kafka_message = windowed_counts.filter(col("transformed_count") > 0) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("emoji"),
        col("transformed_count")
    ).selectExpr(
        "CAST(window_start AS STRING) AS key",  # Optional: can be used as message key
        "to_json(struct(*)) AS value"  # Convert the whole row to JSON format
    )

# Write the output to Kafka (Cluster 1, Topic 1)
query_cluster1 = kafka_message.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", KAFKA_TOPIC_CLUSTER1) \
    .option("checkpointLocation", CHECKPOINT_DIR + "/cluster1") \
    .outputMode("complete") \
    .start()

# Write the output to Kafka (Cluster 1, Topic 2)
query_cluster2 = kafka_message.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", KAFKA_TOPIC_CLUSTER2) \
    .option("checkpointLocation", CHECKPOINT_DIR + "/cluster2") \
    .outputMode("complete") \
    .start()

# Wait for the queries to finish
query_cluster1.awaitTermination()
query_cluster2.awaitTermination()

