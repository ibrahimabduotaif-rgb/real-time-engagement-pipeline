"""
Spark Structured Streaming job for the real‑time engagement pipeline.

This script reads raw engagement events from Kafka, enriches each event with
metadata from the ``content`` table stored in PostgreSQL, derives engagement
metrics, and then fan‑outs the processed records to three destinations:

* Parquet files simulating ingestion into BigQuery (for analytics).
* A Redis sorted set for real‑time leaderboards.
* A placeholder external system (demonstrated via console logging) for
  integration with other services.

The job guarantees at‑least‑once semantics by persisting state internally and
using a checkpoint directory (configured via the writeStream API when
executing this script with spark-submit).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    TimestampType,
)
import redis


def main() -> None:
    """Entry point for the streaming application."""
    spark = (
        SparkSession.builder.appName("EngagementPipeline")
        # These options would normally be configured in spark-submit
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for raw engagement events coming from Kafka
    schema = (
        StructType()
        .add("content_id", StringType())
        .add("user_id", StringType())
        .add("event_type", StringType())
        .add("event_ts", TimestampType())
        .add("duration_ms", IntegerType())
    )

    # Read the raw stream from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "engagement_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON payload from the Kafka messages
    events = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Load the content dimension from Postgres.
    # Note: credentials match those defined in docker-compose.yml.
    content_df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/app_db")
        .option("dbtable", "public.content")
        .option("user", "user")
        .option("password", "password")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Perform enrichment and derive metrics
    enriched_df = (
        events.join(content_df, events.content_id == content_df.id, "left")
        .withColumn("engagement_seconds", col("duration_ms") / 1000.0)
        .withColumn(
            "engagement_pct",
            round(col("engagement_seconds") / col("length_seconds"), 2),
        )
    )

    def write_to_sinks(batch_df, batch_id):
        """Write each micro‑batch to the configured sinks."""
        # Persist to avoid recomputation across sinks
        batch_df.persist()

        # A. BigQuery simulation: write to local Parquet files
        (
            batch_df.write.mode("append")
            .parquet("/app/data/bigquery_output")
        )

        # B. External system simulation: log batch size
        print(f"Batch {batch_id} processed with {batch_df.count()} records.")

        # C. Aggregation for Redis: compute per‑content engagement count
        leaderboard_df = batch_df.groupBy("title").count()

        # Update Redis sorted set using simple Redis client.
        # In production you may want to use a streaming connector.
        client = redis.Redis(host="redis", port=6379, db=0)
        for row in leaderboard_df.collect():
            title = row["title"] or "UNKNOWN"
            score = row["count"]
            # ZINCRBY to increment score by count
            client.zincrby("top_content", score, title)

        batch_df.unpersist()

    # Start the streaming query with our multi‑sink writer
    query = (
        enriched_df.writeStream.foreachBatch(write_to_sinks)
        .trigger(processingTime="2 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()