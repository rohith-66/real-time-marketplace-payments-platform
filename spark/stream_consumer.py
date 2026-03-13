# spark/stream_consumer.py
"""
Spark Structured Streaming consumer that reads marketplace payment events from Kafka,
parses JSON, performs minimal validation, and writes Bronze parquet files partitioned by ingest_date.

Run with spark-submit providing:
  --conf spark.sql.shuffle.partitions=1  (for local dev)
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2

It expects two arguments (or environment variables):
  --bootstrap-servers (e.g. host.docker.internal:9092)
  --topic (marketplace-payments)

Outputs:
  ./data/bronze/ingest_date=YYYY-MM-DD/*.parquet
  checkpoint at ./data/checkpoints/bronze_kafka_consumer
"""
import sys
import json
from pyspark.sql import SparkSession, functions as F, types as T
from datetime import datetime

KAFKA_BOOTSTRAP = None
KAFKA_TOPIC = None
OUTPUT_PATH = "data/bronze"
CHECKPOINT_PATH = "data/checkpoints/bronze_kafka_consumer"

# --- Define schema that matches schemas/payment_event_schema.json ---
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("seller_id", T.StringType(), False),
        T.StructField("city", T.StringType(), False),
        T.StructField("order_category", T.StringType(), False),
        T.StructField("payment_method", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("order_amount", T.DoubleType(), False),
        T.StructField("platform_fee", T.DoubleType(), False),
        T.StructField("seller_payout", T.DoubleType(), False),
        T.StructField("currency", T.StringType(), False),
        T.StructField("event_timestamp", T.StringType(), False),
    ]
)


def parse_args():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap-servers", default=None)
    p.add_argument("--topic", default="marketplace-payments")
    p.add_argument("--output-path", default=OUTPUT_PATH)
    p.add_argument("--checkpoint-path", default=CHECKPOINT_PATH)
    args = p.parse_args()
    return args


def main():
    args = parse_args()
    bs = args.bootstrap_servers
    topic = args.topic
    out_path = args.output_path
    chk = args.checkpoint_path

    if not bs:
        print("ERROR: --bootstrap-servers is required (e.g. host.docker.internal:9092)")
        sys.exit(2)

    spark = (
        SparkSession.builder.appName("marketplace-payments-consumer")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    # Read from Kafka
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bs)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")  # use "earliest" if you want all historical messages
        .load()
    )

    # Kafka value is bytes; cast to string
    json_df = raw_df.selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_ingest_ts")

    # Parse JSON to columns
    parsed = json_df.withColumn("json_data", F.from_json(F.col("value"), EVENT_SCHEMA)).select(
        F.col("json_data.*"), "kafka_ingest_ts"
    )

    # Minimal derived columns & validation
    # 1) convert event_timestamp to timestamp type
    parsed = parsed.withColumn(
        "event_ts",
        F.to_timestamp("event_timestamp"),
    )

    # 2) add ingest_date partition column (UTC date of processing)
    parsed = parsed.withColumn("ingest_date", F.current_date())

    # 3) basic validation: positive amounts and seller_payout approximately equals order_amount - platform_fee
    #    We'll tag records as valid / invalid
    parsed = parsed.withColumn(
        "is_amount_positive",
        (F.col("order_amount") >= 0) & (F.col("platform_fee") >= 0) & (F.col("seller_payout") >= 0),
    )

    # tolerance check for floating arithmetic
    parsed = parsed.withColumn(
        "payout_matches",
        F.abs(F.col("seller_payout") - (F.col("order_amount") - F.col("platform_fee"))) < 0.01,
    )

    parsed = parsed.withColumn(
        "is_valid_record",
        F.col("is_amount_positive") & F.col("payout_matches") & F.col("event_ts").isNotNull(),
    )

    # Separate valid and invalid streams using foreachBatch write (so we can write both to disk)
    def write_batch(batch_df, batch_id):
        # write valid records to bronze
        valid = batch_df.filter(F.col("is_valid_record"))
        if valid.count() > 0:
            (
                valid.drop("is_amount_positive", "payout_matches", "is_valid_record")
                .write.mode("append")
                .partitionBy("ingest_date")
                .parquet(out_path)
            )
        # write invalids to a separate folder for inspection
        invalid = batch_df.filter(~F.col("is_valid_record"))
        if invalid.count() > 0:
            (
                invalid.drop("is_amount_positive", "payout_matches", "is_valid_record")
                .write.mode("append")
                .parquet(out_path + "_invalid")
            )

    query = (
        parsed.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", chk)
        .outputMode("append")
        .start()
    )

    print("Stream started. Writing to:", out_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()