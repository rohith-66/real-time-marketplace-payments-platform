from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"


def main():
    spark = (
        SparkSession.builder
        .appName("marketplace-payments-silver-transform")
        .getOrCreate()
    )

    df = spark.read.parquet(BRONZE_PATH)

    # Standardize timestamp column
    df = df.withColumn("event_ts", F.to_timestamp("event_timestamp"))

    # Basic validation rules
    df = df.filter(F.col("event_ts").isNotNull())
    df = df.filter(F.col("order_amount") >= 0)
    df = df.filter(F.col("platform_fee") >= 0)
    df = df.filter(F.col("seller_payout") >= 0)

    # Ensure payout logic is correct
    df = df.filter(
        F.abs(F.col("seller_payout") - (F.col("order_amount") - F.col("platform_fee"))) < 0.01
    )

    # Deduplicate by event_id, keep latest event_ts
    window_spec = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())

    df = (
        df.withColumn("row_num", F.row_number().over(window_spec))
          .filter(F.col("row_num") == 1)
          .drop("row_num")
    )

    # Add partition column
    df = df.withColumn("event_date", F.to_date("event_ts"))

    # Write silver output
    (
        df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(SILVER_PATH)
    )

    print("Silver transformation completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()