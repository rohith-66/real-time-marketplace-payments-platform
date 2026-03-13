from pyspark.sql import SparkSession, functions as F


SILVER_PATH = "data/silver"
GOLD_PATH = "data/gold"


def main():
    spark = (
        SparkSession.builder
        .appName("marketplace-payments-gold-transform")
        .getOrCreate()
    )

    df = spark.read.parquet(SILVER_PATH)

    # Keep only successful payment capture events for revenue-style metrics
    payments_df = df.filter(F.col("event_type") == "payment_captured")

    # Seller-level metrics
    seller_metrics = (
        payments_df.groupBy("event_date", "seller_id")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("order_amount"), 2).alias("gross_sales"),
            F.round(F.sum("platform_fee"), 2).alias("platform_fee_revenue"),
            F.round(F.sum("seller_payout"), 2).alias("seller_payout_total"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value")
        )
    )

    # City-level metrics
    city_metrics = (
        payments_df.groupBy("event_date", "city")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("order_amount"), 2).alias("gross_sales"),
            F.round(F.sum("platform_fee"), 2).alias("platform_fee_revenue"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
            F.countDistinct("seller_id").alias("active_sellers")
        )
    )

    # Platform-level metrics
    platform_metrics = (
        payments_df.groupBy("event_date")
        .agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("order_amount"), 2).alias("gross_sales"),
            F.round(F.sum("platform_fee"), 2).alias("platform_fee_revenue"),
            F.round(F.sum("seller_payout"), 2).alias("seller_payout_total"),
            F.round(F.avg("order_amount"), 2).alias("avg_order_value"),
            F.countDistinct("seller_id").alias("active_sellers"),
            F.countDistinct("customer_id").alias("active_customers")
        )
    )

    # Write outputs
    (
        seller_metrics.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{GOLD_PATH}/seller_metrics")
    )

    (
        city_metrics.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{GOLD_PATH}/city_metrics")
    )

    (
        platform_metrics.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(f"{GOLD_PATH}/platform_metrics")
    )

    print("Gold transformation completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()