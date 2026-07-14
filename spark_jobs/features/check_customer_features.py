"""Sanity checks for /analytics/customer_features/"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, max as spark_max, avg, col


FEATURES = "hdfs://namenode:9000/analytics/customer_features"


def main():
    spark = SparkSession.builder.appName("finpulse-check-customer-features").getOrCreate()
    df = spark.read.parquet(FEATURES)

    print("Feature aggregation checks:")
    df.agg(
        count("*").alias("rows"),
        countDistinct("card_id").alias("distinct_cards"),
        spark_max("txn_count").alias("max_txn_count"),
        avg("txn_count").alias("avg_txn_count"),
        avg("avg_amount").alias("avg_of_avg_amount"),
        avg("pct_international").alias("avg_pct_international"),
        avg("pct_online").alias("avg_pct_online"),
    ).show(truncate=False)

    print("Duplicate card_id rows; expect no output:")
    df.groupBy("card_id") \
       .count().filter(col("count") > 1).show(10, truncate=False)

    print("Amount and count distributions:")
    df.select(
        "txn_count",
        "avg_amount",
        "stddev_amount",
        "p95_amount",
        "unique_merchant_count",
        "unique_country_count",
        "pct_international",
        "pct_online",
    ).describe().show(truncate=False)

    print("Sample rows:")
    df.select(
        "card_id",
        "txn_count",
        "avg_amount",
        "p95_amount",
        "unique_merchant_count",
        "seen_countries",
        "pct_international",
        "pct_online",
    ).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
