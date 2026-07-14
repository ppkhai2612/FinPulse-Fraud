from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, avg, stddev_samp, coalesce,
    percentile_approx, approx_count_distinct,
    collect_set, col, when, lit, datediff,
    min as spark_min, max as spark_max
)


ENRICHED = "hdfs://namenode:9000/analytics/transactions_enriched"
CUSTOMERS = "hdfs://namenode:9000/curated/customer-profiles"
CUSTOMER_FEATURES = "hdfs://namenode:9000/analytics/customer_features"


def read_transactions(spark):
    return spark \
        .read.parquet(ENRICHED) \
        .select(
            "txn_id", "timestamp",
            "card_id", "merchant_id",
            "amount", "currency",
            "merchant_category", "country",
            "channel", "is_international"
        ).dropDuplicates(['txn_id'])


def read_customers(spark):
    return spark \
        .read.parquet(CUSTOMERS) \
        .select(
            "customer_id", "card_id", "age",
            "income_bracket", "account_age_months",
            "avg_monthly_spend", "home_country",
            "typical_categories", "credit_limit"
        )
    
        
def build_customer_features(transactions):
    """Aggregate transactions into one baseline per card."""
    return transactions.groupBy("card_id") \
        .agg(
            count("*").alias("txn_count"),
            avg("amount").alias("avg_amount"),
            coalesce(stddev_samp("amount"), lit(0.0)).alias("stddev_amount"), # standard deviation
            percentile_approx("amount", 0.50).alias("p50_amount"),
            percentile_approx("amount", 0.95).alias("p95_amount"),
            percentile_approx("amount", 0.99).alias("p99_amount"),
            approx_count_distinct("merchant_id").alias("unique_merchant_count"), # estimates the approximate distinct count of elements in a column
            approx_count_distinct("merchant_category").alias("unique_category_count"),
            approx_count_distinct("country").alias("unique_country_count"),
            collect_set("merchant_category").alias("seen_categories"), # collects the values from a column into a set
            collect_set("country").alias("seen_countries"),
            collect_set("channel").alias("seen_channels"),
            avg(col("is_international").cast("double")).alias("pct_international"),
            avg(when(col("channel") == "online", 1.0).otherwise(0.0)).alias("pct_online"),
            avg(when(col("channel") == "atm", 1.0).otherwise(0.0)).alias("pct_atm"),
            spark_min("timestamp").alias("first_txn_ts"),
            spark_max("timestamp").alias("last_txn_ts")
        ) \
        .withColumn("active_days", datediff("last_txn_ts", "first_txn_ts") + lit(1)) \
        .withColumn("txns_per_active_day", col("txn_count") / col("active_days"))


def attach_customer_profile(customers, customer_features):
    return customers.join(customer_features, on="card_id", how="left")

    # return out.fillna({
    #     "txn_count": 0,
    #     "avg_amount": 0.0,
    #     "stddev_amount": 0.0,
    #     "p50_amount": 0.0,
    #     "p95_amount": 0.0,
    #     "p99_amount": 0.0,
    #     "unique_merchant_count": 0,
    #     "unique_category_count": 0,
    #     "unique_country_count": 0,
    #     "pct_international": 0.0,
    #     "pct_online": 0.0,
    #     "pct_atm": 0.0
    # })

def write(out):
    out.coalesce(4).write.mode("overwrite").parquet(CUSTOMER_FEATURES)


def main():
    spark = SparkSession.builder.appName("finpulse-build-customer-features").getOrCreate()

    # read data
    transactions = read_transactions(spark)
    customers = read_customers(spark)

    # build
    customer_features = build_customer_features(transactions)
    out = attach_customer_profile(customers, customer_features)

    # write out
    write(out)
    print(f"Transactions used: {transactions.count()}")
    print(f"Wrote {out.count()} card feature rows to {CUSTOMER_FEATURES}")

    spark.stop()


if __name__ == "__main__":
    main()