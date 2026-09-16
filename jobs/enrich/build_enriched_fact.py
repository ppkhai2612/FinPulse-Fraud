from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, broadcast, when, to_date, row_number


BOOTSTRAP = "kafka:9094"
TOPIC = "transactions"
CURATED = "hdfs://namenode:9000/curated"
ANALYTICS = "hdfs://namenode:9000/analytics/transactions_enriched"

TRANSACTION_SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("channel", StringType()),
    StructField("is_international", StringType())
])


def read_transactions(spark):
    """Read transactions from Kafka topic"""
    raw = spark \
        .read.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    return raw.select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("t")) \
        .select("t.*") \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("is_international", col("is_international").cast("boolean")) \
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        

def read_dims(spark):
    """Read dimensions data from /curated in HDFS"""
    customers = spark \
        .read.parquet(f"{CURATED}/customer-profiles/") \
        .select(
            "customer_id", "card_id", "age",
            "income_bracket", "account_age_months", "avg_monthly_spend",
            "home_country", "typical_categories", "credit_limit"
        )

    merchants = spark \
        .read.parquet(f"{CURATED}/merchant-directory/") \
        .select(
            "merchant_id", col("name").alias("merchant_name"), "category",
            col("country").alias("merchant_country"),
            "risk_score", "avg_transaction_amount", "monthly_volume"
        )

    fraud_reports = spark \
        .read.parquet(f"{CURATED}/fraud-reports/") \
        .select("txn_id", "fraud_type", "amount_disputed", "resolution")

    window = Window.partitionBy("txn_id").orderBy("session_id")
    devices = spark \
        .read.parquet(f"{CURATED}/device-fingerprints/") \
        .select(
            "session_id", "txn_id", "device_type", "os",
            "browser", "ip_country", "ip_city",
            "is_vpn", "is_known_device", "login_attempt_count"
        ).withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")

    return customers, merchants, fraud_reports, devices


def enrich(transactions, customers, merchants, fraud_reports, devices):
    """Join transactions with its dimensions"""
    return transactions \
        .join(broadcast(customers), on="card_id", how="inner") \
        .join(broadcast(merchants), on="merchant_id", how="inner") \
        .join(fraud_reports, on="txn_id", how="left") \
        .join(devices, on="txn_id", how="left")


def label_and_partition(enriched):
    """Add new columns (confirmed_fraud + dt) support for 'check_fraud_rate' job later"""
    return enriched \
        .withColumn(
            "confirmed_fraud",
            when(col("resolution") == "confirmed_fraud", True).otherwise(False)
        ) \
        .withColumn(
            "dt", to_date(col("timestamp"))
        )


def write(out):
    """Write enriched transactions out to /analytics in HDFS"""
    out.write.mode("overwrite").partitionBy("dt").parquet(ANALYTICS)


def main():
    spark = SparkSession.builder.appName("finpulse-build-enriched-fact").getOrCreate()

    # read data
    transactions = read_transactions(spark)
    customers, merchants, fraud_reports, devices = read_dims(spark)

    # enrich transactions
    enriched = enrich(transactions, customers, merchants, fraud_reports, devices)
    out = label_and_partition(enriched)

    # write out
    write(out)
    print(f"Wrote {out.count()} rows to {ANALYTICS}")

    spark.stop()


if __name__ == "__main__":
    main()