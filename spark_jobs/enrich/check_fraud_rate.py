from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, col, sum as spark_sum


ANALYTICS = "hdfs://namenode:9000/analytics/transactions_enriched/"


def main():
    spark = SparkSession.builder.appName("finpulse-check-fraud-rate").getOrCreate()
    df = spark.read.parquet(ANALYTICS)
    df.agg(
        count("*").alias("total"),
        spark_sum(col("confirmed_fraud").cast("long")).alias("fraud_rows"),
        avg(col("confirmed_fraud").cast("double")).alias("fraud_rate")
    ).show(truncate=False)


if __name__ == "__main__":
    main()