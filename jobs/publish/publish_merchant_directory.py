"""Publish merchant risk scores to Kafka 'merchant-directory' topic"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json


MERCHANTS = "hdfs://namenode:9000/curated/merchant-directory"
BOOTSTRAP = "kafka:9094"
TOPIC = "merchant-directory"


def main():
    spark = SparkSession.builder.appName("finpulse-publish-merchant-directory").getOrCreate()
    
    merchants = spark.read.parquet(MERCHANTS)
    struct_merchants = merchants.select(
        col("merchant_id").cast("string").alias("key"),
        to_json(struct(
            col("risk_score").cast("int").alias("merchant_risk_score")
        )).alias("value")
    )

    struct_merchants.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("topic", TOPIC) \
        .save()

    print(f"Published {struct_merchants.count()} merchant rows to topic '{TOPIC}'")

    spark.stop()


if __name__ == "__main__":
    main()