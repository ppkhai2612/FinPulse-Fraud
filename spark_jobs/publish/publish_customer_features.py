"""Publish customer features to Kafka 'customer-features' topic"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json


FEATURES = "hdfs://namenode:9000/analytics/customer_features"
BOOTSTRAP = "kafka:9094"
TOPIC = "customer-features"


def main():
    spark = SparkSession.builder.appName("finpulse-publish-customer-features").getOrCreate()
    
    features = spark.read.parquet(FEATURES)
    struct_features = features.select(
        col("card_id").cast("string").alias("key"),
        to_json(struct(
            col("avg_monthly_spend"),
            col("home_country"),
            col("seen_countries"),
            col("unique_country_count")
        )).alias("value")
    )

    struct_features.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("topic", TOPIC) \
        .save()

    print(f"Published {struct_features.count()} card feature rows to topic '{TOPIC}'")

    spark.stop()


if __name__ == "__main__":
    main()