"""Spark job for scoring transactions offline with rule-based fraud detection"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when, col, unix_timestamp, lit,
    coalesce, size, count, array_contains
)
from pyspark.sql.window import Window


ENRICHED = "hdfs://namenode:9000/analytics/transactions_enriched"
FEATURES = "hdfs://namenode:9000/analytics/customer_features"
SCORED = "hdfs://namenode:9000/analytics/scored"
RISK_THRESHOLD = 2


def read_scoring_inputs(spark):
    """Read transactions and join customer baseline"""
    transactions = spark.read.parquet(ENRICHED) \
        .select(
            "txn_id",
            "timestamp",
            "card_id",
            "merchant_id",
            "amount",
            "country",
            "channel",
            "is_international",
            "is_vpn",
            "is_known_device",
            col("risk_score").alias("merchant_risk_score"),
            "confirmed_fraud", "dt"
        ).dropDuplicates(['txn_id'])
    
    features = spark.read.parquet(FEATURES) \
        .select(
            "card_id",
            "avg_monthly_spend",
            "home_country",
            "seen_countries",
            "unique_country_count"
        )

    return transactions.join(features, on="card_id", how="left")
    

def apply_rules(df):
    """Apply five rules to compute risk score for transactions"""

    # Define a 10-minute rolling time window for each card, ordered by transaction timestamp
    velocity_window = Window \
        .partitionBy("card_id") \
        .orderBy(unix_timestamp("timestamp")) \
        .rangeBetween(-600, Window.currentRow)

    with_velocity = df.withColumn(
        "velocity_count",
        count("*").over(velocity_window)
    )

    return with_velocity \
        .withColumn(
            "rule_high_amount",
            col("amount") > (lit(3.0) * col("avg_monthly_spend") / lit(30.0))
        ) \
        .withColumn("rule_velocity",col("velocity_count") >= lit(5)) \
        .withColumn(
            "rule_international_mismatch",
            col("is_international")
            & (size(col("seen_countries")) == 1)
            & array_contains(col("seen_countries"), col("home_country"))
        ) \
        .withColumn(
            "rule_unknown_device_vpn",
            (~coalesce(col("is_known_device"), lit(False)))
            & coalesce(col("is_vpn"), lit(False))
        ) \
        .withColumn(
            "rule_high_risk_merchant",
            coalesce(col("merchant_risk_score"), lit(0)) >= lit(8)
        ) \
        .withColumn(
            "risk_score",
            col("rule_high_amount").cast("int")
            + col("rule_velocity").cast("int")
            + col("rule_international_mismatch").cast("int")
            + col("rule_unknown_device_vpn").cast("int")
            + col("rule_high_risk_merchant").cast("int")
        ) \
        .withColumn("predicted_fraud", col("risk_score") >= RISK_THRESHOLD) \
        .withColumn(
            "recommended_action",
            when(col("risk_score") >= lit(3), lit("block"))
            .when(col("risk_score") >= lit(RISK_THRESHOLD), lit("review"))
            .otherwise(lit("approve"))
        )


def select_output(df):
    """Keep the transaction labels and scoring columns."""
    return df.select(
        "txn_id",
        "timestamp",
        "card_id",
        "merchant_id",
        "amount",
        "country",
        "channel",
        "is_international",
        "merchant_risk_score",
        "confirmed_fraud",
        "dt",
        "rule_high_amount",
        "rule_velocity",
        "rule_international_mismatch",
        "rule_unknown_device_vpn",
        "rule_high_risk_merchant",
        "risk_score",
        "predicted_fraud", 
        "recommended_action"
    )


def write(scored_df):
    """Write scored DF out to /analytics/scored"""
    scored_df.write.mode("overwrite").partitionBy("dt").parquet(SCORED)


def main():
    spark = SparkSession.builder.appName("finpulse-score-offline").getOrCreate()

    joined = read_scoring_inputs(spark)
    scored = select_output(apply_rules(joined))
    write(scored)
    print(f"Wrote {scored.count()} scored rows to {SCORED}")
    
    spark.stop()


if __name__ == "__main__":
    main()