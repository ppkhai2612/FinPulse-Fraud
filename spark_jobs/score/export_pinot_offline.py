""""Export /analytics/scored/ to local Parquet files
Therefore, Pinot can perform standalone batch ingestion into an OFFLINE table
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


SCORED = "hdfs://namenode:9000/analytics/scored"
OUT = "file:///opt/pinot-offline/scored"


def parse_args():
    args = argparse.ArgumentParser()
    args.add_argument(
        "--date",
        default=None,
        help="restrict export to a single dt partition (YYYY-MM-DD)"
    )
    return args.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("finpulse-export-pinot-offline").getOrCreate()

    # read
    df = spark.read.parquet(SCORED)
    if args.date: # filter by partition date
        df = df.filter(col("dt") == args.date)

    out = df.select(
        "txn_id", "card_id", "merchant_id",
        "country", "channel", "recommended_action",
        "is_international", "confirmed_fraud", "predicted_fraud",
        "rule_high_amount", "rule_velocity",
        "rule_international_mismatch",
        "rule_unknown_device_vpn",
        "rule_high_risk_merchant",
        "amount", "risk_score", "merchant_risk_score",
        lit(0).cast("long").alias("velocity_count"),
        col("timestamp").alias("event_time")
    ).filter(col("event_time").isNotNull())

    # write
    # Before writing, the data (currently partitioned by date) will be compressed into a single partition
    out.coalesce(1).write.mode("overwrite").parquet(OUT)

    print(f"Exported {out.count()} scored rows to {OUT}")

    spark.stop()


if __name__ == "__main__":
    main()