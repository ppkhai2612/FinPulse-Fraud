"""Curate customer-profiles: gzip JSON at /landing -> Parquet at /curated.

Big dimension (~100K rows) — no partitioning.
"""

from pyspark.sql import SparkSession


LANDING = "hdfs://namenode:9000/landing/customer-profiles/customer-profiles.json.gz"
CURATED = "hdfs://namenode:9000/curated/customer-profiles/"


def main():
    
    spark = SparkSession.builder.appName("finpulse-curate-customers").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("multiline", "true").json(LANDING)
    print(f"Read {df.count()} rows from {LANDING}")
    df.printSchema()

    df.write.mode("overwrite").parquet(CURATED)
    print(f"Wrote Parquet to {CURATED}")

    spark.stop()


if __name__ == "__main__":
    main()