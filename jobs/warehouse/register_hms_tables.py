"""Spark job for registering /curated/* and /analytics/* in the Hive Metastore for Trino"""
from pyspark.sql import SparkSession


CURATED = "hdfs://namenode:9000/curated"
ANALYTICS = "hdfs://namenode:9000/analytics"

TABLES = [
    ("curated", "customer_profiles", f"{CURATED}/customer-profiles", None),
    ("curated", "merchant_directory", f"{CURATED}/merchant-directory", None),
    ("curated", "device_fingerprints", f"{CURATED}/device-fingerprints", "device_type"),
    ("curated", "fraud_reports", f"{CURATED}/fraud-reports", "fraud_type"),
    ("analytics", "transactions_enriched", f"{ANALYTICS}/transactions_enriched", "dt"),
    ("analytics", "customer_features", f"{ANALYTICS}/customer_features", None),
    ("analytics", "scored", f"{ANALYTICS}/scored", "dt")
]

def main():

    spark = SparkSession.builder.appName("finpulse-register-hms-tables").enableHiveSupport().getOrCreate()
    
    for db, table, path, partition_col in TABLES:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        fqtn = f"{db}.{table}"
        df = spark.read.parquet(path)

        # write process
        writer = df.write.mode("overwrite").format("parquet")
        if partition_col is not None:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(fqtn)

        count = spark.sql(f"SELECT COUNT(*) FROM {fqtn}").collect()[0][0]
        print(f"Registered {fqtn}: {count} rows"
            + (f" (partitioned by {partition_col})" if partition_col else ""))

    print("\nDatabases now in HMS:")
    spark.sql("SHOW DATABASES").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()