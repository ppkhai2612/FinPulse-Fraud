"""Spark job for smoke testing: Spark <-> HMS <-> Trino"""
from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder.appName("finpulse-smoke-trino").enableHiveSupport().getOrCreate()
    
    rows = [(1, "alice", 100.0), (2, "bob", 250.0), (3, "carol", 42.0)]
    df = spark.createDataFrame(rows, schema="id INT, name STRING, amount DOUBLE")
    
    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    df.write.mode("overwrite").saveAsTable("default.smoke_hms")

    count = spark.sql("SELECT COUNT(*) FROM default.smoke_hms").collect()[0][0]
    print(f"Row count: {count}")
    assert count == 3, f"Expected 3 rows, got {count}"
    print("smoke_trino OK")

    spark.stop()


if __name__ == "__main__":
    main()