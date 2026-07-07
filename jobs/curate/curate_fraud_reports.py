from pyspark.sql import SparkSession


LANDING = "hdfs://namenode:9000/landing/fraud-reports/fraud-reports.json.gz"
CURATED = "hdfs://namenode:9000/curated/fraud-reports"


def main():
    
    spark = SparkSession.builder.appName("finpulse-curate-fraud-reports").getOrCreate()

    df = spark.read.option("multiline", "true").json(LANDING)
    print(f"Read {df.count()} rows from {LANDING}")
    df.printSchema()

    df.write.mode("overwrite").partitionBy("fraud_type").parquet(CURATED) # create a Spark job
    print(f"Wrote Parquet to {CURATED} (partitioned by fraud_type)")

    spark.stop()


if __name__ == "__main__":
    main()