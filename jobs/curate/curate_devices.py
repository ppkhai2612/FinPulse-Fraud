from pyspark.sql import SparkSession


LANDING = "hdfs://namenode:9000/landing/device-fingerprints/device-fingerprints.csv.gz"
CURATED = "hdfs://namenode:9000/curated/device-fingerprints"


def main():

    spark = SparkSession.builder.appName("finpulse-curate-devices").getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(LANDING)
    df.write.mode("overwrite").partitionBy("device_type").parquet(CURATED) # create a Spark job
    spark.stop()


if __name__ == "__main__":
    main()