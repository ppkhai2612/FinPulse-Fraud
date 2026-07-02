from pyspark.sql import SparkSession


LANDING = "hdfs://namenode:9000/landing/merchant-directory/merchant-directory.csv.gz"
CURATED = "hdfs://namenode:9000/curated/merchant-directory"


def main():

    spark = SparkSession.builder.appName("finpulse-curate-merchants").getOrCreate()
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(LANDING)
    df.write.mode("overwrite").parquet(CURATED) # create a Spark job
    spark.stop()


if __name__ == "__main__":
    main()