from pyspark.sql import SparkSession


LANDING = "hdfs://namenode:9000/landing/customer-profiles/customer-profiles.json.gz"
CURATED = "hdfs://namenode:9000/curated/customer-profiles/"


def main():
    
    spark = SparkSession.builder.appName("finpulse-curate-customers").getOrCreate()
    df = spark.read.option("multiline", "true").json(LANDING)
    df.write.mode("overwrite").parquet(CURATED) # create a Spark job
    spark.stop()


if __name__ == "__main__":
    main()