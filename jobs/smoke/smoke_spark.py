from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode


def main():
    spark = SparkSession.builder.appName("finpulse-smoke-spark").getOrCreate()
    
    # after split(), the result looks [alpha, bravo, charlie]
    # explode(): construct a new row for each element in the given array
    df = spark.read.text("hdfs://namenode:9000/smoke/words.txt")
    words = df.select(explode(split(col("value"), r"\s+")).alias("word"))
    
    total = words.count()
    distinct = words.distinct().count()
    print(
        f"Total Words: {total}. "
        f"Distinct Words: {distinct}"
    )
    spark.stop()


if __name__ == "__main__":
    main()