"""Spark Structured Streaming job

Reads transactions from Kafka, computes a per-card trailing 10 minute velocity
count, enriches with the latest customer and merchant feature messages, and
writes scored transactions plus high-risk alerts back to Kafka
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import (
    array_contains,
    col,
    concat_ws,
    count,
    expr,
    from_json,
    lit,
    row_number,
    struct,
    to_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


APP_NAME = "finpulse-stream-score"
BOOTSTRAP_SERVERS = "kafka:9094"

TRANSACTIONS_TOPIC = "transactions"
CUSTOMER_FEATURES_TOPIC = "customer-features"
MERCHANT_DIRECTORY_TOPIC = "merchant-directory"
TRANSACTIONS_SCORED_TOPIC = "transactions-scored"
FRAUD_ALERTS_TOPIC = "fraud-alerts"

CHECKPOINT_DIR = "hdfs://namenode:9000/checkpoints/finpulse-stream-score"
TRANSACTION_HISTORY_PATH = "hdfs://namenode:9000/stream_state/finpulse/transactions"


TRANSACTION_SCHEMA = StructType(
    [
        StructField("txn_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("card_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("amount", StringType()),
        StructField("currency", StringType()),
        StructField("merchant_category", StringType()),
        StructField("country", StringType()),
        StructField("channel", StringType()),
        StructField("is_international", StringType()),
    ]
)

CUSTOMER_FEATURES_SCHEMA = StructType(
    [
        StructField("avg_monthly_spend", DoubleType()),
        StructField("home_country", StringType()),
        StructField("seen_countries", ArrayType(StringType())),
        StructField("unique_country_count", IntegerType()),
    ]
)

MERCHANT_FEATURES_SCHEMA = StructType(
    [
        StructField("merchant_risk_score", IntegerType()),
    ]
)


def latest_kafka_values(
    spark: SparkSession,
    topic: str,
    key_column: str,
    value_schema: StructType,
) -> DataFrame:
    """Read the newest non-null value for each Kafka key as a batch DataFrame."""

    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.where(col("value").isNotNull())
        .select(
            col("key").cast("string").alias(key_column),
            from_json(col("value").cast("string"), value_schema).alias("data"),
            col("timestamp").alias("_kafka_timestamp"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
        )
        .where(col(key_column).isNotNull() & col("data").isNotNull())
    )

    latest_window = Window.partitionBy(key_column).orderBy(
        col("_kafka_timestamp").desc(),
        col("_kafka_partition").desc(),
        col("_kafka_offset").desc(),
    )

    return (
        parsed.withColumn("_row_number", row_number().over(latest_window))
        .where(col("_row_number") == 1)
        .select(key_column, "data.*")
    )


def write_to_kafka(df: DataFrame, topic: str, key_column: str, value_columns: list[str]) -> None:
    """Write Spark DataFrame to Kafka topic"""
    (
        df.select(
            col(key_column).cast("string").alias("key"),
            to_json(struct(*[col(column) for column in value_columns])).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", topic)
        .save()
    )


def read_transaction_history(spark: SparkSession, empty_source: DataFrame) -> DataFrame:
    """Load historical transactions from Parquet.

    Returns an empty DataFrame with the expected schema when the history
    dataset has not been created yet
    """
    try:
        return spark.read.parquet(TRANSACTION_HISTORY_PATH)
    except AnalysisException:
        return (
            empty_source.select("txn_id", "card_id", "event_time")
            .withColumn("_stream_batch_id", lit(None).cast("long"))
            .limit(0)
        )


def score_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    transactions = batch_df.dropDuplicates(["txn_id"]).cache() # persist DF to memory and disk
    if transactions.rdd.isEmpty():
        transactions.unpersist() # remove DF from memory and disk
        return

    # If the batch_id for the transaction is not yet processed, process that batch and update the history
    current_history = transactions.select("txn_id", "card_id", "event_time").withColumn(
        "_stream_batch_id", lit(batch_id).cast("long")
    )
    existing_history = read_transaction_history(spark, transactions)
    batch_already_recorded = (
        not existing_history.where(col("_stream_batch_id") == lit(batch_id))
        .limit(1)
        .rdd.isEmpty()
    )

    if not batch_already_recorded:
        current_history.write.mode("append").parquet(TRANSACTION_HISTORY_PATH)
        full_history = existing_history.unionByName(current_history)
    else:
        full_history = existing_history
    
    # ---

    history = full_history.dropDuplicates(["txn_id"]).select(
        col("txn_id").alias("history_txn_id"),
        col("card_id").alias("history_card_id"),
        col("event_time").alias("history_event_time"),
    )

    batch = transactions.alias("batch") # current transactions
    trailing_history = history.alias("history") # history transactions

    # for each transaction, find all transactions for the same card_id
    # that occurred during the previous 10 minutes (including the current transaction),
    # and count them
    velocity = (
        batch.join(
            trailing_history,
            (col("batch.card_id") == col("history.history_card_id"))
            & (
                col("history.history_event_time")
                >= col("batch.event_time") - expr("INTERVAL 10 MINUTES")
            )
            & (col("history.history_event_time") <= col("batch.event_time")),
            "left",
        )
        .groupBy(col("batch.txn_id").alias("txn_id"))
        .agg(count("history.history_txn_id").cast("long").alias("velocity_count"))
    )

    # dimensions
    customer_features = latest_kafka_values(
        spark,
        CUSTOMER_FEATURES_TOPIC,
        "card_id",
        CUSTOMER_FEATURES_SCHEMA,
    )
    merchant_features = latest_kafka_values(
        spark,
        MERCHANT_DIRECTORY_TOPIC,
        "merchant_id",
        MERCHANT_FEATURES_SCHEMA,
    )

    # apply rules to transactions
    enriched = (
        transactions.join(velocity, "txn_id", "left")
        .join(customer_features, "card_id", "left")
        .join(merchant_features, "merchant_id", "left")
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("is_international", col("is_international") == lit("true"))
        .withColumn(
            "rule_high_amount",
            when(
                col("avg_monthly_spend").isNotNull()
                & (col("amount") > 3 * col("avg_monthly_spend") / 30),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn("rule_velocity", col("velocity_count") >= lit(5))
        .withColumn(
            "rule_international_mismatch",
            when(
                col("is_international")
                & (col("unique_country_count") == lit(1))
                & array_contains(col("seen_countries"), col("home_country")),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "rule_high_risk_merchant",
            when(col("merchant_risk_score") >= lit(8), lit(True)).otherwise(lit(False)),
        )
        .withColumn(
            "risk_score",
            col("rule_high_amount").cast("int")
            + col("rule_velocity").cast("int")
            + col("rule_international_mismatch").cast("int")
            + col("rule_high_risk_merchant").cast("int"),
        )
        .withColumn("predicted_fraud", col("risk_score") >= lit(2))
        .withColumn(
            "recommended_action",
            when(col("risk_score") >= lit(3), lit("block"))
            .when(col("risk_score") >= lit(2), lit("review"))
            .otherwise(lit("approve")),
        )
        .select(
            "txn_id",
            "card_id",
            "event_time",
            "merchant_id",
            "amount",
            "country",
            "channel",
            "is_international",
            "velocity_count",
            "merchant_risk_score",
            "rule_high_amount",
            "rule_velocity",
            "rule_international_mismatch",
            "rule_high_risk_merchant",
            "risk_score",
            "predicted_fraud",
            "recommended_action",
        )
        .cache()
    )

    # write scored transactions
    scored_columns = [
        "txn_id",
        "card_id",
        "event_time",
        "merchant_id",
        "amount",
        "country",
        "channel",
        "is_international",
        "velocity_count",
        "merchant_risk_score",
        "rule_high_amount",
        "rule_velocity",
        "rule_international_mismatch",
        "rule_high_risk_merchant",
        "risk_score",
        "predicted_fraud",
        "recommended_action",
    ]
    write_to_kafka(enriched, TRANSACTIONS_SCORED_TOPIC, "txn_id", scored_columns)

    # write fraud alerts
    alerts = (
        enriched.where(col("risk_score") >= lit(2))
        .withColumn(
            "triggered_rules",
            concat_ws(
                ",",
                when(col("rule_high_amount"), lit("high_amount")),
                when(col("rule_velocity"), lit("velocity")),
                when(col("rule_international_mismatch"), lit("intl_mismatch")),
                when(col("rule_high_risk_merchant"), lit("high_risk_merchant")),
            ),
        )
        .withColumn(
            "recommended_action",
            when(col("risk_score") >= lit(3), lit("block")).otherwise(lit("review")),
        )
        .select(
            "txn_id",
            "card_id",
            "event_time",
            "amount",
            "risk_score",
            "triggered_rules",
            "recommended_action",
        )
    )
    alert_columns = [
        "txn_id",
        "card_id",
        "event_time",
        "amount",
        "risk_score",
        "triggered_rules",
        "recommended_action",
    ]
    write_to_kafka(alerts, FRAUD_ALERTS_TOPIC, "txn_id", alert_columns)

    enriched.unpersist()
    transactions.unpersist()
    print(f"Scored streaming batch {batch_id}")


def main() -> None:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # read transactions
    transactions = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TRANSACTIONS_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .where(col("txn_id").isNotNull())
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .where(col("event_time").isNotNull())
        # Watermark for handling late data: dropped if event_time is older than 2 mins relative to max event_time seen so far
        .withWatermark("event_time", "2 minutes")
        .select(
            "txn_id",
            "card_id",
            "event_time",
            "merchant_id",
            "amount",
            "country",
            "channel",
            "is_international",
        )
    )

    query = (
        # called for each micro-batch (trigger not specified, defaults to as fast as possible)
        transactions.writeStream.foreachBatch(score_batch)
        .option("checkpointLocation", CHECKPOINT_DIR) # checkpoint directory for recovering from failures
        .queryName(APP_NAME) # unique query name in the associated SparkSession
        .start()
    )
    query.awaitTermination() # waits for this query terminates


if __name__ == "__main__":
    main()