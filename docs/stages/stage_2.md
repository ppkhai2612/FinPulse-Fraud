# Stage 2: Batch Transformation Pipeline

## Tasks

- Read raw data from the HDFS landing zone using PySpark
- Clean, standardize, and validate the data (handle nulls, fix types, deduplicate)
- Join multiple data sources to create enriched datasets
- Aggregate data to answer the company's business questions
- Write curated and analytics outputs as Parquet with appropriate partitioning

## Implementation

### Spark curate jobs

- Spark curate jobs are written in `jobs/curate/*.py` in which each job reads from the `landing/` (`.gz` file) and writes to the `curated/` (`.parquet` file). 
- To submit Spark curate jobs, run `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/curate/curate_*.py`. Note: must have to run each curate job individually
- After Spark curate jobs completed, run `docker compose exec namenode hdfs dfs -du -h /curated/*/*` to display sizes of files in `curated/` directory, a note that the second column in result is disk space consumed with all replicas (e.g., 2 replicas)

    ```bash
    0        0        /curated/customer-profiles/_SUCCESS
    2.3 M    4.5 M    /curated/customer-profiles/part-00000-3c865671-d3dc-48fd-8fd0-4058e685da80-c000.snappy.parquet
    0        0        /curated/device-fingerprints/_SUCCESS
    3.2 M    6.4 M    /curated/device-fingerprints/device_type=desktop/part-00000-53870f9a-43c9-4b90-b766-439cea8410a8.c000.snappy.parquet
    3.6 M    7.2 M    /curated/device-fingerprints/device_type=mobile/part-00000-53870f9a-43c9-4b90-b766-439cea8410a8.c000.snappy.parquet
    1.2 M    2.4 M    /curated/device-fingerprints/device_type=tablet/part-00000-53870f9a-43c9-4b90-b766-439cea8410a8.c000.snappy.parquet
    0        0        /curated/fraud-reports/_SUCCESS
    72.9 K   145.9 K  /curated/fraud-reports/fraud_type=account_takeover/part-00000-c26ad385-91f7-4085-be57-3ab2f496e125.c000.snappy.parquet
    193.8 K  387.5 K  /curated/fraud-reports/fraud_type=card_not_present/part-00000-c26ad385-91f7-4085-be57-3ab2f496e125.c000.snappy.parquet
    41.2 K   82.4 K   /curated/fraud-reports/fraud_type=counterfeit/part-00000-c26ad385-91f7-4085-be57-3ab2f496e125.c000.snappy.parquet
    23.1 K   46.3 K   /curated/fraud-reports/fraud_type=identity_theft/part-00000-c26ad385-91f7-4085-be57-3ab2f496e125.c000.snappy.parquet
    0        0        /curated/merchant-directory/_SUCCESS
    201.2 K  402.4 K  /curated/merchant-directory/part-00000-93c1c9d7-110f-487f-a3f1-57351aadf580-c000.snappy.parquet
    ```

### Spark enrich jobs

- Submit `enrich/build_enriched_fact.py` job to Spark master (note that the package needs to be included for Spark to read data from Kafka topic): `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /opt/spark/work-dir/jobs/enrich/build_enriched_fact.py`
- Submit `enrich/check_fraud_rate.py` job to preliminary check of the fraud rows and fraud rate: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/enrich/check_fraud_rate.py`

    ```bash
    +-------+----------+----------+
    |total  |fraud_rows|fraud_rate|
    +-------+----------+----------+
    |1000000|12671     |0.012671  |
    +-------+----------+----------+
    ```

### Spark customer features jobs

- Submit Spark `build_customer_features.py` job to add customer features to customer profiles from `curated/customer-profiles/`: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/features/build_customer_features.py`
- Submit Spark `check_customer_features.py` job: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/features/check_customer_features.py`. It runs:

    - Feature aggregation checks

        ```bash
        +------+--------------+-------------+-----------------+------------------+---------------------+-------------------+
        |rows  |distinct_cards|max_txn_count|avg_txn_count    |avg_of_avg_amount |avg_pct_international|avg_pct_online     |
        +------+--------------+-------------+-----------------+------------------+---------------------+-------------------+
        |100000|100000        |27           |10.00030000900027|427.07923111033296|0.7110643581115046   |0.40019606252580797|
        +------+--------------+-------------+-----------------+------------------+---------------------+-------------------+
        ```
    
    - Duplicate card_id rows; expect no output

        ```bash
        +-------+-----+
        |card_id|count|
        +-------+-----+
        +-------+-----+
        ```

    - Amount and count distributions

        ```bash

        +-------+-----------------+------------------+------------------+-----------------+---------------------+--------------------+------------------+-------------------+
        |summary|txn_count        |avg_amount        |stddev_amount     |p95_amount       |unique_merchant_count|unique_country_count|pct_international |pct_online         |
        +-------+-----------------+------------------+------------------+-----------------+---------------------+--------------------+------------------+-------------------+
        |count  |99997            |99997             |99997             |99997            |99997                |99997               |99997             |99997              |
        |mean   |10.00030000900027|427.0792311103331 |357.618063747289  |1124.874055221662|9.896996909907298    |4.544996349890496   |0.7110643581115066|0.40019606267839775|
        |stddev |3.160518860120268|135.43632221085625|182.47336234146982|610.0907162235955|3.110989844411728    |1.3899011339815404  |0.251461495759356 |0.16439935261316746|
        |min    |1                |16.21             |0.0               |16.21            |1                    |1                   |0.0               |0.0                |
        |max    |27               |2716.72           |3274.230666797927 |7372.81          |28                   |11                  |1.0               |1.0                |
        +-------+-----------------+------------------+------------------+-----------------+---------------------+--------------------+------------------+-------------------+
        ```

    - Sample rows

        ```bash
        +-----------+---------+------------------+----------+---------------------+------------------------+------------------+-------------------+
        |card_id    |txn_count|avg_amount        |p95_amount|unique_merchant_count|seen_countries          |pct_international |pct_online         |
        +-----------+---------+------------------+----------+---------------------+------------------------+------------------+-------------------+
        |CARD-000001|12       |549.8091666666666 |2560.73   |12                   |[CA, DE, UK, US, AU]    |0.6666666666666666|0.5833333333333334 |
        |CARD-000002|13       |417.5984615384616 |990.07    |13                   |[NG, CA, DE, UK, US, IN]|0.7692307692307693|0.46153846153846156|
        |CARD-000003|14       |486.6892857142857 |1298.17   |14                   |[FR, UK, US]            |1.0               |0.42857142857142855|
        |CARD-000005|15       |527.9026666666666 |1395.99   |14                   |[CA, DE, US, JP, MX]    |1.0               |0.3333333333333333 |
        |CARD-000012|6        |400.49666666666667|686.34    |6                    |[DE, FR, UK, US]        |0.5               |0.3333333333333333 |
        +-----------+---------+------------------+----------+---------------------+------------------------+------------------+-------------------+
        ```

### Understanding some ML concepts

- **Rule-based and ML**: Rules are interpretable and ship instantly; ML adds lift but needs explanation. Real fraud teams ship rules first, then layer ML. Five rules will be applied for transactions

    | Rule | Condition | Meaning |
    |-|-|-|
    | `rule_high_amount` | amount > (avg_monthly_spend * 3 / 30) | Customers spend too much in a single transaction |
    | `rule_velocity` | 5+ transactions from the same card in 10 minutes | Too many transactions in a short period of time |
    | `rule_international_mismatch` | international transaction from a card that has only ever used home_country | A sudden change in the cardholder's geographic spending behavior |
    | `rule_unknown_device_vpn` | unknown device and VPN flagged | transaction originates from an unknown device and is routed through a VPN |
    | `rule_high_risk_merchant` | merchant_risk_score >= 8 | transaction with high risk merchant |

- **Confusion Matrix**: a simple table used to measure how well a classification model is performing. It compares the predictions made by the model with the actual results and shows where the model was right or wrong. It breaks down the predictions into four categories

    - **True Positive (TP)**: The model correctly predicted a positive outcome i.e the actual outcome was positive.
    - **True Negative (TN)**: The model correctly predicted a negative outcome i.e the actual outcome was negative.
    - **False Positive (FP)**: The model incorrectly predicted a positive outcome i.e the actual outcome was negative. It is also known as a Type I error
    - **False Negative (FN)**: The model incorrectly predicted a negative outcome i.e the actual outcome was positive. It is also known as a Type II error

### Spark score offline job

- Submit Spark `score_offline` job to scoring transactions offline with rule-based fraud detection: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/score/score_offline.py`

    - `percentile_approx("amount", 0.95)` function: if this function returns 800, it means that approximately 95% of the total values ​​in the "amount" column are less than or equal to 800, and the remaining approximately 5% is greater than or equal to

- Submit Spark `check_offline_scores` job: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/score/check_offline_scores.py`. It prints some useful information:

    ```bash
    Scored transaction summary:
    +-------+--------------+--------------------+--------------------+
    |rows   |avg_risk_score|predicted_fraud_rows|confirmed_fraud_rows|
    +-------+--------------+--------------------+--------------------+
    |1000000|0.606557      |37924               |12671               |
    +-------+--------------+--------------------+--------------------+

    Confusion matrix
    TP=1130, FP=36794, FN=11541, TN=950535
    Precision=0.0298. Recall=0.0892. F1 Score=0.0447
    Prevented Loss Estimate=1,474,774.48
    Rule rates:
    +----------------+-------------+---------------------------+-----------------------+-----------------------+
    |rule_high_amount|rule_velocity|rule_international_mismatch|rule_unknown_device_vpn|rule_high_risk_merchant|
    +----------------+-------------+---------------------------+-----------------------+-----------------------+
    |0.535078        |0.0          |0.0                        |0.005447               |0.066032               |
    +----------------+-------------+---------------------------+-----------------------+-----------------------+

    Risk score distribution:
    +----------+------+
    |risk_score| count|
    +----------+------+
    |         0|431558|
    |         1|530518|
    |         2| 37733|
    |         3|   191|
    +----------+------+

    Sample scored rows:
    +-----------+-----------+-------+----------+---------------+---------------+------------------+----------------+-------------+---------------------------+-----------------------+-----------------------+
    |txn_id     |card_id    |amount |risk_score|predicted_fraud|confirmed_fraud|recommended_action|rule_high_amount|rule_velocity|rule_international_mismatch|rule_unknown_device_vpn|rule_high_risk_merchant|
    +-----------+-----------+-------+----------+---------------+---------------+------------------+----------------+-------------+---------------------------+-----------------------+-----------------------+
    |TXN-0568458|CARD-001932|1006.9 |2         |true           |false          |review            |true            |false        |false                      |false                  |true                   |
    |TXN-0732965|CARD-002026|393.81 |2         |true           |false          |review            |true            |false        |false                      |false                  |true                   |
    |TXN-0815797|CARD-002639|1035.98|2         |true           |false          |review            |true            |false        |false                      |false                  |true                   |
    |TXN-0697668|CARD-003135|257.59 |2         |true           |false          |review            |true            |false        |false                      |false                  |true                   |
    |TXN-0659146|CARD-003473|262.75 |2         |true           |false          |review            |true            |false        |false                      |false                  |true                   |
    +-----------+-----------+-------+----------+---------------+---------------+------------------+----------------+-------------+---------------------------+-----------------------+-----------------------+
    ```

## Troubleshooting

- **Transactions enriched (after joined) produce over 1M rows** (see text `Wrote 1149336 rows to hdfs://namenode:9000/analytics/transactions_enriched` in the terminal)
    
    - The reason is that in `device-fingerprints.csv`, a single transaction (identified by `txn_id`) can have multiple `session_ids`. Therefore, to ensure that a transaction has only one session_id assigned to it, I use a window function to retrieve only the line with the smallest session_id (`rn = 1`)

        ```python
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number

        window = Window.partitionBy("txn_id").orderBy("session_id") # get a Window
        devices = spark \
            .read.parquet(f"{CURATED}/device-fingerprints/") \
            .select(
                "session_id", "txn_id", "device_type", "os",
                "browser", "ip_country", "ip_city",
                "is_vpn", "is_known_device", "login_attempt_count"
            ).withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")
        ```
    
    - So, after the join, transaction enriched has 1M rows (you see text `Wrote 1000000 rows to hdfs://namenode:9000/analytics/transactions_enriched` in the terminal)