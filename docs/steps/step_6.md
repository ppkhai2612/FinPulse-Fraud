# Step 6 - Offline fraud detection scoring

This step stands up [jobs/score/score_offline.py](../../jobs/score/score_offline.py) - a Spark batch job that reads the enriched transaction fact and customer feature store, applies five fraud rules, writes /`analytics/scored/`, and prints imbalance-aware evaluation metrics against `confirmed_fraud`.

Step 7 will reuse the same rule logic in Flink for real-time scoring. Step 6 proves the rules work offline before we move them into a long-lived streaming job.

## What this step delivers

| **Artifact** | **Purpose** |
|-|-|
| [jobs/score/score_offline.py](../../jobs/score/score_offline.py) | Spark batch job: rules + risk score + predictions |
| [jobs/score/check_offline_scores.py](../../jobs/score/check_offline_scores.py) | One-shot verifier for metrics and rule trigger rates |
| `/analytics/scored/`| Snappy-Parquet, one row per transaction, partitioned by `dt` |

The output schema keeps the transaction keys, the five rule flags, the aggregate score, and the label for evaluation:

```txt
txn_id, timestamp, card_id, merchant_id, amount, country, channel,
is_international, merchant_risk_score, confirmed_fraud, dt,
rule_high_amount, rule_velocity, rule_international_mismatch,
rule_unknown_device_vpn, rule_high_risk_merchant,
risk_score, predicted_fraud, recommended_action
```

## What this step does NOT do

- **No ML model yet**. This step ships interpretable rules first. A simple Spark MLlib classifier is an optional follow-up.
- **No streaming**. Velocity here is computed with a Spark window over the full offline dataset. Flink will re-implement velocity with true event-time state in Step 7.
- **No HMS registration**. Output is plain Parquet on HDFS. Step 9 registers analytics tables for Trino.
- **No Pinot writes**. Pinot consumes the Flink output topic in Step 8, not this offline scored table.

## Concepts you'll meet here

- **Rule-based detection first**. Fraud teams usually ship explicit rules before ML because rules are explainable and fast to tune.
- **Class imbalance**. Only about 1.3% of transactions are confirmed fraud in the generated seed data. Accuracy is misleading; use precision, recall, F1, and business impact instead.
- **Label noise**. `confirmed_fraud` comes from sparse fraud reports and includes false alarms. A perfect classifier cannot reach 100% recall on this label.
- **Windowed velocity offline**. The velocity rule counts how many transactions a card has in the previous 10 minutes using `Window.partitionBy("card_id").orderBy(unix_timestamp("timestamp")) .rangeBetween(-600, 0)`. This is the offline analogue of the Flink keyed window in Step 7.
- **Deduping before scoring**. Like Step 5, we collapse duplicate `txn_id`s from Kafka replays or sparse-dimension fan-out before applying rules.

## Pre-flight

```bash
# 1. Step 4 output exists.
docker compose exec namenode hdfs dfs -ls /analytics/transactions_enriched/ | head

# 2. Step 5 output exists.
docker compose exec namenode hdfs dfs -ls /analytics/customer_features/

# 3. Spark can still read HDFS.
make smoke-spark

# 4. Step 6 output does not need to exist yet.
docker compose exec namenode hdfs dfs -ls /analytics/scored/ 2>/dev/null \
    || echo "OK: /analytics/scored not yet"
```

If Step 4 or Step 5 is missing, rerun those jobs first.

## Warmup - Inspect one card's velocity pattern

Before writing the scoring job, open PySpark and look at one card with several transactions close together:

```bash
docker compose exec spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077
```

Inside the shell:

```python
from pyspark.sql.functions import col, count, unix_timestamp
from pyspark.sql.window import Window

df = (spark.read.parquet("hdfs://namenode:9000/analytics/transactions_enriched/")
      .dropDuplicates(["txn_id"])
      .filter(col("card_id") == "CARD-000001")
      .orderBy("timestamp"))

w = Window.partitionBy("card_id").orderBy(unix_timestamp("timestamp")).rangeBetween(-600, 0)
(
    df.withColumn("velocity_count", count("*").over(w))
      .select("txn_id", "timestamp", "amount", "velocity_count")
      .show(20, truncate=False)
)
```

You should see `velocity_count` climb when several transactions land within 10 minutes of each other.

Exit with `Ctrl+D`.

## 6a - Build the offline scoring job

Create [score_offline.py](../../jobs/score/score_offline.py)

The five rules from the brief:

| **Rule** | **Condition** |
|-|-|
| `rule_high_amount` | `amount > 3 * avg_monthly_spend / 30` |
| `rule_velocity` | 5+ transactions from the same card in 10 minutes |
| `rule_international_mismatch` | international txn from a card that has only ever used `home_country` |
| `rule_unknown_device_vpn` | unknown device and VPN flagged |
| `rule_high_risk_merchant` | `merchant_risk_score >= 8` |

Aggregate scoring:
- `risk_score` = sum of the five rule flags (0-5)
- `predicted_fraud` = `risk_score >= 2`
- `recommended_action` = `approve` / `review` / `block`

Submit (no `--packages` flag is needed):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/score/score_offline.py
```

Expected:

```txt
Wrote 1000000 scored rows to hdfs://namenode:9000/analytics/scored

TP=...  FP=...
FN=...  TN=...

precision=...
recall=...
f1=...
predicted_fraud_rows=...
confirmed_fraud_rows=...

avg_confirmed_fraud_amount=...
prevented_loss_estimate=...
```

## 6b - Add the verifier

Create [check_offline_scores.py](../../jobs/score/check_offline_scores.py)

Submit:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/score/check_offline_scores.py
```

Expected:
- `rows = 1000000`
- confusion matrix with non-zero TP and FP
- each rule trigger rate printed
- a small sample of predicted-fraud rows

## Verification

```bash
# 1. Scored output exists and is partitioned by dt.
docker compose exec namenode hdfs dfs -ls /analytics/scored/ | head

# 2. Verifier prints metrics and rule rates.
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/score/check_offline_scores.py

# 3. Row count matches distinct txn_id from Step 4.
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/score/score_offline.py 2>/dev/null | grep "wrote"
```