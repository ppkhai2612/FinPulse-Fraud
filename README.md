# FinPulse-Fraud

Fraud detection & transaction analytics on a HDFS / Spark / Kafka / Spark Structured Streaming / Pinot / Trino-on-HMS / Superset / Airflow stack, following the **Lambda** pattern (Kafka -> Spark Structured Streaming -> Pinot for streaming, Spark + HMS on HDFS + Trino for batch / granular DWH). The project brief is described [docs/scenario.md](docs/scenario.md).

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Source data"]
        Files["Local gzip dimension datasets<br/>data/*.csv.gz, data/*.json.gz"]
        TxnFile["transactions.csv.gz"]
    end

    subgraph Ingestion["Ingestion"]
        Land["scripts/land_data.py"]
        Producer["kafka_producers/transaction_producer.py"]
        Kafka[("Kafka")]
    end

    subgraph Lake["HDFS data lake"]
        Landing["/landing<br/>customer profiles<br/>merchant directory<br/>device fingerprints<br/>fraud reports"]
        Curated["/curated<br/>customer-profiles<br/>merchant-directory<br/>device-fingerprints<br/>fraud-reports"]
        Analytics["/analytics<br/>transactions_enriched<br/>customer_features<br/>scored"]
        StreamState["/stream_state + /checkpoints<br/>streaming history and recovery"]
    end

    subgraph Batch["Batch processing - Spark"]
        Curate["Curate dimensions"]
        Enrich["Build enriched transaction fact"]
        Features["Build customer features"]
        OfflineScore["Offline fraud scoring"]
        PinotExport["Export scored Parquet<br/>pinot-offline/scored"]
        PublishFeatures["Publish customer features"]
        PublishMerchants["Publish merchant risk scores"]
        RegisterHMS["Register HMS tables"]
    end

    subgraph Realtime["Realtime processing - Spark Structured Streaming"]
        StreamScore["stream_score.py<br/>velocity + rules scoring"]
    end

    subgraph Topics["Kafka topics"]
        TxnTopic["transactions"]
        CustomerFeaturesTopic["customer-features"]
        MerchantTopic["merchant-directory"]
        ScoredTopic["transactions-scored"]
        AlertsTopic["fraud-alerts"]
    end

    subgraph Warehouse["SQL warehouse"]
        HMS[("Hive Metastore<br/>Postgres metadata")]
        Trino["Trino coordinator"]
    end

    subgraph OLAP["Low-latency OLAP"]
        Pinot[("Apache Pinot<br/>transactions_scored hybrid table")]
    end

    subgraph Orchestration["Orchestration and monitoring"]
        Airflow["Airflow<br/>daily_batch + streaming_monitor"]
    end

    Superset["Superset dashboards"]
    Analysts["Analysts / BI users"]

    Files --> Land --> Landing
    TxnFile --> Producer --> TxnTopic
    Kafka --- TxnTopic

    Landing --> Curate --> Curated
    TxnTopic --> Enrich
    Curated --> Enrich --> Analytics
    Analytics --> Features --> Analytics
    Curated --> Features
    Analytics --> OfflineScore --> Analytics
    Analytics --> PinotExport --> Pinot
    Analytics --> RegisterHMS
    Curated --> RegisterHMS

    RegisterHMS --> HMS --> Trino --> Superset --> Analysts
    Pinot --> Superset

    Analytics --> PublishFeatures --> CustomerFeaturesTopic
    Curated --> PublishMerchants --> MerchantTopic
    CustomerFeaturesTopic --> StreamScore
    MerchantTopic --> StreamScore
    TxnTopic --> StreamScore
    StreamScore --> ScoredTopic
    StreamScore --> AlertsTopic
    StreamScore --> StreamState
    ScoredTopic --> Pinot

    Airflow -. schedules .-> Curate
    Airflow -. schedules .-> Enrich
    Airflow -. schedules .-> Features
    Airflow -. schedules .-> OfflineScore
    Airflow -. schedules .-> PinotExport
    Airflow -. schedules .-> RegisterHMS
    Airflow -. monitors .-> StreamScore
```

FinPulse follows a Lambda-style data architecture:

- **Batch path:** local source files land in HDFS, Spark curates dimensions, joins Kafka transaction facts into `analytics.transactions_enriched`, builds customer features, scores fraud rules offline, registers Parquet datasets in Hive Metastore, and serves them through Trino.
- **Realtime path:** a Kafka producer streams raw transactions into `transactions`; Spark Structured Streaming enriches each micro-batch with customer and merchant feature topics, computes fraud rules, emits scored transactions and fraud alerts, and persists stream state/checkpoints in HDFS.
- **Serving path:** Trino serves granular warehouse tables from HDFS/HMS, while Pinot serves the `transactions_scored` hybrid OLAP table from offline Parquet exports plus realtime Kafka messages. Superset connects to both serving layers.
- **Orchestration:** Airflow runs the daily batch DAG and monitors the long-running streaming scorer.

**For better viewing, please export to a PNG or SVG file**

## Data

Source datasets live in `data/` and were copied from [here](https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/05-finpulse-fraud). All files are gzip-compressed; Spark reads gzipped files (`.csv.gz` and `.json.gz` natively), so no manual `gunzip` is needed


## Getting Started

Follow these steps to bring up the local FinPulse stack, load the sample data, and run the batch and streaming pipelines.

### 1. Prerequisites

Install the following tools on your machine:

- Docker with Docker Compose v2
- Python 3.10+ for the local helper scripts
- `curl` and `bash`

Make sure Docker is running, then clone the project and enter the repository:

```bash
git clone <repo-url>
cd FinPulse-Fraud
```

The Hive Metastore needs the PostgreSQL JDBC driver in `docker/hive-metastore/jars/`. The driver is already included in this repository. If it is missing, download it with:

```bash
make hive-deps
```

On Linux, set `AIRFLOW_UID` before starting Airflow so generated files are owned by your user:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Start the Stack

Start all services with Docker Compose:

```bash
make up
```

Wait until the main services are healthy:

```bash
docker compose ps
```

Useful local UIs:

- HDFS NameNode: http://localhost:9870
- Spark master: http://localhost:8080
- Airflow: http://localhost:8081 (`airflow` / `airflow`)
- Trino: http://localhost:8086
- Pinot controller: http://localhost:9100
- Superset: http://localhost:8088 (`admin` / `admin`)

### 3. Land Source Data in HDFS

Copy the gzipped dimension datasets from `data/` into the HDFS landing zone:

```bash
python3 scripts/land_data.py
```

Verify the files landed:

```bash
docker compose exec -T namenode hdfs dfs -ls -R /landing
```

### 4. Seed Kafka Transactions

Create the `transactions` topic:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9094 \
  --create --if-not-exists \
  --topic transactions \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=-1
```

Publish sample transactions from `data/transactions.csv.gz`:

```bash
docker compose exec -T kafka-producer \
  python /opt/producers/transaction_producer.py --rate 1000 --limit 50000
```

Check that Kafka received messages:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9094 \
  --topic transactions
```

### 5. Register Pinot Tables

Register the `transactions_scored` schema plus realtime and offline table configs:

```bash
bash scripts/load_tables.sh
```

### 6. Run the Batch Pipeline

Use Airflow to run the full daily batch flow:

```bash
docker compose exec -T airflow-apiserver airflow dags unpause daily_batch
docker compose exec -T airflow-apiserver airflow dags trigger daily_batch
```

Open Airflow at http://localhost:8081 and wait for the `daily_batch` run to complete. The DAG curates dimensions, builds the enriched fact table, creates customer features, scores transactions offline, exports Pinot offline files, and ingests them into Pinot.

After the DAG succeeds, verify the Hive/Trino serving layer:

```bash
docker compose exec -T trino-coordinator trino \
  --catalog hive \
  --execute 'SHOW SCHEMAS'
```

Verify the Pinot serving layer:

```bash
curl -fsS -X POST http://localhost:8099/query/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT COUNT(*) FROM transactions_scored"}'
```

### 7. Run the Realtime Scoring Pipeline

Publish customer and merchant lookup topics that the streaming scorer uses for enrichment:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/spark/work-dir/jobs/publish/publish_customer_features.py

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/spark/work-dir/jobs/publish/publish_merchant_directory.py
```

Start the Spark Structured Streaming scorer:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/spark/work-dir/jobs/kafka_consumer/stream_score.py
```

In another terminal, send more transactions while the streaming job is running:

```bash
docker compose exec -T kafka-producer \
  python /opt/producers/transaction_producer.py --rate 200 --limit 10000
```

Inspect scored transactions or fraud alerts:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9094 \
  --topic transactions-scored \
  --from-beginning \
  --max-messages 5

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9094 \
  --topic fraud-alerts \
  --from-beginning \
  --max-messages 5
```

### 8. Run Smoke Tests

Run smoke tests for the core services:

```bash
make smoke
```

### 9. Stop or Reset

Stop containers while keeping data volumes:

```bash
docker compose down
```

Remove containers and volumes for a clean reset:

```bash
docker compose down -v
```

## References

- [Data architecture 101](https://vutr.substack.com/p/data-architecture-101)
- [The Hadoop Distributed File System](https://vutr.substack.com/p/i-spent-8-hours-reading-the-paper-523)
- [The Overview Of Apache Spark](https://vutr.substack.com/p/the-overview-of-apache-spark)
- [If you're learning Kafka, this article is for you](https://vutr.substack.com/p/if-youre-learning-kafka-this-article)
- [A glimpse of Apache Pinot, the real-time OLAP system from LinkedIn](https://vutr.substack.com/p/a-glimpse-of-apache-pinot-the-real)
- [What is Apache Hive?](https://vutr.substack.com/p/what-is-apache-hive)
- [8 minutes to understand Presto](https://vutr.substack.com/p/8-minutes-to-understand-presto)
- [Data engineering system design: 11 data sourcing problems](https://vutr.substack.com/p/data-engineering-system-design-11)
- [Data engineering system design: 9 data serving problems](https://vutr.substack.com/p/data-engineering-system-design-9)
- [Data engineering system design: 9 data processing problems](https://vutr.substack.com/p/data-engineering-system-design-9-4c5)
- [Data Engineering System Design: Orchestration + Apache Airflow](https://vutr.substack.com/p/data-engineering-system-design-orchestration)
