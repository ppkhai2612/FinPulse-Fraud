# FinPulse-Fraud — End-to-End Implementation Plan

## Context

The infrastructure is up and `make smoke` passes. The full local stack is:

- **HDFS** (1 NameNode + 2 DataNodes) — dim landing + Spark analytics outputs.
- **Spark** (1 master + 2 workers) — batch consumer of Kafka `transactions`

    - HDFS dim joins + Pinot offline-segment generation.

- **Kafka** (single broker, KRaft) — source of truth for the transaction fact stream. Three topics: `transactions`, `transactions-scored`, `fraud-alerts`.
- **Flink** (1 jobmanager + 1 taskmanager, 4 slots) — streaming consumer of Kafka transactions, writes scored events back to Kafka.
- **Pinot** (zookeeper + controller + broker + server) — real-time OLAP serving layer; will host the `transactions_scored` hybrid table (pre-aggregated, real-time from Kafka + offline from HDFS).
- **Hive Metastore + Trino** (`metastore-db` + `hive-metastore` + `trino-coordinator`) — DWH serving layer for the granular Parquet in `/curated/*` and `/analytics/*`. Spark saveAsTable registers tables in HMS over Thrift; Trino reads them via the Hive connector.
- **Superset** — BI front-end on Pinot (`pinotdb`) and Presto (`pyhive[trino]`) via two separate SQLAlchemy drivers.
- **Airflow** (LocalExecutor) — orchestrates the daily Spark batch DAG and monitors the long-running Flink job.

The 5 source datasets (1M transactions, 100K customers, 600K device sessions, 15K fraud reports, 10K merchants) are sitting in `data/` as gzipped CSV/JSON.

The brief in [docs/scenario.md](../scenario.md) defines four stages (HDFS Lake -> Spark Batch -> Kafka Streaming -> Airflow Orchestration) and seven business questions to answer. The rubric weights **feature engineering depth**, **class-imbalance awareness**, **real-time architecture quality**, and **dollar-impact framing** — not pure accuracy.

This plan turns that brief into **12 small, observable steps** so each new concept lands one at a time. Steps 1-7 + 11-12 implement the four brief stages; Steps 8-10 add the two complementary serving layers — Pinot for pre-aggregated streaming (Step 8), Trino-on-HMS for granular ad-hoc (Step 9), Superset on top of both (Step 10). Neither serving layer is strictly required by the brief, but together they make the real-time architecture credit easier to demonstrate and let the analysis notebook share a catalog with the dashboards. Every step ends with something runnable and a single command to verify it. The end-to-end shape lives in [docs/plans/data_flow.md](../plans/data_flow.md) — read that diagram before starting any step.

## At a glance

| Step | Output | New concepts |
|-|-|-|
| 0 | infra healthy | (no) |
| 1 | `/landing/{customers,merchants,devices,fraud-reports}/` raw `.gz` | zones, replication factors |
||||
||||
||||
||||
||||
||||
||||
||||
||||
||||