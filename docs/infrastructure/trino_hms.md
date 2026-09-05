# Trino + Hive Metastore

Trino is the DWH serving layer for granular Parquet that Spark writes into HDFS under `/curated/*` and `/analytics/*`. It complements Pinot, which serves the pre-aggregated streaming layer

Trino talks to the Hive Metastore (HMS) over Thrift to discover what tables exist; HMS talks to a dedicated Postgres for catalog storage; data bytes still live in HDFS. This is the lakehouse pattern - three independent concerns: storage (HDFS), metadata (HMS-on-Postgres), engines (Spark + Trino).

## Topology

| **Service** | **Image** | **Hostname** | **Host -> container ports** | **What other services talk to it** |
|-|-|-|-|-|
||||||

## Configuration

## Volumes


## Healthcheck

## Why this shape?


## Alternatives


## References

- [8 minutes to understand Presto](https://vutr.substack.com/p/8-minutes-to-understand-presto)
- [What is Apache Hive?](https://vutr.substack.com/p/what-is-apache-hive)