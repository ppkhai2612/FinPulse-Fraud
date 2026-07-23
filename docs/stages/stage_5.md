# Stage 5: Serving Layer

## Apache Pinot

- In Pinot, there's a concept called **hybrid table**, it contains data loaded from both a batch source and a streaming source

- With batch source, data is ingested from a shared file system between Spark and Pinot. To export data to this file system, submit a Spark `export_pinot_offline.py` job: `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/jobs/score/export_pinot_offline.py`. After the job is completed, you should see the `pinot-offline/scored/` directory with the exported data. You can check with

    ```bash
    tree pinot-offline/

    pinot-offline/
    └── scored
        ├── part-00000-d37e82f9-7415-47cb-baed-504a150ae54c-c000.snappy.parquet
        └── _SUCCESS
    ```

Run the Pinot ingestion batch job

cp pinot_conf/offline_ingestion_job.yaml pinot-offline/

`docker compose exec pinot-controller bin/pinot-admin.sh LaunchDataIngestionJob -jobSpecFile /opt/pinot/data/pinot-offline/offline_ingestion_job.yaml`

curl -fsS -X POST http://localhost:8099/query/sql \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) FROM transactions_scored"}'

```json
{
  "resultTable": {
    "dataSchema": {
      "columnNames": [
        "count(*)"
      ],
      "columnDataTypes": [
        "LONG"
      ]
    },
    "rows": [
      [
        994463
      ]
    ]
  },
  "numRowsResultSet": 1,
  "partialResult": false,
  "exceptions": [],
  "numGroupsLimitReached": false,
  "timeUsedMs": 20,
  "requestId": "1899055845000000009",
  "brokerId": "Broker_pinot-broker_8099",
  "numDocsScanned": 994463,
  "totalDocs": 1000000,
  "numEntriesScannedInFilter": 1000000,
  "numEntriesScannedPostFilter": 0,
  "numServersQueried": 2,
  "numServersResponded": 2,
  "numSegmentsQueried": 7,
  "numSegmentsProcessed": 1,
  "numSegmentsMatched": 1,
  "numConsumingSegmentsQueried": 6,
  "numConsumingSegmentsProcessed": 0,
  "numConsumingSegmentsMatched": 0,
  "minConsumingFreshnessTimeMs": 1784285164299,
  "numSegmentsPrunedByBroker": 0,
  "numSegmentsPrunedByServer": 6,
  "numSegmentsPrunedInvalid": 0,
  "numSegmentsPrunedByLimit": 0,
  "numSegmentsPrunedByValue": 0,
  "brokerReduceTimeMs": 0,
  "offlineThreadCpuTimeNs": 0,
  "realtimeThreadCpuTimeNs": 0,
  "offlineSystemActivitiesCpuTimeNs": 0,
  "realtimeSystemActivitiesCpuTimeNs": 0,
  "offlineResponseSerializationCpuTimeNs": 0,
  "realtimeResponseSerializationCpuTimeNs": 0,
  "offlineTotalCpuTimeNs": 0,
  "realtimeTotalCpuTimeNs": 0,
  "explainPlanNumEmptyFilterSegments": 0,
  "explainPlanNumMatchAllFilterSegments": 0,
  "traceInfo": {}
}
```

```bash
curl -fsS -X POST http://localhost:8099/query/sql \
    -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT recommended_action, count(*) FROM transactions_scored GROUP BY recommended_action"}'
```

```json
{
  "resultTable": {
    "dataSchema": {
      "columnNames": [
        "recommended_action",
        "count(*)"
      ],
      "columnDataTypes": [
        "STRING",
        "LONG"
      ]
    },
    "rows": [
      [
        "approve",
        956762
      ],
      [
        "review",
        37512
      ],
      [
        "block",
        189
      ]
    ]
  },
  "numRowsResultSet": 3,
  "partialResult": false,
  "exceptions": [],
# expect: schema + REALTIME + OFFLINE "successfully added", then
#         {"tables":["transactions_scored"]}

  "numGroupsLimitReached": false,
  "timeUsedMs": 88,
  "requestId": "1899055845000000008",
  "brokerId": "Broker_pinot-broker_8099",
  "numDocsScanned": 994463,
  "totalDocs": 1000000,
  "numEntriesScannedInFilter": 1000000,
  "numEntriesScannedPostFilter": 994463,
  "numServersQueried": 2,
  "numServersResponded": 2,
  "numSegmentsQueried": 7,
  "numSegmentsProcessed": 1,
  "numSegmentsMatched": 1,
  "numConsumingSegmentsQueried": 6,
  "numConsumingSegmentsProcessed": 0,
  "numConsumingSegmentsMatched": 0,
  "minConsumingFreshnessTimeMs": 1784285116501,
  "numSegmentsPrunedByBroker": 0,
  "numSegmentsPrunedByServer": 6,
  "numSegmentsPrunedInvalid": 0,
  "numSegmentsPrunedByLimit": 0,
  "numSegmentsPrunedByValue": 0,
  "brokerReduceTimeMs": 8,
  "offlineThreadCpuTimeNs": 0,
  "realtimeThreadCpuTimeNs": 0,
  "offlineSystemActivitiesCpuTimeNs": 0,
  "realtimeSystemActivitiesCpuTimeNs": 0,
  "offlineResponseSerializationCpuTimeNs": 0,
  "realtimeResponseSerializationCpuTimeNs": 0,
  "offlineTotalCpuTimeNs": 0,
  "realtimeTotalCpuTimeNs": 0,
  "explainPlanNumEmptyFilterSegments": 0,
  "explainPlanNumMatchAllFilterSegments": 0,
  "traceInfo": {}
}
```




## Trino

According to [this document](https://trino.io/docs/current/connector/hive.html#requirements), for Trino to be able to query data contained in the Apache Hive data warehouse, it needs:
- Data files in varying formats, that are typically stored in the HDFS or in object storage systems such as Amazon S3
- Metadata about how the data files are mapped to schemas and tables. This metadata is stored in a database, such as MySQL, and is accessed via the Hive metastore service

In this project, Trino will query files in `curated/*` and `analytics/*`, so these files will be wrapped by the metadata contained in the Hive Metastore


## Hive Metastore

### Decisions: Why managed tables, not external tables in HMS

- **Concept**: [Managed vs External tables](https://hive.apache.org/docs/latest/language/managed-vs--external-tables/)

    - A **managed table** is stored under the `hive.metastore.warehouse.dir` path property. The default location can be overridden by the location property during table creation. If a managed table or partition is dropped, the data and metadata associated with that table or partition are deleted. Use managed tables **when Hive should manage the lifecycle of the table**, or when generating temporary tables
    - An **external table** describes the metadata / schema on external files. External table files can be accessed and managed by processes outside of Hive. External tables can access data stored in sources such as Azure Storage Volumes (ASV) or remote HDFS locations. If the structure or partitioning of an external table is changed, an `MSCK REPAIR TABLE table_name` statement can be used to refresh metadata information. Use external tables **when files are already present or in remote locations, and the files should remain even if the table is dropped**

- This step uses `saveAsTable` from Spark because it is the same path the `make smoke-trino` round-trip already proves works, and it sidesteps the SerDe / partition-discovery friction of hand-writing external-table DDL for partitioned Parquet

- `saveAsTable`

  - Writes Parquet into the Hive warehouse dir (`hdfs://namenode:9000/warehouse/<db>.db/<table>/`), and registers the table (and its partitions) in HMS over Thrift, in one call
  - These are managed tables, so the data is copied into the warehouse rather than referenced in place - a deliberate trade-off for reliability at this scale. `DROP TABLE` would remove the warehouse copy, not the original `/curated` or `/analytics` Parquet


The plan sketches two ways to register tables - saveAsTable from Spark or CREATE EXTERNAL TABLE from the Presto CLI. 

docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/warehouse/register_hms_tables.py

```bash
Registered curated.customer_profiles: 100000 rows
Registered curated.merchant_directory: 10000 rows
Registered curated.device_fingerprints: 600000 rows (partitioned by device_type)
Registered curated.fraud_reports: 15000 rows (partitioned by fraud_type)
Registered analytics.transactions_enriched: 1000000 rows (partitioned by dt)
Registered analytics.customer_features: 100000 rows
Registered analytics.scored: 1000000 rows (partitioned by dt)

Databases now in HMS:
+---------+
|namespace|
+---------+
|analytics|
|curated  |
|default  |
+---------+
```


```bash
docker compose exec trino-coordinator trino --catalog hive \
    --execute 'SHOW SCHEMAS'

# "analytics"
# "curated"
# "default" # created in smoke_trino
# "information_schema"
```

```bash
docker compose exec trino-coordinator trino --catalog hive \
    --schema analytics --execute 'SHOW TABLES'

# "customer_features"
# "scored"
# "transactions_enriched"
```


```bash
# Granular cross-schema join with partition pruning - the access pattern
# Pinot cannot serve (no row-level joins).
docker compose exec trino-coordinator trino --catalog hive --execute "
SELECT m.category, count(*) AS txn_count
FROM analytics.transactions_enriched t
JOIN curated.merchant_directory m ON t.merchant_id = m.merchant_id
WHERE t.dt = DATE '2025-03-14'
GROUP BY m.category
ORDER BY txn_count DESC
LIMIT 5"
```

```bash
"grocery","808"
"retail","788"
"restaurant","720"
"gas_station","570"
"online_marketplace","559"
```


## Superset

docker compose cp docker/superset/register_superset.py superset:/app/register_superset.py
docker compose exec superset python /app/register_superset.py


Dashboard 1 - Live fraud-rate monitor (Pinot)
Dataset: transactions_scored (Pinot). Set the dashboard auto-refresh to 10s. Charts:

-- Fraud rate, last 60 minutes (Big Number)
SELECT CAST(SUM(CASE WHEN predicted_fraud THEN 1 ELSE 0 END) AS DOUBLE)
       / count(*) AS fraud_rate
FROM transactions_scored
WHERE event_time > ago('PT60M');

-- Alert volume by action (Pie)
SELECT recommended_action, count(*)
FROM transactions_scored
GROUP BY recommended_action;
Pinot wins here: sub-second on a fixed schema, refreshed every few seconds.

Dashboard 2 - Per-rule trigger analysis (Pinot)
Dataset: transactions_scored (Pinot). One Big Number / bar per rule:

SELECT
  SUM(CASE WHEN rule_high_amount        THEN 1 ELSE 0 END) AS high_amount,
  SUM(CASE WHEN rule_velocity           THEN 1 ELSE 0 END) AS velocity,
  SUM(CASE WHEN rule_intl_mismatch      THEN 1 ELSE 0 END) AS intl_mismatch,
  SUM(CASE WHEN rule_high_risk_merchant THEN 1 ELSE 0 END) AS high_risk_merchant
FROM transactions_scored;
Dashboard 3 - Cross-segment fraud breakdown (Presto)
Dataset: transactions_enriched (Presto), joined to merchant_directory and customer_profiles. This is the free-form, multi-table drill-down Pinot cannot serve:

SELECT m.category,
       t.country,
       count(*)                                            AS txns,
       SUM(CASE WHEN t.confirmed_fraud THEN 1 ELSE 0 END)  AS frauds,
       CAST(SUM(CASE WHEN t.confirmed_fraud THEN 1 ELSE 0 END) AS DOUBLE)
           / count(*)                                      AS fraud_rate
FROM analytics.transactions_enriched t
JOIN curated.merchant_directory m ON t.merchant_id = m.merchant_id
GROUP BY m.category, t.country
ORDER BY fraud_rate DESC
LIMIT 50;
Export each finished dashboard (Settings -> Dashboards -> ... -> Export) to docker/superset/dashboards/ so they re-import on a fresh stack.

