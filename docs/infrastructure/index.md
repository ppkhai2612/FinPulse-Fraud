# Infrastructure reference

Per-component reference for every service in [docker-compose.yml](../../docker-compose.yml) - image, ports, volumes, configuration, healthcheck, and the why behind each non-obvious choice. It answers **"how is component X wired?"**

## Components

| **Doc** | **What is covers** | **Compose services** |
|-|-|-|
| [hdfs.md](hdfs.md) | Distributed FS for the four dimension datasets and `/analytics` | `namenode`, `datanode-1`, `datanode-2` |
| [spark.md](spark.md) | Batch compute - Kafka + HDFS dim joins, feature store, scoring	| `spark-master`, `spark-worker-1`, `spark-worker-2` |
| [kafka.md](kafka.md) | Source of truth for the `transactions` fact stream + UI | `kafka`, `kafdrop` |
| [airflow.md](airflow.md) | Orchestrator for the nightly batch + monitoring DAGs | `postgres`, `airflow-init`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` |
| [flink.md](flink.md) | Streaming scoring - event-time, exactly-once with Kafka | `flink-jobmanager`, `flink-taskmanager` |
| [pinot.md](pinot.md) | Real-time OLAP - pre-aggregated streaming + offline hybrid table | `pinot-zookeeper`, `pinot-controller`, `pinot-broker`, `pinot-server` |
| [trino_hms.md](trino_hms.md) | DWG serving layer - granular Parquet via Hive Metastore | `metastore-db`, `hive-metastore-init`, `hive-metastore`, `trino-coordinator` |
| [superset.md](superset.md) | BI / dashboards on top of Pinot and Trino | `superset-init`, `superset` |

## Port map (host <-> service)

| **Service** | **Host port** | **Container port** | **Notes** |
|-|-|-|-|
| `namenode` | 9870 | 9870 | HDFS NameNode UI: http://localhost:9870 |
| `namenode` | 9000 | 9000 | HDFS NameNode RPC, for `hdfs://namenode:9000` clients |
| `spark-master` | 8080 | 8080 | Spark Master UI: http://localhost:8080 |
| `spark-master` | 7077 | 7077 | Spark Master RPC: `spark://spark-master:7077` |
| `kafka` | 9092 | 9092 | Kafka broker - host clients: `localhost:9092`;  in-network: `kafka:9094` |
| `kafdrop` | 9001 | 9000 | http://localhost:9001 |
|||||

## Profile groups

[Makefile](../../Makefile) ships three subset bring-up targets that match natural component clusters. Use them when you only need part of the stack and want to skip the ~12 GB resident-memory cost of the full `make up`.

| **Target** | **Components started** | **Use when...** |
|-|-|-|
| `make up-core` | HDFS, Spark, Kafka | Working on Spark batch jobs or Kafka producers |
| `make up-stream` | Kakfa, Flink | Working on the Flink streaming app |
| `make up-bi` | Pinot, Superset, HMS + Trino | Working on dashboards or ad-hoc DWH SQL |
| `make up-dwh` | Postgres-backed HMS | Inspecting / debugging the catalog in isolation |
| `make up` | Everything | Full integration tests, `make smoke`, demos |

## Per-component smoke tests

Each component has a smoke target in [Makefile](../../Makefile) that runs an end-to-end probe inside the running stack. They're the first thing to try when something feels off.

```bash
make smoke-hdfs       # put / ls / cat / rm round-trip
make smoke-kafka      # create + produce + consume on an ephemeral topic
make smoke-spark      # spark-submit a job that reads HDFS
make smoke-airflow    # trigger smoke_dag and wait for success
make smoke-pinot      # /health on controller + broker, instance registration
make smoke-flink      # /overview on jobmanager + ≥ 1 taskmanager registered
make smoke-trino      # /v1/info + hive catalog + Spark<->HMS<->Trino round-trip
make smoke            # all of the above
```

The exact probes live in [smoke.sh](../../scripts/smoke.sh).

## Volumes and make nuke

Every component except Kafdrop, the Airflow init container, and the Superset init container persists state to a named Docker volume. make nuke runs `docker compose down -v` and **deletes every named volume**, which means HDFS data, Kafka topics + offsets, Postgres (Airflow metadata), Pinot ZK + controller + server data, the Superset SQLite metadata DB, and Flink checkpoints + savepoints all go away.