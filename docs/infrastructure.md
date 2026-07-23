# FinPulse-Fraud Infrastructure

This documentation explains the services and containers in `docker-compose.yml`

## Overview

- **HDFS (1 NameNode + 2 DataNodes)** — dim landing + Spark curated/analytics outputs
- **Spark (1 master + 2 workers)** — batch consumer of Kafka transactions
HDFS dim joins + Pinot offline-segment generation
- **Kafka (single broker, KRaft)** — source of truth for the transaction fact stream. Three topics: transactions, transactions-scored, fraud-alerts
- **Pinot (zookeeper + controller + broker + server)** — real-time OLAP serving layer; will host the transactions_scored hybrid table (pre-aggregated, real-time from Kafka + offline from HDFS)
- **Hive Metastore + Trino (metastore-db + hive-metastore + trino-coordinator)** — DWH serving layer for the granular Parquet in /curated/* and /analytics/*. Spark saveAsTable registers tables in HMS over Thrift; Trino reads them via the Hive connector
Superset — BI front-end on Pinot (pinotdb) and Trino (pyhive[Trino]) via two separate SQLAlchemy drivers
- **Airflow (LocalExecutor)** — orchestrates the daily Spark batch DAG and monitors the long-running Spark-Structured Streaming job

## HDFS - Distributed File System

### Containers

| Container | Role |
|-|-|
| `namenode` | Managing the file system namespace and regulates access to files by clients |
| `datanode-1`/`datanode-2` | Managing storage attached to the nodes that they run on |

### Configurations

The configurations for the HDFS cluster are defined in `docker/hadoop-server`, consisting of 2 files

`core-site.xml`

| Property | Value | Description |
|-|-|-|
| `fs.defaultFS` | `hdfs://namenode:9000` | The name of the default file system |

`hdfs-site.xml`

| Property | Value | Description |
|-|-|-|
| `dfs.replication` | 2 | Default block replication |
| `dfs.permissions.enabled` | `false` | Turn off permission checking |
| `dfs.client.use.datanode.hostname` | `true` | Clients use datanode hostnames when connecting to datanodes |
| `dfs.namenode.rpc-address` | `namenode:9000` | RPC address that handles all clients requests |
| `dfs.namenode.name.dir` | `file:///hadoop/dfs/name` | Determines where on the local filesystem the DFS name node should store the name table (fsimage) |
| `dfs.namenode.datanode.registration.ip-hostname-check` | `false` | The namenode allows connections from datanodes without requiring their IP addresses to be resolved to hostnames |
| `dfs.datanode.data.dir` | `file:///hadoop/dfs/data` | Determines where on the local filesystem an DFS data node should store its blocks |

## Spark - Distributed Compute Engine

Spark is deployed with 1 master (`spark-master`) and 2 workers (`spark-worker-1` and `spark-worker-2`)
- **Spark master** acquiring resources on the cluster
- **Spark workers** run application code in the cluster

All Spark jobs in this project are deployed in **client mode** (the driver remains on the client machine that submitted the application) with Spark Standalone cluster manager

To enable **Spark to read and write from HDFS**
- Set `HADOOP_CONF_DIR` to a location containing the configuration files (e.g., `/etc/hadoop/conf`)
- Add two Hadoop configuration files (in [hadoop-client/](../../docker/hadoop-client/)) to Spark's classpath
    - `hdfs-site.xml`, which provides default behaviors for the HDFS client
    - `core-site.xml`, which sets the default filesystem name

## Kafka - Distributed Event Streaming Platform

- Kafka broker and controller are deployed in the same container `kafka` (run Kafka in `KRaft` mode)
- Explain the environment variables in `kafka` container

    ```bash
    # BROKER-AND-CONTROLLER LEVEL CONFIGURATIONS

    KAFKA_NODE_ID=0 # node ID associated with the roles in process.roles 
    KAFKA_PROCESS_ROLES=broker,controller # the roles that Kafka process plays
    KAFKA_LISTENERS=INTERNAL://:9094,CONTROLLER://:9093,EXTERNAL://:9092 # a list of listeners
    KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:9094,EXTERNAL://localhost:9092 # addresses that the Kafka brokers will advertise to clients
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT # map each security protocol for each listener name
    KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS=kafka:9093 # endpoints for bootstrapping the cluster metadata
    KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER # listeners used by the controller
    KAFKA_CONTROLLER_QUORUM_VOTERS=0@kafka:9093 # map of id/endpoint for the set of voters
    KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL # Name of listener used for communication between brokers
    KAFKA_LOG_DIRS=/var/lib/kafka/data # directory the log data is stored

    # TOPIC-AND-PARTITION LEVEL CONFIGURATIONS
    KAFKA_AUTO_CREATE_TOPICS_ENABLE=true # enable auto creation of topic on server
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 # replication factor for the offsets topic
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACOTR=1 # replication factor for the transaction topic
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 # minimum no. replicas that must acknowledge a write to transaction topic in order to be considered successful
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 # amount of time the group coordinator will wait for more consumers to join a new group before performing the first rebalance
    ```

- After `kafka` is healthy, `kafdrop` and `producer` (Python app) containers are up and running
    - `kafdrop` (optional) is a web UI for viewing Kafka topics and browsing consumer groups. The tool displays information such as brokers, topics, partitions, consumers, and lets you view messages
    - `producer` works as a Kafka producer that read transaction data and stream it into Kafka `transaction` topic. Details about Kafka producer in `kafka_producers/transaction_producer.py`

## Pinot - Real-time OLAP serving layer

### Containers

| Container | Role |
|-|-|
| `pinot-zookeeper` | Provides fault-tolerant, persistent storage of metadata, including table configurations, schemas, segment metadata, and cluster state that controller uses |
| `pinot-controller` | Schedules and reschedules resources in a Pinot cluster when metadata changes or a node fails |
| `pinot-server` | Provide the primary storage for segments and perform the computation required to execute queries |
| `pinot-broker` | Take query requests from client processes, scatter them to applicable servers, gather the results, and return results to the client |

### Configurations

A few notes regarding infrastructure configuration

- Administrative tasks (e.g., cluster configuration) and batch ingestion jobs are handled by the controller. Therefore, the configurations in `pinot_conf/` only need to be passed into the `pinot-controller` container. In `pinot_conf/`

    - `offline_ingestion_job.yaml`: The ingestion job spec is used while generating, running, and pushing segments from the input files
    - `transactions_scored_offline_table_config.json`: The offline table spec

        - `segmentsConfig.timeColumnName` is required for `ingestionConfig.batchIngestionConfig.segmentIngestionType="APPEND"`
        - `tableIndexConfig.invertedIndexColumns` for columns commonly used in predicates such as `IN`, `BETWEEN`
        - `tableIndexConfig.rangeIndexColumns` for metrics columns that have a very large number of unique values and commonly used in range predicates such as `>`, `<`, `>=`, `<=`, or `BETWEEN`

    - `transactions_scored_realtime_table_config.json`: The realtime table spec

         
    - `transactions_scored-schema.json`: The table schema spec. Columns in a Pinot table can be categorized into three categories

        - **Dimension**: these columns are typically used in slice and dice operations for answering business queries
        - **Metric**: these columns represent the quantitative data of the table. Such columns are used for aggregation
        - **DateTime**: this column represents time columns in the data

## HDFS + HMS + Trino - DWH serving layer for the granular ad-hoc SQL

This combination brings a DWH serving layer for ad-hoc SQL queries against Parquet files in `/curated` and `/analytics` (stored in HDFS). It allows end users to answer business questions. Detailed sample questions are listed at `notebooks/analysis.ipynb`

### Containers

> **Note**: The HDFS cluster has already been explained in the "HDFS" section, so it will not be repeated here (only HMS and Trino mentioned)

| Container | Role |
|-|-|
| `metastore-db` | HMS requires a RDBMS to persist the Hive object definitions such as databases, tables, and functions. In this project setup, Postgres is used with a database named `metastore`. |
| `hive-metastore-init` | one-shot, idempotent - runs schematool only if the schema doesn't already exist. Mirrors the airflow-init / superset-init pattern. |
| `hive-metastore` | long-running Thrift server on `:9083` (Spark or Trino ping it). |
|||
|||
|||

 


## Airflow - Orchestration Platform

### Containers

| Container | Role |
|-|-|
| `postgres` |  Stores the state of tasks, Dags and variables |
| `airflow-init` | Run once —  db migrate + create-admin, then exits |
| `airflow-apiserver` | Serves the REST API and presents a user interface to inspect, trigger and debug the behaviour of Dags and tasks. Web UI: http://localhost:8081 (`airflow`/`airflow`) |
| `airflow-scheduler` | Handles both triggering scheduled workflows, and submitting Tasks to the executor to run. With `LocalExecutor`, executor runs within the scheduler process |
| `airflow-dag-processor` | Parses Dag files from a Dag bundle and serializes them into the metadata database |

### Configurations

Some essential configurations include environment variables and bind mounts

| Env var | Value | What is means? |
|-|-|-|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | The executor class that Airflow use. `LocalExecutor` means tasks are run locally within the scheduler process |
| `AIRFLOW__CORE__AUTH_MANAGER` | `airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager` | The auth manager class that Airflow use. `SimpleAuthManager` manages each user through their username and role (e.g., `bob` whose role is `admin`) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:airflow@postgres/airflow` | The SQLAlchemy connection string to the metadata database. In this project, Postgres was selected |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `'http://airflow-apiserver:8080/execution/`'| The url of the execution api server (`http://localhost:8080` is default) |

| Host path | Container path | Purpose |
|-|-|-|
| `./airflow/*` | `/opt/airflow/*` | Data shared between Airflow containers and the host, including: `dags/` (DAG files), `logs/` (logs info), `config/` (configuration options), and `plugin/` (external plugins) |
| `./spark_jobs` | `/opt/jobs` | Spark jobs that Airflow orchestrates |

> **IMPORTANT**: `user: "50000:0"` runs all Airflow processes as UID 50000 (the default Airflow image user) with GID 0, which avoids permission errors when writing to the bind-mounted `airflow/logs/` directory on the host. The host UID isn't used because the bind-mount permission model with GID 0 + the official entrypoint covers it

sudo chmod -R 777 airflow/