# HDFS

## Topology

| **Service** | **Hostname** | **Host -> container ports** | **What other services talk to it** |
|-|-|-|-|
| `namenode` | `namenode` | `9870->9870`, `9000->9000` | DataNodes, Spark master/workers, Flink job/task managers, Airflow tasks |
| `datanode-1` | `datanode-1` | (no host ports) | NameNode (heartbeats), clients via NN-redirected reads/writes |
| `datanode-2` | `datanode-2` | (no host ports) | NameNode (heartbeats), clients via NN-redirected reads/writes |

NameNode `:9870` is the web UI, `:9000` is the RPC port that `fs.defaultFS=hdfs://namenode:9000` clients dial (in [core-site.xml](../../docker/hadoop-server/core-site.xml)). DataNodes are registered with the NameNode by hostname, with `dfs.namenode.datanode.registration.ip-hostname-check=false` set in [hdfs-site.xml](../../docker/hadoop-server/hdfs-site.xml) so the container's bridge-network IPs don't trip up registration.

## Configuration

Hadoop config is bind-mounted XML:

| **File** | **Mounted into** | **Purpose** |
|-|-|-|
| [`hadoop-server/core-site.xml`](../../docker/hadoop-server/core-site.xml) | NN + DN-1 + DN-2 at `/opt/hadoop/etc/hadoop/` | `fs.defaultFS`, `hadoop.tmp.dir` |
| [`hadoop-server/hdfs-site.xml`](../../docker/hadoop-server/hdfs-site.xml) | NN + DN-1 + DN-2 at `/opt/hadoop/etc/hadoop/` | replication, name/data dirs, NN address, permission off |
| [`hadoop-client/core-site.xml`](../../docker/hadoop-client/core-site.xml) | Spark master + workers + Flink jm/tm at `/opt/hadoop-conf/` | minimal client side: `fs.defaultFS=hdfs://namenode:9000` |
| [`hadoop-client/hdfs-site.xml`](../../docker/hadoop-client/hdfs-site.xml) | Spark master + workers + Flink jm/tm at `/opt/hadoop-conf/` | client-side block-access defaults |

When you change cluster-wide HDFS settings, edit the server files and restart the affected NN/DN containers. The client-side files are intentionally minimal - they exist only so Spark and Flink know where the NameNode is via `HADOOP_CONF_DIR=/opt/hadoop-conf`

`dfs.replication` is set to 2 (we run 2 DataNodes); the audit-grade dim datasets (`fraud-reports`, `customer-profiles`) to be written with replication 3 at write time

## Volumes

| **Volume** | **Mount path** | **What's persisted** |
|-|-|-|
| `namenode-data` | NN: `/hadoop/dfs/name` | NameNode metadata (inode, fsimage, edit log) |
| `datanode1-data` | DN-1: `/hadoop/dfs/data` | DataNode block storage |
| `datanode2-data` | DN-2: `/hadoop/dfs/data` | DataNode block storage |

The NameNode bash entrypoint formats `/hadoop/dfs/name` only when it doesn't already contain a `current/` subdirectory, so a normal restart preserves the cluster ID and existing blocks. After `make nuke`, the next bring-up formats fresh with cluster ID `finpulse`

## Healthcheck

NameNode only:

```yml
healthcheck:
  test: ["CMD-SHELL", "curl -f http://localhost:9870/ > /dev/null"]
  interval: 10s
  timeout: 5s
  retries: 10
```

**DataNodes have no healthcheck** - they're declared:

```yml
depends_on:
  namenode:
    condition: service_healthy
```

So they only start once the NN web UI is responsive, and their own liveness is observable via the NN's "Datanodes" tab

## Alternatives

| **System** | **What is it** | **Pick instead when...** |
|-|-|-|
| **Amazon S3**	| Cloud object store | You're in AWS, want zero ops, can tolerate network latency. The default for greenfield data lakes. |
| **MinIO** | Open-source S3-compatible | You want S3 semantics on-prem or for local dev. Drop-in for any tool that speaks S3. Often the right modern pick over HDFS. |
| **Google Cloud Storage** / **Azure Blob Storage** | Cloud object stores | You're in GCP / Azure. Same shape as S3. |

**In the modern systems, HDFS is rarely chosen. S3-compatible object storage has won**. I use HDFS here because the class rubric mandates the Hadoop ecosystem; in production, swap to S3/MinIO and most of this stack (Spark, Flink, Pinot offline segments, and PrestoDB via the Hive connector's `s3a://` reader) keeps working with a one-line config change.