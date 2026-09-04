# HDFS

## Topology

| **Service** | **Hostname** | **Host -> container ports** | **What other services talk to it** |
|-|-|-|-|
| `namenode` | `namenode` | `9870->9870`, `9000->9000` | DataNodes, Spark master/workers, Flink job/task managers, Airflow tasks |
| `datanode-1` | `datanode-1` | (no host ports) | NameNode (heartbeats), clients via NN-redirected reads/writes |
| `datanode-2` | `datanode-2` | (no host ports) | NameNode (heartbeats), clients via NN-redirected reads/writes |

NameNode `:9870` is the web UI, `:9000` is the RPC port that `fs.defaultFS=hdfs://namenode:9000` clients dial (in [core-site.xml](../../docker/hadoop-server/core-site.xml)). DataNodes are registered with the NameNode by hostname, with `dfs.namenode.datanode.registration.ip-hostname-check=false` set in [hdfs-site.xml](../../docker/hadoop-server/hdfs-site.xml) so the container's bridge-network IPs don't trip up registration.

## Configuration

## Volumes

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