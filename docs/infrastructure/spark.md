# Spark

**Batch compute only**. Spark's job in this stack is to nightly-batch consume the Kafka `transactions` topic, join it with the four HDFS dimension tables, and write enriched facts, customer features, and offline-scored output back to HDFS under `/analytics/`

## Topology

| **Service** | **Hostname** | **Host -> container ports** | **What other services talk to it** |
|-|-|-|-|
| `spark-master` | spark-master | `8080->8080`, `7077->7077` | Workers (RPC `:7077`), `spark-submit` clients |
| `spark-worker-1` | spark-worker-1 | (no host ports) | Master only |
| `spark-worker-2` | spark-worker-2 | (no host ports) | Master only |

Each worker is configured with 2 cores, 2 GB. Master UI is at http://localhost:8080. Submit jobs to `spark://spark-master:7077` (in-network) - there is no external Spark RPC endpoint, so all `spark-submit` invocations run inside the master container via `docker compose exec`.

## Configuration

Spark is launched without Bitnami's init script - the compose file gives the explicit `org.apache.spark.deploy.master.Master` / `...worker.Worker` Java mainclass. Everything else is environment-driven or bind-mounted:

| **Mechanism** | **What it sets** |
|-|-|
| `command:` in compose | master/worker mainclass + flags (--host, --port, --cores, --memory) |
| `HADOOP_CONF_DIR=/opt/hadoop-conf` | Tells Spark where the HDFS client config lives, so `hdfs://namenode:9000` resolves |
| `SPARK_NO_DAEMONIZE=true` | Run Spark in foreground, so docker can manage the process lifecycle |
| Bind mount `./docker/spark:/opt/spark/conf` | In `/opt/spark/conf`, there are two files: `spark-defaults.conf` (`spark.sql.catalogImplementation=hive` + warehouse dir, so `saveAsTable` registers in HMS) and `log4j2.properties` (for logging) |
| Bind mount `./spark_jobs:/opt/spark/work-dir/jobs` | Job source code is editable on the host; submit by container path |
| Bind mount `./docker/hadoop-client:/opt/hadoop-conf` | Minimal client-side `core-site.xml` + `hdfs-site.xml` |
| Bind mount `./docker/hadoop-client/hive-site.xml` inside `/opt/hadoop-conf` | Tells Spark where the Hive Metastore Thrift endpoint lives (`thrift://hive-metastore:9083`) |

Spark 4.0 uses log4j2, not log4j1 - the file extension matters. The bundled image looks for `log4j2.properties` in `/opt/spark/conf/`; mounting at any other path silently does nothing.

## Hive Metastore integration

Spark sees the Hive Metastore via:
1. `hive-site.xml` in `docker/hadoop-client/` - `hive.metastore.uris=thrift://hive-metastore:9083`. Spark picks this up automatically because `HADOOP_CONF_DIR=/opt/hadoop-conf` already points at that directory.
2. `spark-defaults.conf` - turns on `spark.sql.catalogImplementation=hive` and pins `spark.sql.warehouse.dir=hdfs://namenode:9000/warehouse` (must match `metastore.warehouse.dir` in HMS or `saveAsTable` writes Parquet to one place and HMS records another).

After, `df.write.saveAsTable("default.foo")` writes Parquet to `hdfs://namenode:9000/warehouse/foo/` AND registers `default.foo` in HMS. Trino sees the table immediately via the `hive` catalog

**Path-based writes still bypass HMS**. `df.write.parquet("hdfs://...")` keeps working untouched and stays invisible to Trino. To make a path-based table queryable from Trino, register it manually with `CREATE EXTERNAL TABLE ... LOCATION 'hdfs://...'`.

## Volumes

Spark has no named volumes. State is intentionally ephemeral - job source comes in via the bind mount and outputs land in HDFS, so nothing on the master/worker filesystems needs to persist across restarts.

## Healthcheck

None - Spark master/workers don't ship with a health endpoint. Instead of, using the smoke check (`make smoke-spark`) as the integration test: it submits `/opt/jobs/smoke/smoke_spark.py`, which reads from HDFS and asserts the expected word counts.

## Why this shape?

- **Kafka connector is not pre-baked**. The `org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0` package is not in the `apache/spark:4.0.0` image. Pass it via `--packages` on every Kafka-source `spark-submit`. First run downloads the JAR into the worker's Ivy cache; subsequent submits are warm.
- **`user: root`**. Workers need to write Spark event logs and shuffle files under bind-mounted directories owned by the host user. Running as root sidesteps UID alignment headaches.
- **`HADOOP_CONF_DIR`**: Without it, Spark falls back to `file:///` for `fs.defaultFS` and silently writes to the container local filesystem instead of HDFS. The bind mount `./docker/hadoop-client:/opt/hadoop-conf` plus the env var is what makes `df.write.parquet("/analytics/...")` actually land in HDFS.

### Caveats

- **Driver memory defaults are tight**. Each worker advertises 2 GB total. If a job reports `OutOfMemoryError` or `killed by signal 9`, lower `--executor-memory` or shrink the input rather than bumping the worker beyond 2 GB on a 16 GB Docker allocation (Pinot + Superset already eat ~3 GB; HDFS + Kafka + Airflow another ~4 GB).

## Alternatives

| **System** | 	**What is it** | **Pick instead when...** |
|-|-|-|
| **Hadoop MapReduce** | Spark's predecessor | Disk-based intermediates make MR 10–100× slower than Spark on the same hardware; the API is verbose. Therefore, it is almost never used in modern systems |
| *a 16 GB Docker allocation (Pinot + Superset already eat ~3 GB; HDFS + Kafka + Airflow another ~4 GB*Presto** / **Trino** | Distributed SQL engine | Interactive ad-hoc SQL over heterogeneous sources (S3, Hive, Kafka, MySQL). Not for general ETL - Spark's DataFrame API is more flexible |

## Anatomy of a spark-submit invocation

The `spark-submit` command is included four pieces, each tied to a specific bit of the stack:

| **Piece** | **What it does** | **Why it looks the way it does** |
|-|-|-|
| `docker compose exec spark-master` | Step into the running `spark-master` container and run a command | Spark binaries aren't on your laptop, and the master RPC port is in-network only - so the submit has to originate from inside the docker network |
| `/opt/spark/bin/spark-submit` |  the Spark CLI that launches a Spark driver JVM and sends jobs to a cluster | `apache/spark:4.0.0` installs Spark at `/opt/spark/` (`ls /opt/spark/bin/` to confirm) |
| `--master spark://spark-master:7077` | Tells the driver which cluster manager to join | `spark://` = Spark's standalone manager. The hostname `spark-master` resolves only on the `finpulse-network` docker network (Compose registers each service name as DNS). 7077 is the master's RPC port; the web UI on 8080 is a different port for a different protocol. |
| `/opt/spark/work-dir/jobs/<subdir>/<your_job>.py` | The script the driver loads | Path inside the container. The bind mount `./spark_jobs:/opt/jobs` mounts the whole `jobs/` tree, so each step's sub-folder (`smoke/`, `curate/`, ...) is visible at `/opt/jobs/<subdir>/`. The mount is live - editing on the host updates the container view immediately, no rebuild needed. |

### What happens after you press enter

1. The Docker daemon execs `spark-submit` inside `spark-master`.
2. `spark-submit` boots a Spark driver JVM in the same container (this is client deploy mode, the default).
3. The driver opens a TCP connection to `spark-master:7077` - the master process is in the same container (started by the `command:` block in compose), but the driver still talks to it over the cluster RPC like any other clients would.
4. Master assigns executors on `spark-worker-1` and `spark-worker-2`.
5. Driver reads `/opt/jobs/<subdir>/<your_job>.py`, runs it, ships pickled task closures to the executors. PySpark workers run Python over py4j.
6. Output streams back to your terminal; the exec session ends when the script returns.

### Other --master values you'll see in the wild

- `local[*]` - no cluster; driver + executors in one JVM. Useful for unit tests.
- yarn - submit to a YARN cluster. Expects YARN config in `HADOOP_CONF_DIR`.
- `k8s://https://kube-api:6443` - submit to Kubernetes. Spark spawns one pod per executor.
- `mesos://...` - Mesos cluster. Mostly historical.

I use `spark://` because the compose stack runs Spark's own standalone cluster manager (no YARN, no k8s).

### Why workers also bind-mount `./spark_jobs:/opt/spark/work-dir/jobs`

In **client** deploy mode, the driver ships pickled task closures, so workers don't strictly need the script file. The mount is on workers anyway because
- (a) it's harmless symmetry across the same image
- (b) some jobs read sibling files relatively (a SQL template next to the Python)
- (c) switching to cluster deploy mode (driver runs on a worker) would otherwise silently break

## Common commands

Submit a vanilla batch job (HDFS-only):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/work-dir/jobs/<subdir>/<your_job>.py
```

Submit a Kafka-source batch job (note `--packages`):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    /opt/spark/work-dir/jobs/<subdir>/<your_job>.py
```

Open a PySpark REPL on the master (handy for poking at HDFS):

```bash
docker compose exec spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077
```

Master UI: http://localhost:8080. Workers list themselves there; each running app has its own driver UI linked from the master page.

```bash    
make smoke-spark   # spark-submit a job that reads HDFS
```