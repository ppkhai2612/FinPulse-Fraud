# Step 1 - Land the dimension datasets into HDFS

This step gets the 4 dimension `.gz` files from [data/](../../data/) into `/landing/<dataset>/` on HDFS. The `/landing` zone is the immutable record of what the upstream provider gave us — no transforms, no schema changes, no flattening. Audit, reruns all start by reading from `/landing`.

## What this step delivers

| **Source file** | **Lands at** | **Replication** |
|-|-|-|
| `data/customer-profiles.json.gz` | `/landing/customer-profiles/` | 3 (regulatoty) |
| `data/device-fingerprints.csv.gz` | `/landing/device-fingerprints/` | 2 (default) |
| `data/fraud-reports.json.gz` | `/landing/device-fingerprints/` | 3 (audit) |
| `data/merchant-directory.csv.gz` | `/landing/merchant-directory/` | 2 (default) |

`fraud-reports` and `customer-profiles` get replication 3 because they're audit / regulatory grade; the other two stay at the cluster default of 2 (we run 2 DataNodes, so 2 is also the practical maximum for those).

## What this step does NOT do

- **`transactions.csv.gz` is not landed**. transactions are a Kafka-only stream — both Spark batch (Step 4) and Flink stream (Step 7) consume from Kafka topic `transactions`. There must be no `/landing/transactions/` and no `/curated/transactions/`.
- **No Parquet conversion**.  Schema changes / partitioning / column derivation are Step 2 (Curate).
- **No replication on the empty parent dir**. `setrep` is applied per-dataset directory, not on `/landing` itself — `/landing` is just a namespace, no blocks of its own.

## Concepts you'll meet here

- **HDFS zone pattern** — `/landing` is the immutable raw zone, `/curated` is the cleaned/parqueted zone (Step 2), `/analytics` is for derived outputs (Step 4 onwards). Audits + reruns require the original bytes to still exist; the zone separation is what makes that work.
- **Per-file replication factor**. The cluster default is 2 (set in [hdfs-site.xml](../../docker/hadoop-server/hdfs-site.xml)). `hdfs dfs -setrep N <path>` raises or lowers replication on a single file or directory; the change is queued through the replication priority queue and picked up asynchronously by the NameNode.
- **The write pipeline**. When you `put` a file, the client asks the NameNode for replica locations, then streams 64 KB packets through a chain of DataNodes. Knowing this is what's happening makes "why did one DN's disk fill faster?" concrete later.
- **Idempotency**. A Step 1 loader script must be re-runnable: if `/landing/foo/foo.gz` already exists, skip the `put` rather than duplicating or failing. Audit zones are append-only by intent — re-running today's load shouldn't double the bytes.

## Pre-flight

Confirm HDFS is up and `/landing` is empty:

```bash
# HDFS + Spark + Kafka
make up-core

# All three should be Up. namenode should be (healthy).
docker compose ps namenode datanode-1 datanode-2

# no /landing
docker compose exec namenode hdfs dfs -ls /
```

You should see all three containers `Up` with `namenode` reporting `(healthy)`, and list `/` don't show `/landing` dir

```bash
NAME         IMAGE                 COMMAND                  SERVICE      CREATED              STATUS                        PORTS
datanode-1   apache/hadoop:3.5.0   "/usr/local/bin/dumb…"   datanode-1   About a minute ago   Up 59 seconds                 
datanode-2   apache/hadoop:3.5.0   "/usr/local/bin/dumb…"   datanode-2   About a minute ago   Up 59 seconds                 
namenode     apache/hadoop:3.5.0   "/usr/local/bin/dumb…"   namenode     About a minute ago   Up About a minute (healthy)   0.0.0.0:9000->9000/tcp, [::]:9000->9000/tcp, 0.0.0.0:9870->9870/tcp, [::]:9870->9870/tcp
```

## 1a — Manual round-trip on one file

Before writing any script, you firstly apply the commands to a sample dataset to understand what each command does. Here use `merchant-directory.csv.gz` as a sample dataset.

```bash
# 1. Create the per-dataset directory in HDFS.
docker compose exec namenode hdfs dfs -mkdir -p /landing/merchant-directory

# 2. Copy the local file into the namenode container's /tmp.
docker compose cp data/merchant-directory.csv.gz namenode:/tmp/

# 3. Stream it from the container's /tmp into HDFS.
docker compose exec namenode hdfs dfs -put \
    /tmp/merchant-directory.csv.gz \
    /landing/merchant-directory/

# 4. Verify it landed at replication 2.
docker compose exec namenode hdfs dfs -ls -h /landing/merchant-directory

# 5. Tidy up the staging copy inside the container.
docker compose exec namenode rm /tmp/merchant-directory.csv.gz
```

If 1a worked, you've now got:

```bash
Found 1 items
-rw-r--r--   2 root supergroup    140.0 K 2026-09-11 08:25 /landing/merchant-directory/merchant-directory.csv.gz
```

## 1b — Idempotent loader script

Script [land_data.py](../../scripts/land_data.py) runs on the host (not inside a container). It runs the above commands in order + `hdfs dfs -test -e` for the idempotency check.

**Why a host-side driver instead of a script-inside-the-namenode?** The `namenode` container has no view of `data/`. We could bind-mount it, but that's a `docker-compose` change for one job. The wrapper-from-host approach keeps the compose file untouched and makes the staging step visible.

Now run it:

```bash
python scripts/land_data.py
```

Each command is printed before it runs, with a short line (line starts with `->`) explaining what it does. On a re-run, every dataset hits the [SKIP] branch (the file already exists in HDFS) and only the `setrep` calls fire

What a fresh run looks like for one dataset:

```bash
Landing 4 dimension datasets to HDFS...
Source: /home/khai2612/FinPulse-Fraud/data

=== customer-profiles.json.gz -> /landing/customer-profiles (rep=3) ===
$ docker compose exec -T namenode hdfs dfs -test -e /landing/customer-profiles/customer-profiles.json.gz
  -> Check whether /landing/customer-profiles/customer-profiles.json.gz already exists in HDFS (if yes, return 0)
$ docker compose exec -T namenode hdfs dfs -mkdir -p /landing/customer-profiles
  -> Create /landing/customer-profiles (and any missing parent dirs) in HDFS
$ docker compose cp /home/khai2612/FinPulse-Fraud/data/customer-profiles.json.gz namenode:/tmp/customer-profiles.json.gz
  -> Copy customer-profiles.json.gz from host into the namenode container's /tmp
$ docker compose exec -T namenode hdfs dfs -put /tmp/customer-profiles.json.gz /landing/customer-profiles/
  -> Copy /tmp/customer-profiles.json.gz into /landing/customer-profiles/ (in HDFS)
$ docker compose exec -T namenode rm /tmp/customer-profiles.json.gz
  -> Remove the temporary copy
$ docker compose exec -T namenode hdfs dfs -setrep 3 /landing/customer-profiles
  -> Set the replication factor of /landing/customer-profiles to 3 (audit/regulatory = 3, default = 2)
Replication 3 set: /landing/customer-profiles/customer-profiles.json.gz
```

## Verification

Run all five checks. The first three are the rubric; the last two are sanity.

```bash
# 1. Tree of /landing — expect 4 dirs, each with one .gz.
docker compose exec namenode hdfs dfs -ls -R /landing

# 2. Replication on the regulatory datasets — expect 3.
#    Note: quote the wildcards so the shell doesn't try to glob them
#    locally (zsh errors with `no matches found` otherwise). HDFS
#    expands them server-side.
docker compose exec namenode hdfs dfs -stat "%r %n" \
    '/landing/fraud-reports/*' '/landing/customer-profiles/*'

# 3. Replication on the non-regulatory datasets — expect 2.
docker compose exec namenode hdfs dfs -stat "%r %n" \
    '/landing/merchant-directory/*' '/landing/device-fingerprints/*'

# 4. Confirm transactions are NOT in HDFS — expect empty output.
docker compose exec namenode hdfs dfs -ls /landing 2>&1 | grep -i transactions

# 5. Sizes match the local files (bytes-in == bytes-out).
docker compose exec namenode hdfs dfs -du -h /landing 
ls -lh data/*.gz
```