# Step 2 - Curate the dimension datasets

This step takes the four `.gz` files that Step 1 landed in `/landing/` on HDFS, parses them with Spark, and writes the result as Parquet to `/curated/<dataset>/` — partitioned where it makes sense, untouched otherwise.

## What this step delivers

Every output is Snappy-compressed Parquet under `/curated/<dataset>/`. The table below summarises which input format each row starts from and what's partition-worthy:

| **Source (from Step 1)** | **Ouput (always Parquet)** | **Partitioned by** | **Input quirk** |
|-|-|-|-|
| `/landing/customer-profiles/customer-profiles.json.gz` | `/curated/customer-profiles/` | (none) | gzip JSON-array -> Parquet; needs `multiLine=true`; `typical_categories` stays as `array<string>` |
| `/landing/device-fingerprints/device-fingerprints.csv.gz` | `/curated/device-fingerprints/`	 | `device_type`  | gzip CSV -> Parquet |
| `/landing/fraud-reports/fraud-reports.json.gz` | `/curated/fraud-reports/` | `fraud_type` | gzip JSON-array -> Parquet; needs `multiLine=true` |
| `/landing/merchant-directory/merchant-directory.csv.gz` | `/curated/merchant-directory/` | (none) | gzip CSV -> Parquet |

**Don't expect the Parquet output to be dramatically smaller than the landing `.gz`**. gzip is a strong general-purpose compressor; on narrow tables Snappy-Parquet often comes in about the same size or slightly larger, and only wins on wide tables with highly repetitive columns. **File size isn't why we're moving to Parquet here** — the wins are downstream. Steps 4, 8, and 9 are all easier because the data is Parquet, regardless of whether it shrunk.

## What this step does NOT do

- **No catalog registration**. No `saveAsTable`, no HMS, no Presto visibility. That's Step 9 — keeping it separate is deliberate so the catalog-vs-storage-vs-engine split lands clean later.
- **No joins, no feature engineering, no derived columns**. Step 4 reads `/curated/` + Kafka `transactions` and produces `/analytics/transactions_enriched/`.
- **No transactions**. `transactions` is a Kafka-only stream — it never lives in `/landing` or `/curated`.
- **No explicit `StructType` schemas**. `inferSchema=true` is fine at this scope. If a column infers to the wrong type (timestamps as strings is the usual one), cast it inline; don't write a schema builder.

## Concepts you'll meet here

- **`spark-submit` from inside the master container**. Spark batch jobs are submitted to the standalone cluster via `docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/jobs/<subdir>/<file>.py`. `jobs/` is bind-mounted at `/opt/jobs` so changes on your host show up immediately. Each step's jobs live in their own sub-folder (`jobs/smoke/`, `jobs/curate/`, ...).
- **HDFS as Spark's source and sink**. Spark talks to HDFS via the Hadoop client config bind-mounted at `/opt/hadoop-conf` in the master and workers. All paths use the in-network scheme `hdfs://namenode:9000/...`.
- **Spark autodetects gzip from the `.gz` extension**. No codec flag needed — `spark.read.csv("....csv.gz")` and `spark.read.json("....json.gz")` both decompress transparently.
- **`multiLine=true` for JSON arrays**. Both `.json.gz` files are JSON arrays (`[ {...}, {...} ]`), not JSON-Lines. By default Spark assumes one record per line — without `multiLine=true` it reads the whole array as a single giant null row.
- **`partitionBy(col)`**. Spark writes one sub-directory per distinct value of `col`, like` /curated/device-fingerprints/device_type=mobile/...`. Predicates on that column become directory pruning at read time. Pick low-cardinality columns — `device_type` (3 values) and `fraud_type` (~5) are good; `txn_id` (~600K distinct) would be a disaster.
- **`mode("overwrite")` and the "rebuilt-from-landing" mental model**. `/landing` is immutable; `/curated` is rebuildable from `/landing`. Overwriting `/curated/<dataset>/` on every run is intentional — the job is idempotent because the inputs are immutable.
- **Why Parquet, not gzip-CSV/JSON**. Not because Parquet is smaller — often it isn't (gzip is genuinely good). The reasons are access-pattern, all paying off in later steps:

    - **Columnar reads**. `select country, risk_score` reads only those two columns off disk. CSV has to scan every byte of every row to find the column boundaries.
    - **Predicate pushdown**. where `risk_score >= 8` is evaluated against per-row-group min/max stats stored in the Parquet footer; non-matching row groups are skipped without ever being decompressed.
    - **Splittable**. Spark hands different row groups in the same file to different executors in parallel. A `.csv.gz` is opaque to the splitter — one task has to decompress it from the top.
    - **Schema preserved**. Types, nullability, and nested structures (the `typical_categories` array) survive a round-trip; no re-running `inferSchema` on every read.
    - **Partition pruning**. Paired with `partitionBy()`, Spark skips whole `<col>=<value>/` sub-directories at plan time before any file is opened.
    - **Step 4** (Spark joins on `/curated/*`), **Step 8** (Pinot offline segments built from Parquet), and **Step 9** (Trino-on-HMS reading `/curated/*` directly) all consume these files, and all of them benefit from the bullets above. That's the payoff.

## Pre-flight

```bash
# 1. /landing has the four datasets from Step 1.
docker compose exec namenode hdfs dfs -ls -R /landing
# Tree of /landing — expect 4 dirs, each with one .gz.

# 2. Spark cluster is up.
docker compose ps spark-master spark-worker-1 spark-worker-2
# all three Up;

# 3. Smoke job still works (sanity that HDFS <-> Spark is wired).
make smoke-spark
# expect a green "OK: HDFS + Spark integration works" line
#
# Use the wrapper, NOT a bare `spark-submit /opt/jobs/smoke/smoke_spark.py`.
# smoke_spark.py expects an input file at hdfs:///smoke/words.txt;
# `make smoke-spark` (via scripts/smoke.sh) seeds that file, runs the
# job, then cleans it up. The bare submit fails with PATH_NOT_FOUND
# unless a previous run is mid-flight.
```

## Warmup — Read one file by hand in a PySpark shell

Before writing any job file, open an interactive PySpark shell inside the `spark-master` container and round-trip one file. For example, picking the `merchant-directory.csv.gz`

```bash
docker compose exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077
```

Inside the shell:

```bash
LANDING = "hdfs://namenode:9000/landing/merchant-directory/merchant-directory.csv.gz"
CURATED = "hdfs://namenode:9000/curated/merchant-directory/"

# 1. Read the gz CSV straight from HDFS — gzip codec auto-detected.
df = (spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(LANDING))

# 2. What did Spark think of it?
df.printSchema()
df.show(5, truncate=False)
print("rows:", df.count())     # expect ~10000

# 3. Write Parquet (no partitioning yet — small dim).
df.write.mode("overwrite").parquet(CURATED)

# 4. Read it back and confirm same row count + schema.
df2 = spark.read.parquet(CURATED)
print("rows back:", df2.count())
df2.printSchema()
```

What the warmup looks like end-to-end inside the pyspark shell — read the gz CSV, inspect the inferred schema, preview a few rows, count, write Parquet, then re-read and confirm rows back.

Then in another terminal:

```bash
docker compose exec namenode hdfs dfs -ls -h /curated/merchant-directory
docker compose exec namenode hdfs dfs -du -h /landing/merchant-directory /curated/merchant-directory
```

You should see one `*.snappy.parquet` file (plus a `_SUCCESS` marker) in `/curated/merchant-directory/`. The `du` line will be roughly the same size as the landing gz, maybe slightly larger — that's expected. What we care about is that the file exists, parses back, and round-trips the row count.

```bash
140.0 K  280.0 K  /landing/merchant-directory/merchant-directory.csv.gz
0        0        /curated/merchant-directory/_SUCCESS
201.2 K  402.4 K  /curated/merchant-directory/part-00000-16ede8df-76c8-4308-b11f-e22f71ecb80f-c000.snappy.parquet
```

## 2a — First curate job: `curate_merchants.py` (no partitioning)

Job script: [curate_merchants.py](../../jobs/curate/curate_merchants.py). Mirror the [smoke_spark.py](../../jobs/smoke/smoke_spark.py) shape: module docstring -> `main()` -> `if __name__ == "__main__"`. The body is the same three calls you ran in the warmup (read -> write -> stop), wrapped so `spark-submit` can drive it.

Submit:

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/jobs/curate/curate_merchants.py
```

No `--packages` needed — CSV / Parquet / HDFS codecs all ship in the base Spark image. A successful run printing `Read ... rows from ...` + the schema + `wrote Parquet to ...`

Verify:

```bash
docker compose exec namenode hdfs dfs -ls -R /curated/merchant-directory
docker compose exec namenode hdfs dfs -du -h \
    /landing/merchant-directory /curated/merchant-directory
```

Showing the `_SUCCESS` marker and one `part-...snappy.parquet` file:

```bash
-rw-r--r--   2 root supergroup          0 2026-09-16 02:22 /curated/merchant-directory/_SUCCESS
-rw-r--r--   2 root supergroup     206042 2026-09-16 02:22 /curated/merchant-directory/part-00000-94807eb5-1ddb-4948-aaee-2c6030b41002-c000.snappy.parquet
```

Commit as `step 2a: curate merchant-directory to Parquet`.

## 2b — Add partitioning: `curate_devices.py`

Job script: [curate_devices.py](../../jobs/curate/curate_devices.py). Same shape as 2a, with two changes:

```bash
# File paths
LANDING = "hdfs://namenode:9000/landing/device-fingerprints/device-fingerprints.csv.gz"
CURATED = "hdfs://namenode:9000/curated/device-fingerprints/"

# partitionBy
(df.write
    .mode("overwrite")
    .partitionBy("device_type") 
    .parquet(CURATED))
```

Why partition `device_type` and not `txn_id`? Cardinality. `device_type` has 3 distinct values -> 3 sub-directories. `txn_id` has ~600K -> 600K sub-directories, each with one tiny file -> the small-files problem.

Submit the same way. Verify the directory layout reflects partitioning:

```bash
docker compose exec namenode hdfs dfs -ls /curated/device-fingerprints
docker compose exec namenode hdfs dfs -ls /curated/device-fingerprints/device_type=mobile
# expect: one or more part-*.snappy.parquet files
```

```bash
-rw-r--r--   2 root supergroup          0 2026-09-16 02:38 /curated/device-fingerprints/_SUCCESS
drwxr-xr-x   - root supergroup          0 2026-09-16 02:38 /curated/device-fingerprints/device_type=desktop
drwxr-xr-x   - root supergroup          0 2026-09-16 02:38 /curated/device-fingerprints/device_type=mobile
drwxr-xr-x   - root supergroup          0 2026-09-16 02:38 /curated/device-fingerprints/device_type=tablet
```

Commit as `step 2b: curate device-fingerprints partitioned by device_type`.

## 2c — JSON `multiline`: `curate_customers.py`

Job script: [curate_customers.py](../../jobs/curate/curate_customers.py). The two changes here are the reader (`.json` instead of `.csv`) and adding `multiLine=true`. That's because `customer-profiles.json.gz` is a JSON array, and Spark's default JSON reader expects one record per line (JSONL).

```bash
LANDING = "hdfs://namenode:9000/landing/customer-profiles/customer-profiles.json.gz"
CURATED = "hdfs://namenode:9000/curated/customer-profiles/"

df = (spark.read
    .option("multiLine", "true")
    .json(LANDING))
```

Note:` typical_categories` stays as `array<string>`. The rule is: in `/curated`, schema is the source's schema with the cleanups Spark needs to read it correctly.

Submit + verify the same way. Commit as `step 2c: curate customer-profiles (multiLine JSON)`.

## 2d — Combine both: `curate_fraud_reports.py`

Job script: [curate_devices.py](../../jobs/curate/curate_devices.py). Apply JSON `multiline` and `partitioning` together. 

```bash
LANDING = "hdfs://namenode:9000/landing/fraud-reports/fraud-reports.json.gz"
CURATED = "hdfs://namenode:9000/curated/fraud-reports/"

df = (spark.read
      .option("multiLine", "true")
      .json(LANDING))

(df.write
    .mode("overwrite")
    .partitionBy("fraud_type")
    .parquet(CURATED))
```

Verify the directory layout shows one sub-dir per `fraud_type` (roughly 4-5 distinct values from the generator):

```bash
docker compose exec namenode hdfs dfs -ls /curated/fraud-reports
```

```bash
-rw-r--r--   2 root supergroup          0 2026-09-16 03:20 /curated/fraud-reports/_SUCCESS
drwxr-xr-x   - root supergroup          0 2026-09-16 03:20 /curated/fraud-reports/fraud_type=account_takeover
drwxr-xr-x   - root supergroup          0 2026-09-16 03:20 /curated/fraud-reports/fraud_type=card_not_present
drwxr-xr-x   - root supergroup          0 2026-09-16 03:20 /curated/fraud-reports/fraud_type=counterfeit
drwxr-xr-x   - root supergroup          0 2026-09-16 03:20 /curated/fraud-reports/fraud_type=identity_theft
```

Commit as `step 2d: curate fraud-reports (multiLine JSON, partitioned by fraud_type)`.

## Verification

After all four jobs have run at least once:

```bash
# 1. Tree of /curated/ — expect 4 dirs
docker compose exec namenode hdfs dfs -ls /curated

# 2. Sizes — sanity check only. Parquet won't be dramatically smaller
#    than gzip; sometimes it's slightly larger.
for ds in merchant-directory device-fingerprints customer-profiles fraud-reports; do
  echo "=== $ds ==="
  docker compose exec namenode hdfs dfs -du -h /landing/$ds  /curated/$ds
done

# 3. Spot-check that each Parquet still parses with the right row count.
# expect: ~10000 / ~600000 / 100000 / 15000
docker compose exec -T spark-master /opt/spark/bin/pyspark --master spark://spark-master:7077 <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

for ds in ["merchant-directory", "device-fingerprints", "customer-profiles", "fraud-reports"]:
    n = spark.read.parquet(f"hdfs://namenode:9000/curated/{ds}/").count()
    print(f"{ds}: {n}")
PY

# 4. Confirm partitioned datasets have sub-dirs.
docker compose exec namenode hdfs dfs -ls /curated/device-fingerprints | grep device_type=
docker compose exec namenode hdfs dfs -ls /curated/fraud-reports       | grep fraud_type=
```