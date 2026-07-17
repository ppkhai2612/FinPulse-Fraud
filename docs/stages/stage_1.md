# Stage 1 - Data Lake Foundation

## Tasks

- Design and create the HDFS directory structure (landing / curated / analytics zones)
- Load the provided raw datasets into the landing zone
- Choose appropriate file formats for each data source and justify your choices (CSV, JSON, Avro, Parquet — consider schema evolution, compression, query patterns)
- Set replication factors based on data criticality and volume
- Document your data lake architecture with a diagram

## Decisions

- In `landing/` zone, dimension files will be stored in `.gz` format because
    
    - `.gz` is more efficient for storage (compared to `.csv` or `.json`)
    - Spark can read directly from `gz` files

- In `curate/` zone, dimension files are serialized into `.parquet` files because

    - Spark offers excellent support when working with Parquet filesWhen reading, it automatically detect partitioned table or merges schema as the schema evolution occurs. When writing, users can specify the desired number of partitions
    - The `curate/` zone acts as an intermediary. It can be used to allow joins or aggregates in the downstream, so storing it as Parquet files is very convenient for Spark to read and write to

- In `analytics/` zone, the files used primarily for analysis are divided into several different directories

    - `transactions_enriched/`: the transactions have been joined with their dimensions (customer, merchant, fraud_report, device)
    - `customer_features/`: add customer features to customer profiles from `curated/customer-profiles/`
    - `scored/`: scored transactions offline with rule-based fraud detection

- Since my HDFS cluster has a maximum of 2 DataNodes, the actual useful number of replications is 2 (although I could set it higher)

## Implementation

### Landing Script

- When run the Python script `scripts/land_data.py`, the five commands are executed sequentially (e.g., for `merchant-directory.csv.gz`)

    ```bash
    # Create the per-dataset directory in HDFS
    docker compose exec -T namenode hdfs dfs -mkdir -p /landing/merchant-directory

    # Copy the local file into the namenode container's /tmp
    # Required for the put command in the next command
    docker compose cp data/merchant-directory.csv.gz namenode:/tmp/

    # Stream tmp/ dir into merchant-directory/ dir
    docker compose exec -T namenode hdfs dfs -put \
        /tmp/merchant-directory.csv.gz \
        /landing/merchant-directory/

    # Tidy up the tmp copy
    docker compose exec -T namenode rm /tmp/merchant-directory.csv.gz

    # Set the replication factor of the files in merchant-directory/
    docker compose exec -T namenode hdfs dfs -setrep 2 /landing/merchant-directory/ 
    ```

- Verification

    - List of stats of the subdirectories/files in `landing/` directory: `docker compose exec namenode hdfs dfs -ls -R /landing` 

        ```bash
        drwxr-xr-x   - root supergroup          0 2026-07-16 16:57 /landing/customer-profiles
        -rw-r--r--   2 root supergroup    2816936 2026-07-16 16:57 /landing/customer-profiles/customer-profiles.json.gz
        drwxr-xr-x   - root supergroup          0 2026-07-16 16:57 /landing/device-fingerprints
        -rw-r--r--   2 root supergroup    7252050 2026-07-16 16:57 /landing/device-fingerprints/device-fingerprints.csv.gz
        drwxr-xr-x   - root supergroup          0 2026-07-16 16:57 /landing/fraud-reports
        -rw-r--r--   2 root supergroup     283783 2026-07-16 16:57 /landing/fraud-reports/fraud-reports.json.gz
        drwxr-xr-x   - root supergroup          0 2026-07-16 16:57 /landing/merchant-directory
        -rw-r--r--   2 root supergroup     143366 2026-07-16 16:57 /landing/merchant-directory/merchant-directory.csv.gz
        ```