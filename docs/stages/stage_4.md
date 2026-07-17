# Stage 4: Pipeline Orchestration

## Tasks

- Create Airflow DAGs to schedule and monitor your batch pipeline
- Implement at least one data quality gate between pipeline stages
- Add retry logic, SLAs, and failure alerting
- Ensure your pipeline is idempotent (safe to re-run without creating duplicates)
- Create a monitoring or health-check DAG for your streaming components

## Implementation

Two Airflow DAGs are defined in `airflow/dags`, one for the batch pipeline and one for the streaming pipeline
- **Daily Batch DAG**

    - Runs at 2AM every day
    - Task dependencies
        
        - **Spark curate jobs**: move landing to curate
        - **Spark enrich job**: enrich transactions (Kafka transactions must be pre-built)
        - **Spark feature job**: add customer features
        - **Spark score job**: score transactions offline with rule-based fraud detection
        - **Data quality gate**: check if the predicted fraud rate is within the acceptable range; if it's okay, the downstream tasks will be run, otherwise they will be skipped
        - **Spark export Pinot job**: export scored transactions to shared files, So, Pinot will be able to ingest data into its server
        - **Pinot batch ingestion job**: ingest batch data into Pinot tables and segments
        - **Spark register HMS job**:

- **Streaming Monitor DAG**

