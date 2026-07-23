from datetime import datetime, timedelta

import requests
from airflow.sdk import dag, task
# from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator

from finpulse_lib import run_in, spark_submit


KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"

CURATE_JOBS = {
    "curate_customers": "/opt/spark/work-dir/jobs/curate/curate_customers.py",
    "curate_merchants": "/opt/spark/work-dir/jobs/curate/curate_merchants.py",
    "curate_devices": "/opt/spark/work-dir/jobs/curate/curate_devices.py",
    "curate_fraud_reports": "/opt/spark/work-dir/jobs/curate/curate_fraud_reports.py"
}
FRAUD_RATE_MIN = 0.005
FRAUD_RATE_MAX = 0.05


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}
@dag(
    dag_id="daily_batch",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    default_args=default_args,
    catchup=False,
    tags=["finpulse", "batch"],
)
def daily_batch():

    @task
    def curate_dimensions(job_path: str):
        run_in("spark-master", spark_submit(job_path))

    @task
    def build_enriched_fact():
        run_in("spark-master", spark_submit(
            "/opt/spark/work-dir/jobs/enrich/build_enriched_fact.py",
            packages=KAFKA_PACKAGE)
        )

    @task
    def build_customer_features():
        run_in("spark-master", spark_submit(
            "/opt/spark/work-dir/jobs/features/build_customer_features.py"))

    @task
    def run_offline_scoring():
        run_in("spark-master", spark_submit(
            "/opt/spark/work-dir/jobs/score/score_offline.py"))

    @task
    def export_pinot_offline():
        run_in("spark-master", spark_submit(
            "/opt/spark/work-dir/jobs/score/export_pinot_offline.py"))

    @task
    def ingest_pinot_offline():
        run_in("pinot-controller", [
            "./bin/pinot-admin.sh", "LaunchDataIngestionJob",
            "-jobSpecFile", "/opt/pinot-offline/offline_ingestion_job.yaml",
        ])

    @task.short_circuit
    def quality_gate() -> bool:
        """Sanity-check the predicted-fraud rate from the Pinot serving table

        Returns False (short-circuit) if the rate is outside the expected band,
        which skips the downstream publish tasks
        """
        sql = ("""
            SELECT
                CAST(SUM(
                    CASE 
                        WHEN predicted_fraud THEN 1
                        ELSE 0
                    END) AS DOUBLE) / count(*)
            FROM transactions_scored"""
        )
        r = requests.post("http://pinot-broker:8099/query/sql",
                        json={"sql": sql}, timeout=30)
        r.raise_for_status()
        rate = r.json()["resultTable"]["rows"][0][0]
        print(f"predicted_fraud rate = {rate:.4f} (band {FRAUD_RATE_MIN}-{FRAUD_RATE_MAX})")
        ok = FRAUD_RATE_MIN <= rate <= FRAUD_RATE_MAX
        if not ok:
            print("OUT OF BAND - short-circuiting publish")
        return ok

    


    # Curate dimensions (dynamic task mapping)
    curates = [
        curate_dimensions.override(task_id=name)(path)
        for name, path in CURATE_JOBS.items()
    ]

    enrich = build_enriched_fact()
    features = build_customer_features()
    scoring = run_offline_scoring()
    export = export_pinot_offline()
    ingest = ingest_pinot_offline()
    gate = quality_gate()

    (
        curates >> enrich >> features >> scoring
        >> export >> ingest
        >> gate
    )


daily_batch()