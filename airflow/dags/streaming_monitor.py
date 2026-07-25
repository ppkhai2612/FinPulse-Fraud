"""streaming_monitor - liveness + health checks for the Spark Structured Streaming layer.

Runs every 15 minutes. Three checks:
  1. The Spark Structured Streaming application is RUNNING.
  2. Its checkpoint directory is fresh (micro-batches are still committing).
  3. The transactions-scored topic is non-empty (the scorer is producing).

Any failed check raises, so the task goes red and Airflow surfaces the
alert. Spark is reached over the standalone master's JSON endpoint; HDFS
checkpoint metadata and Kafka offsets are read via CLI execs.
"""

import time
from datetime import datetime
from pathlib import PurePosixPath

import requests

from airflow.sdk import dag, task
from finpulse_lib import run_in

SPARK_MASTER_JSON = "http://spark-master:8080/json"
APP_NAME = "finpulse-stream-score"
CHECKPOINT_OFFSETS_PATH = "/checkpoints/finpulse-stream-score/offsets"
MAX_CHECKPOINT_AGE_S = 300


@dag(
    dag_id="streaming_monitor",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finpulse", "streaming"],
)
def streaming_monitor():
    @task
    def check_spark_running() -> str:
        response = requests.get(SPARK_MASTER_JSON, timeout=30)
        response.raise_for_status()
        apps = response.json().get("activeapps", [])

        running_apps = [
            app["id"]
            for app in apps
            if app.get("name") == APP_NAME and app.get("state") == "RUNNING"
        ]
        print(f"Running Spark apps matching {APP_NAME!r}: {running_apps}")

        if not running_apps:
            states = [
                {"id": app.get("id"), "name": app.get("name"), "state": app.get("state")}
                for app in apps
            ]
            raise RuntimeError(
                f"No RUNNING Spark application named {APP_NAME!r} (active apps: {states})"
            )

        return running_apps[0]

    @task
    def check_checkpoint_age(app_id: str) -> None:
        out = run_in("namenode", ["hdfs", "dfs", "-ls", CHECKPOINT_OFFSETS_PATH])
        offset_files = []
        for line in out.splitlines():
            parts = line.split()
            if len(parts) < 8: # skip first line in result
                continue
            path = parts[-1] # path to offset files
            name = PurePosixPath(path).name # name of files (e.g., 0, 1, 2,...)
            if name.isdigit():
                offset_files.append((int(name), path))

        if not offset_files:
            print("no committed Spark offset files yet (stream may have just started)")
            return

        latest_batch_id, latest_path = max(offset_files) # take latest micro-batch
        raw_mtime = run_in("namenode", ["hdfs", "dfs", "-stat", "%Y", latest_path]).strip()
        mtime = int(raw_mtime)
        mtime_s = mtime / 1000.0 if mtime > 10_000_000_000 else float(mtime)
        age_s = time.time() - mtime_s # diff between the current time and the time the latest micro-batch was created (in seconds)

        print(
            f"Spark app {app_id} latest checkpoint batch={latest_batch_id} "
            f"age={age_s:.0f}s (max {MAX_CHECKPOINT_AGE_S}s)"
        )

        if age_s > MAX_CHECKPOINT_AGE_S:
            raise RuntimeError(
                f"Checkpoint stale: latest Spark micro-batch is "
                f"{age_s:.0f}s old > {MAX_CHECKPOINT_AGE_S}s"
            )

    @task
    def check_scored_topic() -> None:
        out = run_in("kafka", [
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "kafka:9094",
            "--topic", "transactions-scored",
        ])
        total = sum(int(line.rsplit(":", 1)[1]) for line in out.splitlines() if ":" in line)
        print(f"transactions-scored total offset: {total}")
        if total <= 0:
            raise RuntimeError("transactions-scored is empty - scorer not producing")

    spark_app_id = check_spark_running()
    check_checkpoint_age(spark_app_id)
    check_scored_topic()


streaming_monitor()