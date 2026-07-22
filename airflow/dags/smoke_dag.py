from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def _say_hello():
    return "smoke test ok"


with DAG(
    dag_id="smoke_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke"]
) as dag:
    PythonOperator(task_id="hello_python", python_callable=_say_hello) >> BashOperator(
        task_id="hello_bash", bash_command="echo smoke test ok"
    )