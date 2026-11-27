from datetime import datetime, timedelta
import json
import os
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_pipeline(**context):
    conf = context["dag_run"].conf or {}
    source = conf.get("source", "csv")
    incremental = str(conf.get("incremental", True)).lower()
    token = os.environ["DATAFLOW_PIPELINE_TOKEN"]
    request = Request(
        f"http://backend:8000/pipeline/execute?source={source}&incremental={incremental}",
        headers={"X-Pipeline-Token": token},
        method="POST",
    )
    with urlopen(request, timeout=900) as response:
        if response.status != 200:
            raise RuntimeError(f"DataFlow360 pipeline failed: HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


with DAG(
    dag_id="dataflow360_daily_pipeline",
    description="Run the DataFlow360 incremental retail pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=15),
    tags=["dataflow360", "retail", "incremental"],
) as dag:
    PythonOperator(
        task_id="run_incremental_pipeline",
        python_callable=run_pipeline,
    )
