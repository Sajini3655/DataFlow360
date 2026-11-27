from datetime import datetime, timedelta
import json
import os
from urllib.parse import quote
from urllib.request import Request, urlopen

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator


def run_pipeline(**context):
    conf = context["dag_run"].conf or {}
    source = conf.get("source", "csv")
    incremental = str(conf.get("incremental", True)).lower()
    engine = conf.get("engine", "pandas")
    airflow_run_id = context["dag_run"].run_id
    encoded_run_id = quote(airflow_run_id, safe="")
    token = os.environ["DATAFLOW_PIPELINE_TOKEN"]
    if engine in {"pyspark", "benchmark"}:
        runner_url = os.environ["SPARK_RUNNER_URL"].rstrip("/")
        runner_path = "/benchmark" if engine == "benchmark" else "/run"
        spark_request = Request(
            f"{runner_url}{runner_path}",
            data=json.dumps(
                {"source": source, "airflow_run_id": airflow_run_id}
            ).encode("utf-8"),
            headers={
                "X-Pipeline-Token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(spark_request, timeout=1800) as response:
            spark_result = json.loads(response.read().decode("utf-8"))
        if engine == "benchmark":
            return spark_result
    request = Request(
        f"http://backend:8000/pipeline/execute?source={source}&incremental={incremental}"
        f"&airflow_run_id={encoded_run_id}&engine={engine}",
        headers={"X-Pipeline-Token": token},
        method="POST",
    )
    with urlopen(request, timeout=900) as response:
        if response.status != 200:
            raise RuntimeError(f"DataFlow360 pipeline failed: HTTP {response.status}")
        return json.loads(response.read().decode("utf-8"))


def run_dbt_build(**context):
    if os.getenv("DBT_ENABLED", "false").lower() != "true":
        raise AirflowSkipException("dbt profile is disabled")
    token = os.environ["DATAFLOW_PIPELINE_TOKEN"]
    runner_url = os.environ["DBT_RUNNER_URL"].rstrip("/")
    request = Request(
        f"{runner_url}/run",
        data=b"{}",
        headers={
            "X-Pipeline-Token": token,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urlopen(request, timeout=1800) as response:
        result = json.loads(response.read().decode("utf-8"))
    if result.get("state") != "success":
        raise RuntimeError("dbt runner returned a non-success state")
    return result


with DAG(
    dag_id="dataflow360_daily_pipeline",
    description="Run the DataFlow360 incremental retail pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=90),
    tags=["dataflow360", "retail", "incremental"],
) as dag:
    PythonOperator(
        task_id="run_incremental_pipeline",
        python_callable=run_pipeline,
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=45),
    )

    dbt_build = PythonOperator(
        task_id="dbt_build",
        python_callable=run_dbt_build,
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=31),
    )

    dag.get_task("run_incremental_pipeline") >> dbt_build
