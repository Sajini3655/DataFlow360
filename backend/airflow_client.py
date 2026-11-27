import base64
import json
import os
from urllib.parse import quote
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class AirflowError(RuntimeError):
    pass


def _request(path, method="GET", body=None, timeout=15):
    base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080").rstrip("/")
    username = os.environ["AIRFLOW_USERNAME"]
    password = os.environ["AIRFLOW_PASSWORD"]
    token = base64.b64encode(f"{username}:{password}".encode()).decode("ascii")
    data = json.dumps(body).encode() if body is not None else None
    request = Request(
        f"{base_url}/api/v1/{path.lstrip('/')}",
        data=data,
        headers={
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise AirflowError(f"Airflow API request failed: {exc}") from exc


def trigger_dag(source, incremental, engine="pandas"):
    return _request(
        "dags/dataflow360_daily_pipeline/dagRuns",
        method="POST",
        body={"conf": {"source": source, "incremental": incremental, "engine": engine}},
    )


def get_dag_run(dag_run_id):
    dag = "dataflow360_daily_pipeline"
    encoded_run_id = quote(dag_run_id, safe="")
    run = _request(f"dags/{dag}/dagRuns/{encoded_run_id}")
    tasks = _request(f"dags/{dag}/dagRuns/{encoded_run_id}/taskInstances")
    dag_info = _request(f"dags/{dag}")
    task_instances = []
    for task in tasks.get("task_instances", []):
        task_instances.append(
            {
                "task_id": task.get("task_id"),
                "state": task.get("state"),
                "start_date": task.get("start_date"),
                "end_date": task.get("end_date"),
                "duration": task.get("duration"),
                "try_number": task.get("try_number"),
                "max_tries": task.get("max_tries"),
                "log_url": task.get("log_url"),
            }
        )
    return {
        "dag_id": dag,
        "dag_run_id": dag_run_id,
        "state": run.get("state"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "duration": run.get("duration"),
        "scheduled_run": run.get("run_type") == "scheduled",
        "schedule": dag_info.get("timetable_description")
        or dag_info.get("schedule_interval"),
        "tasks": task_instances,
    }


def retry_failed_task(dag_run_id, task_id):
    if not task_id.strip():
        raise ValueError("Task ID cannot be empty")
    dag = "dataflow360_daily_pipeline"
    return _request(
        f"dags/{dag}/clearTaskInstances",
        method="POST",
        body={
            "dry_run": False,
            "task_ids": [task_id],
            "dag_run_id": dag_run_id,
            "only_failed": True,
        },
    )
