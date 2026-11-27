import hashlib
import hmac
import json
import os
import threading
from pathlib import Path
from uuid import uuid4

import psycopg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from sales_transform import create_spark_session, run_benchmark, run_spark


DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data")).resolve()
OUTPUT_DIR = Path(os.getenv("SPARK_OUTPUT_DIR", "/app/parquet/spark")).resolve()
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
JOB_LOCK = threading.Lock()
app = FastAPI(title="DataFlow360 PySpark runner")


class SparkRunRequest(BaseModel):
    source: str = "csv"
    airflow_run_id: str | None = None


def _validate_request(x_pipeline_token):
    expected = os.getenv("PIPELINE_INTERNAL_TOKEN", "")
    if not expected or not x_pipeline_token or not hmac.compare_digest(
        expected, x_pipeline_token
    ):
        raise HTTPException(status_code=401, detail="Invalid internal pipeline token")


def _input_file(source):
    if source != "csv":
        raise HTTPException(
            status_code=400,
            detail="PySpark currently supports the generated CSV adapter only",
        )
    path = (DATA_DIR / "sales_50000.csv").resolve()
    if not path.is_relative_to(DATA_DIR) or not path.is_file():
        raise HTTPException(status_code=404, detail="Generated CSV dataset not found")
    return path


def _run_directory(run_id):
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:20]
    return OUTPUT_DIR / "runs" / digest


def _persist_execution(metric, run_id, state="succeeded"):
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO processing_executions(benchmark_id,engine,dataset_name,input_rows,"
                "accepted_rows,rejected_rows,duration_seconds,throughput_rows_per_second,"
                "output_bytes,state,resource_metrics) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb) "
                "RETURNING execution_id",
                (
                    run_id,
                    metric["engine"],
                    "data/sales_50000.csv",
                    metric["input_rows"],
                    metric["accepted_rows"],
                    metric["rejected_rows"],
                    metric["duration_seconds"],
                    metric["throughput_rows_per_second"],
                    metric["output_bytes"],
                    state,
                    json.dumps(
                        {
                            "revenue": metric["revenue"],
                            "output_paths": {
                                "silver": metric["silver_path"],
                                "rejected": metric["rejected_path"],
                                "gold": metric["gold_path"],
                            },
                        }
                    ),
                ),
            )
            return cursor.fetchone()[0]


@app.get("/health")
def health():
    return {"status": "ok", "engine": "pyspark", "master": SPARK_MASTER_URL}


@app.post("/run")
def run(request: SparkRunRequest, x_pipeline_token: str | None = Header(default=None)):
    _validate_request(x_pipeline_token)
    if not JOB_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A Spark job is already running")
    run_id = request.airflow_run_id or str(uuid4())
    spark = None
    try:
        input_path = _input_file(request.source)
        spark = create_spark_session(SPARK_MASTER_URL)
        metric = run_spark(spark, input_path, _run_directory(run_id), run_id)
        metric["execution_id"] = _persist_execution(metric, run_id)
        return metric
    except HTTPException:
        raise
    except (OSError, ValueError, psycopg.Error) as exc:
        raise HTTPException(status_code=500, detail=f"Spark processing failed: {exc}") from exc
    finally:
        if spark is not None:
            spark.stop()
        JOB_LOCK.release()


@app.post("/benchmark")
def benchmark(request: SparkRunRequest, x_pipeline_token: str | None = Header(default=None)):
    _validate_request(x_pipeline_token)
    if not JOB_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A Spark job is already running")
    run_id = request.airflow_run_id or str(uuid4())
    spark = None
    try:
        input_path = _input_file(request.source)
        spark = create_spark_session(SPARK_MASTER_URL)
        result = run_benchmark(spark, input_path, OUTPUT_DIR / "benchmarks", run_id)
        for metric in (result["pandas"], result["pyspark"]):
            metric["execution_id"] = _persist_execution(metric, run_id)
        return result
    except HTTPException:
        raise
    except (OSError, ValueError, psycopg.Error) as exc:
        raise HTTPException(status_code=500, detail=f"Benchmark failed: {exc}") from exc
    finally:
        if spark is not None:
            spark.stop()
        JOB_LOCK.release()
