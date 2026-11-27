import os
import json
import hashlib
import csv
import io
import importlib.util
import time
import tempfile
import shutil
import logging
from pathlib import Path
from datetime import date
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

import psycopg
import pyarrow.parquet as parquet_reader
from psycopg.rows import dict_row
from fastapi import Body, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from pipeline import transform_records
from adapters import csv_adapter, json_adapter, rest_adapter
from airflow_client import AirflowError, get_dag_run, retry_failed_task, trigger_dag
from data_quality import (
    detect_schema_drift,
    evaluate_rules,
    persist_lineage,
    persist_quality_results,
    validate_rule_config,
)
from medallion import parquet_layer_status, write_parquet_files
from warehouse_loader import load_silver_rows
from cloud_storage import (
    CloudStorageConfigurationError,
    CloudStorageError,
    CloudStorageConfig,
    create_storage_adapter,
)

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data" / "sales_50000.csv"
LOGGER = logging.getLogger(__name__)


class QualityRuleUpdate(BaseModel):
    enabled: bool | None = None
    threshold_percent: float | None = Field(default=None, ge=0, le=100)
    config: dict[str, Any] | None = None


app = FastAPI(title="DataFlow 360")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")
        if origin.strip()
    ],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def spark_runner_status():
    runner_url = os.getenv("SPARK_RUNNER_URL")
    if not runner_url:
        return False, "SPARK_RUNNER_URL is not configured"
    try:
        with urlopen(f"{runner_url.rstrip('/')}/health", timeout=2) as response:
            return (
                response.status == 200,
                None if response.status == 200 else f"health endpoint returned HTTP {response.status}",
            )
    except (URLError, TimeoutError) as exc:
        return False, str(exc)


def record_stage(cursor, pipeline_run_id, stage_name, rows_read=0, rows_written=0, state="succeeded", error=None):
    cursor.execute(
        "INSERT INTO pipeline_stage_runs(pipeline_run_id,stage_name,state,finished_at,"
        "rows_read,rows_written,error_message) "
        "VALUES (%s,%s,%s,now(),%s,%s,%s) "
        "ON CONFLICT(pipeline_run_id,stage_name,attempt) DO UPDATE SET "
        "state=EXCLUDED.state,finished_at=now(),rows_read=EXCLUDED.rows_read,"
        "rows_written=EXCLUDED.rows_written,error_message=EXCLUDED.error_message",
        (pipeline_run_id, stage_name, state, rows_read, rows_written, error),
    )


def ensure_schema():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute((ROOT / "warehouse" / "init.sql").read_text(encoding="utf-8"))
            migrations = ROOT / "warehouse" / "migrations"
            for migration in sorted(migrations.glob("*.sql")):
                cursor.execute(migration.read_text(encoding="utf-8"))


ensure_schema()


@app.get("/health")
def health():
    return {"status": "ok"}


def _record_cloud_event(
    operation: str,
    storage_mode: str,
    state: str,
    *,
    layer: str | None = None,
    object_name: str | None = None,
    bytes_transferred: int = 0,
    message: str | None = None,
    details: dict[str, Any] | None = None,
):
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO cloud_operation_events "
                "(operation,storage_mode,state,layer,object_name,bytes_transferred,"
                "message,details) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                (
                    operation,
                    storage_mode,
                    state,
                    layer,
                    object_name,
                    bytes_transferred,
                    message,
                    json.dumps(details or {}),
                ),
            )


def _azure_sdk_available() -> bool:
    try:
        return all(
            importlib.util.find_spec(module) is not None
            for module in (
                "azure.identity",
                "azure.storage.filedatalake",
                "azure.core.pipeline.policies",
            )
        )
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


@app.get("/cloud/status")
def cloud_status():
    config_error = None
    try:
        config = CloudStorageConfig.from_env()
    except CloudStorageConfigurationError as exc:
        config = None
        config_error = str(exc)
    mode = config.mode if config else os.getenv("CLOUD_STORAGE_MODE", "local").strip().lower()
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT ON (operation) event_id,operation,storage_mode,state,"
                "layer,object_name,bytes_transferred,message,started_at,finished_at "
                "FROM cloud_operation_events ORDER BY operation,started_at DESC,event_id DESC"
            )
            latest_events = cursor.fetchall()
    latest = {item["operation"]: item for item in latest_events}
    account_host = None
    if config and config.account_url:
        account_host = config.account_url.split("://", 1)[-1].split("/", 1)[0]
    source_dir = Path(os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")))
    source_datasets = parquet_layer_status(source_dir) if source_dir.is_dir() else []
    connection_event = latest.get("connection_test")
    sync_event = latest.get("dataset_sync")
    if connection_event and connection_event["storage_mode"] != mode:
        connection_event = None
    if sync_event and sync_event["storage_mode"] != mode:
        sync_event = None
    storage_configured = config_error is None and (
        mode == "local" or bool(config and config.account_url)
    )
    fabric_workspace = os.getenv("FABRIC_WORKSPACE_NAME", "").strip() or None
    fabric_lakehouse = os.getenv("FABRIC_LAKEHOUSE_NAME", "").strip() or None
    fabric_configured = bool(fabric_workspace and fabric_lakehouse)
    return {
        "execution_modes": [
            {"mode": "local", "available": True},
            {
                "mode": "azure",
                "available": bool(config and config.mode == "azure" and _azure_sdk_available()),
            },
        ],
        "storage": {
            "mode": mode,
            "configured": storage_configured,
            "account_host": account_host,
            "authentication": config.auth_mode if config else "entra",
            "sdk_available": _azure_sdk_available() if mode == "azure" else None,
            "containers": config.containers if config else {},
            "configuration_error": config_error,
            "connection": (
                {
                    "state": "not_tested",
                    "message": "No storage connection test has been recorded",
                }
                if connection_event is None
                else {
                    "state": connection_event["state"],
                    "message": connection_event["message"],
                    "tested_at": connection_event["finished_at"],
                }
            ),
            "last_sync": sync_event,
            "local_datasets": source_datasets,
        },
        "fabric": {
            "configured": fabric_configured,
            "state": "configured_unverified" if fabric_configured else "not_configured",
            "workspace": fabric_workspace,
            "lakehouse": fabric_lakehouse,
            "execution": "not_run",
            "message": (
                "Workspace settings are present, but Fabric execution is not connected or verified"
                if fabric_configured
                else "Set FABRIC_WORKSPACE_NAME and FABRIC_LAKEHOUSE_NAME after creating a Fabric workspace and Lakehouse"
            ),
        },
    }


@app.get("/cloud/history")
def cloud_history(limit: int = Query(default=50, ge=1, le=100)):
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT event_id,operation,storage_mode,state,layer,object_name,"
                "bytes_transferred,message,details,started_at,finished_at "
                "FROM cloud_operation_events ORDER BY started_at DESC,event_id DESC LIMIT %s",
                (limit,),
            )
            return {"events": cursor.fetchall()}


@app.post("/cloud/storage/test")
def cloud_storage_test():
    mode = os.getenv("CLOUD_STORAGE_MODE", "local").strip().lower()
    try:
        adapter = create_storage_adapter()
        result = adapter.test_connection()
    except CloudStorageConfigurationError as exc:
        _record_cloud_event(
            "connection_test", mode, "failed", message=str(exc), details={"error_type": type(exc).__name__}
        )
        return {"success": False, "mode": mode, "message": str(exc)}
    except CloudStorageError as exc:
        _record_cloud_event(
            "connection_test", mode, "failed", message=str(exc), details={"retryable": exc.retryable}
        )
        return {"success": False, "mode": mode, "message": str(exc)}
    _record_cloud_event(
        "connection_test",
        adapter.mode,
        "succeeded",
        message=result["message"],
        details={"containers": result["containers"]},
    )
    return result


@app.post("/cloud/sync")
def cloud_sync():
    mode = os.getenv("CLOUD_STORAGE_MODE", "local").strip().lower()
    try:
        adapter = create_storage_adapter()
        config = CloudStorageConfig.from_env()
        source_root = Path(os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")))
        results = []
        for layer in ("bronze", "silver", "gold"):
            source = source_root / f"{layer}_sales.parquet"
            if not source.is_file():
                raise CloudStorageError(f"Local {layer} Parquet snapshot is unavailable")
            result = adapter.upload_file(source, layer, source.name)
            _record_cloud_event(
                "dataset_sync_file",
                adapter.mode,
                result["status"],
                layer=layer,
                object_name=result["name"],
                bytes_transferred=result["bytes"] if result["status"] == "uploaded" else 0,
                message=f"{result['status']} {layer} dataset",
                details={"etag": result.get("etag"), "container": config.containers[layer]},
            )
            results.append(result)
        _record_cloud_event(
            "dataset_sync",
            adapter.mode,
            "succeeded",
            message=f"Synchronized {len(results)} medallion datasets",
            details={
                "files": results,
                "containers": config.containers,
            },
        )
    except CloudStorageConfigurationError as exc:
        _record_cloud_event(
            "dataset_sync",
            mode,
            "failed",
            message=str(exc),
            details={"error_type": type(exc).__name__},
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except CloudStorageError as exc:
        _record_cloud_event(
            "dataset_sync",
            mode,
            "failed",
            message=str(exc),
            details={"retryable": exc.retryable},
        )
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {
        "success": True,
        "mode": adapter.mode,
        "message": (
            "Files synchronized to the configured ADLS Gen2 file systems"
            if adapter.mode == "azure"
            else "Files synchronized to the local storage adapter; no cloud service was contacted"
        ),
        "files": results,
    }


@app.get("/cloud/storage/files/{layer}")
def cloud_storage_files(layer: str):
    try:
        adapter = create_storage_adapter()
        files = adapter.list_files(layer)
    except CloudStorageConfigurationError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except CloudStorageError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"mode": adapter.mode, "layer": layer, "files": files}


@app.get("/cloud/storage/files/{layer}/{object_name:path}")
def cloud_storage_download(layer: str, object_name: str):
    temporary_dir = None
    try:
        adapter = create_storage_adapter()
        temporary_dir = Path(tempfile.mkdtemp(prefix="dataflow360-cloud-download-"))
        target = temporary_dir / Path(object_name).name
        result = adapter.download_file(layer, object_name, target)
    except CloudStorageConfigurationError as exc:
        if temporary_dir:
            shutil.rmtree(temporary_dir, ignore_errors=True)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except CloudStorageError as exc:
        if temporary_dir:
            shutil.rmtree(temporary_dir, ignore_errors=True)
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(
        target,
        filename=Path(result["name"]).name,
        background=BackgroundTask(shutil.rmtree, temporary_dir, ignore_errors=True),
    )


def _spark_output_rows(airflow_run_id):
    digest = hashlib.sha256(airflow_run_id.encode("utf-8")).hexdigest()[:20]
    root = Path(os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")))
    run_dir = root / "spark" / "runs" / digest
    silver_path = run_dir / "silver_sales"
    rejected_path = run_dir / "rejected_sales"
    if not silver_path.is_dir() or not rejected_path.is_dir():
        raise FileNotFoundError(f"PySpark outputs not found for Airflow run {airflow_run_id}")
    accepted = parquet_reader.read_table(silver_path).to_pylist()
    for row in accepted:
        if row.get("order_date") is not None:
            row["order_date"] = str(row["order_date"])
    rejected_rows = parquet_reader.read_table(rejected_path).to_pylist()
    rejected = [
        {
            "record": json.loads(item["record_json"]),
            "reason": item["reason"],
            "source_row_number": item["source_row_number"],
        }
        for item in rejected_rows
    ]
    return accepted, rejected


def execute_pipeline(
    incremental: bool,
    source: str,
    airflow_run_id: str | None = None,
    engine: str = "pandas",
):
    started = time.perf_counter()
    source_name = "data/sales_50000.csv"
    source_path = SOURCE

    if source == "json":
        records = json_adapter(ROOT / "data" / "sales_50000.json")
        source_name = "data/sales_50000.json"
        source_path = ROOT / "data" / "sales_50000.json"
    elif source == "rest":
        records = rest_adapter("http://backend:8000/adapters/rest/sample")
        source_name = "rest://sales"
        source_path = None
    elif source == "postgres":
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT order_id,line_number,customer_id,customer_name,product_id,product,"
                    "category,supplier_id,region,order_date,quantity,unit_price "
                    "FROM silver_sales"
                )
                records = cursor.fetchall()
        source_name = "postgres://silver_sales"
        source_path = None
    else:
        records = csv_adapter(source_path)

    source_bytes = source_path.read_bytes() if source_path else json.dumps(records, default=str).encode("utf-8")
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    if engine == "pyspark":
        if source != "csv" or not airflow_run_id:
            raise ValueError("PySpark execution requires a CSV source and Airflow run ID")
        rows, rejected = _spark_output_rows(airflow_run_id)
    elif engine == "pandas":
        rows, rejected = transform_records(records, require_order_date=True)
    else:
        raise ValueError(f"Unsupported processing engine: {engine}")

    quality_failure_message = None
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_hash FROM ingestion_watermarks WHERE source_name=%s",
                (source_name,),
            )
            prior = cursor.fetchone()
            if incremental and prior and prior["source_hash"] == source_hash:
                cursor.execute(
                    "INSERT INTO pipeline_runs(accepted,rejected,source_name,state,engine,"
                    "airflow_run_id,started_at,finished_at,rows_read) "
                    "VALUES (0,0,%s,'skipped',%s,%s,now(),now(),0) RETURNING id",
                    (source_name, engine, airflow_run_id),
                )
                run_id = cursor.fetchone()["id"]
                return {
                    "accepted": 0,
                    "rejected": 0,
                    "skipped": True,
                    "run_id": run_id,
                    "source_hash": source_hash,
                    "bronze": len(records),
                    "silver": len(rows),
                    "gold": len(rows),
                    "parquet": write_parquet_snapshot(),
                    "quality": None,
                }

            cursor.execute(
                "INSERT INTO pipeline_runs(accepted,rejected,source_name,state,engine,airflow_run_id,"
                "started_at,rows_read) VALUES (%s,%s,%s,'running',%s,%s,now(),%s) RETURNING id",
                (len(rows), len(rejected), source_name, engine, airflow_run_id, len(records)),
            )
            run_id = cursor.fetchone()["id"]
            if engine == "pyspark" and airflow_run_id:
                cursor.execute(
                    "UPDATE processing_executions SET pipeline_run_id=%s "
                    "WHERE benchmark_id=%s AND engine='pyspark'",
                    (run_id, airflow_run_id),
                )
            detect_schema_drift(cursor, source_name, records, run_id)
            record_stage(cursor, run_id, "extract_source", len(records), len(records))

            cursor.executemany(
                "INSERT INTO bronze_sales(source_row_number,source_name,payload) "
                "VALUES (%s,%s,%s::jsonb) "
                "ON CONFLICT(source_name,source_row_number) "
                "DO UPDATE SET payload=EXCLUDED.payload,ingested_at=now()",
                [
                    (row_number, source_name, json.dumps(record, default=str))
                    for row_number, record in enumerate(records, start=1)
                ],
            )
            record_stage(cursor, run_id, "persist_bronze", len(records), len(records))
            record_stage(cursor, run_id, "validate_and_quarantine", len(records), len(rows))
            cursor.executemany(
                "INSERT INTO rejected_sales(pipeline_run_id,source_name,source_row_number,record,reason) "
                "VALUES (%s,%s,%s,%s::jsonb,%s)",
                [
                    (run_id, source_name, item["source_row_number"], json.dumps(item["record"], default=str), item["reason"])
                    for item in rejected
                ],
            )
            cursor.execute("SAVEPOINT warehouse_load")
            load_silver_rows(cursor, rows)
            record_stage(cursor, run_id, "load_warehouse", len(rows), len(rows))
            quality = persist_quality_results(
                cursor, run_id, source_name, records, rows, rejected
            )
            persist_lineage(cursor, run_id, source_name, len(records), len(rows))
            cursor.execute(
                "SELECT rule_id,state,checked_count,failure_count,failure_rate,threshold_percent "
                "FROM data_quality_results WHERE pipeline_run_id=%s",
                (run_id,),
            )
            quality_results = cursor.fetchall()
            failed_rules = [item["rule_id"] for item in quality_results if item["state"] == "failed"]
            if failed_rules:
                cursor.execute(
                    "SELECT source_name,rows_checked,rows_accepted,rows_rejected,acceptance_rate,"
                    "rejection_rate,overall_score FROM quality_run_summaries WHERE pipeline_run_id=%s",
                    (run_id,),
                )
                quality_summary = cursor.fetchone()
                cursor.execute("ROLLBACK TO SAVEPOINT warehouse_load")
                cursor.executemany(
                    "INSERT INTO data_quality_results(pipeline_run_id,source_name,dataset_name,"
                    "data_layer,rule_id,state,checked_count,failure_count,failure_rate,threshold_percent) "
                    "VALUES (%s,%s,'sales','silver',%s,%s,%s,%s,%s,%s)",
                    [
                        (
                            run_id,
                            source_name,
                            item["rule_id"],
                            item["state"],
                            item["checked_count"],
                            item["failure_count"],
                            item["failure_rate"],
                            item["threshold_percent"],
                        )
                        for item in quality_results
                    ],
                )
                cursor.execute(
                    "INSERT INTO quality_run_summaries(pipeline_run_id,source_name,rows_checked,"
                    "rows_accepted,rows_rejected,acceptance_rate,rejection_rate,overall_score) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        run_id,
                        source_name,
                        quality_summary["rows_checked"],
                        quality_summary["rows_accepted"],
                        quality_summary["rows_rejected"],
                        quality_summary["acceptance_rate"],
                        quality_summary["rejection_rate"],
                        quality_summary["overall_score"],
                    ),
                )
                quality_failure_message = (
                    "Quality thresholds failed: " + ", ".join(sorted(failed_rules))
                )
                record_stage(
                    cursor,
                    run_id,
                    "quality_checks",
                    len(records),
                    len(rows),
                    state="failed",
                    error=quality_failure_message,
                )
                cursor.executemany(
                    "INSERT INTO lineage_events(pipeline_run_id,source_name,from_dataset,"
                    "to_dataset,from_layer,to_layer,rows_written,metadata) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    [
                        (
                            run_id,
                            source_name,
                            from_dataset,
                            to_dataset,
                            from_layer,
                            to_layer,
                            row_count,
                            json.dumps({"quality_gate": "failed", "rules": failed_rules}),
                        )
                        for from_dataset, to_dataset, from_layer, to_layer, row_count in (
                            (source_name, "bronze_sales", "source", "bronze", len(records)),
                            ("bronze_sales", "rejected_sales", "bronze", "quarantine", len(rejected)),
                        )
                    ],
                )
                cursor.execute(
                    "UPDATE pipeline_runs SET state='failed',finished_at=now(),error_message=%s "
                    "WHERE id=%s",
                    (quality_failure_message, run_id),
                )
            else:
                record_stage(cursor, run_id, "quality_checks", len(records), len(rows))
                cursor.execute(
                    "INSERT INTO ingestion_watermarks(source_name,source_hash,rows_read,"
                    "rows_accepted,rows_rejected) VALUES (%s,%s,%s,%s,%s) "
                    "ON CONFLICT(source_name) DO UPDATE SET "
                    "source_hash=EXCLUDED.source_hash,last_processed_at=now(),"
                    "rows_read=EXCLUDED.rows_read,rows_accepted=EXCLUDED.rows_accepted,"
                    "rows_rejected=EXCLUDED.rows_rejected",
                    (source_name, source_hash, len(records), len(rows), len(rejected)),
                )

    if quality_failure_message:
        raise ValueError(quality_failure_message)

    try:
        parquet = write_parquet_snapshot()
    except (OSError, ValueError, psycopg.Error) as exc:
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE pipeline_runs SET state='failed',finished_at=now(),"
                    "error_message=%s WHERE id=%s",
                    (f"Parquet publication failed: {exc}", run_id),
                )
                record_stage(
                    cursor,
                    run_id,
                    "publish_parquet",
                    len(records),
                    0,
                    state="failed",
                    error=str(exc),
                )
        raise

    elapsed = time.perf_counter() - started
    output_bytes = sum(Path(item["path"]).stat().st_size for item in parquet.values())
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE pipeline_runs SET state='succeeded',finished_at=now() WHERE id=%s",
                (run_id,),
            )
            record_stage(
                cursor,
                run_id,
                "publish_parquet",
                len(records),
                sum(item["rows"] for item in parquet.values()),
            )
            record_stage(cursor, run_id, "finalize_execution", len(records), len(rows))
            if engine == "pandas":
                cursor.execute(
                    "INSERT INTO processing_executions(pipeline_run_id,engine,dataset_name,input_rows,"
                    "accepted_rows,rejected_rows,duration_seconds,throughput_rows_per_second,"
                    "output_bytes,state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'succeeded')",
                    (
                        run_id,
                        engine,
                        source_name,
                        len(records),
                        len(rows),
                        len(rejected),
                        round(elapsed, 4),
                        round(len(records) / elapsed, 4) if elapsed else 0,
                        output_bytes,
                    ),
                )
    return {
        "accepted": len(rows),
        "rejected": len(rejected),
        "skipped": False,
        "run_id": run_id,
        "source_hash": source_hash,
        "parquet": parquet,
        "bronze": len(records),
        "silver": len(rows),
        "gold": len(rows),
        "quality": quality,
    }


def write_parquet_snapshot():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_row_number,source_name,payload::text AS payload,ingested_at "
                "FROM bronze_sales ORDER BY source_name,source_row_number"
            )
            bronze = cursor.fetchall()
            cursor.execute("SELECT * FROM silver_sales ORDER BY order_id,line_number")
            silver = cursor.fetchall()
            cursor.execute("SELECT * FROM gold_sales_fact ORDER BY order_id,line_number")
            gold = cursor.fetchall()
    return write_parquet_files(
        bronze,
        silver,
        gold,
        os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")),
    )


def queue_pipeline(incremental: bool, source: str, engine: str):
    if source not in {"csv", "json", "rest", "postgres"}:
        raise HTTPException(status_code=400, detail="Unsupported source adapter")
    if not os.getenv("AIRFLOW_USERNAME") or not os.getenv("AIRFLOW_PASSWORD"):
        raise HTTPException(
            status_code=503,
            detail="Airflow integration is not configured; set AIRFLOW_USERNAME and AIRFLOW_PASSWORD",
        )
    if engine not in {"pandas", "pyspark", "benchmark"}:
        raise HTTPException(status_code=400, detail="Unsupported processing engine")
    if engine in {"pyspark", "benchmark"} and source != "csv":
        raise HTTPException(status_code=400, detail="PySpark engines currently support CSV sources only")
    if engine in {"pyspark", "benchmark"}:
        available, error = spark_runner_status()
        if not available:
            raise HTTPException(
                status_code=503,
                detail=f"Spark runner is unavailable: {error}",
            )
    try:
        payload = trigger_dag(source, incremental, engine)
    except AirflowError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    dag_run_id = payload.get("dag_run_id")
    if not dag_run_id:
        raise HTTPException(status_code=502, detail="Airflow did not return a DAG run ID")
    job_id = str(payload.get("run_id") or dag_run_id)
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_jobs(job_id,dag_run_id,source_name,engine) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT(dag_run_id) DO NOTHING",
                (job_id, dag_run_id, source, engine),
            )
    return {
        "job_id": job_id,
        "dag_id": "dataflow360_daily_pipeline",
        "dag_run_id": dag_run_id,
        "state": payload.get("state", "queued"),
        "source": source,
        "engine": engine,
    }


@app.post("/pipeline/run", status_code=status.HTTP_202_ACCEPTED)
def run(incremental: bool = True, source: str = "csv", engine: str = "pandas"):
    return queue_pipeline(incremental, source, engine)


@app.post("/processing/run", status_code=status.HTTP_202_ACCEPTED)
def run_processing(engine: str = "pandas"):
    return queue_pipeline(incremental=True, source="csv", engine=engine)


@app.post("/processing/benchmark", status_code=status.HTTP_202_ACCEPTED)
def run_processing_benchmark():
    return queue_pipeline(incremental=True, source="csv", engine="benchmark")


@app.get("/pipeline/runs/{dag_run_id}")
def pipeline_run_status(dag_run_id: str):
    try:
        result = get_dag_run(dag_run_id)
    except AirflowError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id,state,source_name,engine,accepted,rejected,rows_read,started_at,"
                "finished_at,error_message FROM pipeline_runs WHERE airflow_run_id=%s "
                "ORDER BY id DESC LIMIT 1",
                (dag_run_id,),
            )
            execution = cursor.fetchone()
            if execution:
                cursor.execute(
                    "SELECT stage_name,state,started_at,finished_at,rows_read,rows_written,error_message "
                    "FROM pipeline_stage_runs WHERE pipeline_run_id=%s ORDER BY started_at,stage_name",
                    (execution["id"],),
                )
                execution["stages"] = cursor.fetchall()
                cursor.execute(
                    "SELECT overall_score,acceptance_rate,rejection_rate,rows_checked,rows_accepted,"
                    "rows_rejected FROM quality_run_summaries WHERE pipeline_run_id=%s",
                    (execution["id"],),
                )
                execution["quality"] = cursor.fetchone()
    result["pipeline_execution"] = execution
    return result


@app.post("/pipeline/runs/{dag_run_id}/tasks/{task_id}/retry")
def retry_pipeline_task(dag_run_id: str, task_id: str):
    try:
        return retry_failed_task(dag_run_id, task_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AirflowError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/pipeline/execute")
def execute_from_airflow(
    source: str = "csv",
    incremental: bool = True,
    airflow_run_id: str | None = None,
    engine: str = "pandas",
    x_pipeline_token: str | None = Header(default=None),
):
    expected = os.getenv("PIPELINE_INTERNAL_TOKEN", "")
    if not expected or x_pipeline_token != expected:
        raise HTTPException(status_code=401, detail="Invalid internal pipeline token")
    if source not in {"csv", "json", "rest", "postgres"}:
        raise HTTPException(status_code=400, detail="Unsupported source adapter")
    if engine not in {"pandas", "pyspark"}:
        raise HTTPException(status_code=400, detail="Unsupported processing engine")
    try:
        return execute_pipeline(
            incremental=incremental,
            source=source,
            airflow_run_id=airflow_run_id,
            engine=engine,
        )
    except (OSError, ValueError, psycopg.Error) as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc


@app.get("/adapters")
def adapters():
    return {
        "csv": {"status": "available", "sample": "sales_50000.csv"},
        "json": {"status": "available", "sample": "sales_50000.json"},
        "rest": {"status": "available", "sample": "/adapters/rest/sample"},
        "postgres": {"status": "available", "source": "silver_sales"},
    }


@app.get("/adapters/rest/sample")
def rest_sample():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT order_id,line_number,customer_id,customer_name,product_id,product,"
                "category,supplier_id,region,order_date,quantity,unit_price "
                "FROM silver_sales LIMIT 100"
            )
            return {"records": cursor.fetchall()}


@app.get("/overview")
def overview():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) AS records,coalesce(sum(quantity*unit_price),0) AS revenue,"
                "coalesce(sum(quantity),0) AS units FROM silver_sales"
            )
            metrics = cursor.fetchone()
            cursor.execute(
                "SELECT category,coalesce(sum(revenue),0) AS revenue "
                "FROM silver_sales GROUP BY category ORDER BY revenue DESC"
            )
            categories = cursor.fetchall()
            cursor.execute(
                "SELECT region,coalesce(sum(revenue),0) AS revenue "
                "FROM silver_sales GROUP BY region ORDER BY revenue DESC"
            )
            regions = cursor.fetchall()
            cursor.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 20")
            runs = cursor.fetchall()
            cursor.execute(
                "SELECT count(*) AS rows,count(DISTINCT pipeline_run_id) AS runs "
                "FROM rejected_sales"
            )
            rejected = cursor.fetchone()
            cursor.execute(
                "SELECT (SELECT count(*) FROM bronze_sales) AS bronze,"
                "(SELECT count(*) FROM silver_sales) AS silver,"
                "(SELECT count(*) FROM gold_sales_fact) AS gold,"
                "(SELECT count(*) FROM inventory_fact) AS inventory"
            )
            layers = cursor.fetchone()
            cursor.execute(
                "SELECT source_name,source_hash,last_processed_at,rows_read,"
                "rows_accepted,rows_rejected FROM ingestion_watermarks "
                "ORDER BY last_processed_at DESC"
            )
            watermarks = cursor.fetchall()
    return {
        "metrics": metrics,
        "categories": categories,
        "regions": regions,
        "runs": runs,
        "rejected": rejected,
        "layers": layers,
        "watermarks": watermarks,
    }


@app.get("/quality")
def quality():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT reason,count(*) AS rows FROM rejected_sales "
                "GROUP BY reason ORDER BY rows DESC"
            )
            reasons = cursor.fetchall()
            cursor.execute("SELECT count(*) AS rows FROM bronze_sales")
            bronze = cursor.fetchone()
            cursor.execute("SELECT count(*) AS rows FROM silver_sales")
            silver = cursor.fetchone()
            cursor.execute(
                "SELECT pipeline_run_id,source_name,rows_checked,rows_accepted,rows_rejected,"
                "acceptance_rate,rejection_rate,overall_score,checked_at "
                "FROM quality_run_summaries ORDER BY checked_at DESC LIMIT 100"
            )
            history = cursor.fetchall()
            cursor.execute(
                "SELECT source_name,avg(overall_score) AS average_score,"
                "count(*) AS pipeline_runs FROM quality_run_summaries "
                "GROUP BY source_name ORDER BY source_name"
            )
            sources = cursor.fetchall()
            cursor.execute(
                "SELECT r.rule_id,r.rule_name,r.dimension,r.severity,r.threshold_percent,r.enabled,"
                "coalesce(sum(q.failure_count),0) AS failure_count "
                "FROM data_quality_rules r LEFT JOIN data_quality_results q ON q.rule_id=r.rule_id "
                "GROUP BY r.rule_id ORDER BY r.dimension,r.rule_id"
            )
            rules = cursor.fetchall()
            cursor.execute(
                "SELECT drift_id,pipeline_run_id,source_name,changes,detected_at "
                "FROM schema_drift_events ORDER BY detected_at DESC LIMIT 50"
            )
            schema_drift = cursor.fetchall()
            cursor.execute(
                "SELECT count(*) AS pending FROM rejected_sales WHERE reprocessed_at IS NULL"
            )
            quarantine = cursor.fetchone()
    latest = history[0] if history else None
    return {
        "reasons": reasons,
        "bronze": bronze["rows"],
        "silver": silver["rows"],
        "latest": latest,
        "history": history,
        "sources": sources,
        "rules": rules,
        "schema_drift": schema_drift,
        "quarantine_pending": quarantine["pending"],
    }


@app.get("/quality/rules")
def quality_rules():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT rule_id,rule_name,description,dimension,severity,"
                "threshold_percent,enabled,config,updated_at "
                "FROM data_quality_rules ORDER BY dimension,rule_id"
            )
            return cursor.fetchall()


@app.patch("/quality/rules/{rule_id}")
def update_quality_rule(rule_id: str, update: QualityRuleUpdate):
    changes = update.model_dump(exclude_unset=True)
    if not changes or any(value is None for value in changes.values()):
        raise HTTPException(status_code=400, detail="Provide a non-null rule setting")
    if "config" in changes:
        try:
            validate_rule_config(rule_id, changes["config"])
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE data_quality_rules SET "
                "enabled=COALESCE(%s,enabled),"
                "threshold_percent=COALESCE(%s,threshold_percent),"
                "config=COALESCE(%s::jsonb,config),updated_at=now() "
                "WHERE rule_id=%s RETURNING rule_id,enabled,threshold_percent,updated_at",
                (
                    changes.get("enabled"),
                    changes.get("threshold_percent"),
                    json.dumps(changes["config"]) if "config" in changes else None,
                    rule_id,
                ),
            )
            result = cursor.fetchone()
            if result is None:
                raise HTTPException(status_code=404, detail="Quality rule not found")
            return result


@app.get("/quality/quarantine")
def quality_quarantine(
    source_name: str | None = None,
    reason: str | None = None,
    include_reprocessed: bool = False,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    conditions = []
    parameters: list[Any] = []
    if source_name:
        conditions.append("source_name=%s")
        parameters.append(source_name)
    if reason:
        conditions.append("reason=%s")
        parameters.append(reason)
    if not include_reprocessed:
        conditions.append("reprocessed_at IS NULL")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    parameters.extend([limit, offset])
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id,pipeline_run_id,source_name,source_row_number,record,reason,"
                "rejected_at,corrected_record,reprocessed_at "
                f"FROM rejected_sales {where} ORDER BY rejected_at DESC,id DESC "
                "LIMIT %s OFFSET %s",
                tuple(parameters),
            )
            return cursor.fetchall()


@app.get("/quality/quarantine/export.csv")
def export_quality_quarantine(
    source_name: str | None = None,
    include_reprocessed: bool = False,
):
    conditions = []
    parameters: list[Any] = []
    if source_name:
        conditions.append("source_name=%s")
        parameters.append(source_name)
    if not include_reprocessed:
        conditions.append("reprocessed_at IS NULL")
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id,pipeline_run_id,source_name,source_row_number,record,reason,"
                "rejected_at,reprocessed_at FROM rejected_sales "
                f"{where} ORDER BY rejected_at DESC,id DESC",
                tuple(parameters),
            )
            rows = cursor.fetchall()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "id",
            "pipeline_run_id",
            "source_name",
            "source_row_number",
            "record",
            "reason",
            "rejected_at",
            "reprocessed_at",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row["id"],
                row["pipeline_run_id"],
                row["source_name"],
                row["source_row_number"],
                json.dumps(row["record"], default=str),
                row["reason"],
                row["rejected_at"],
                row["reprocessed_at"],
            ]
        )
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=dataflow360-quarantine.csv"},
    )


@app.post("/quality/quarantine/{rejected_id}/reprocess", status_code=201)
def reprocess_quarantined_record(
    rejected_id: int,
    corrected_record: dict[str, Any] = Body(...),
):
    validation_rules = [
        {
            "rule_id": rule_id,
            "config": {
                "columns": [
                    "order_id",
                    "product",
                    "category",
                    "quantity",
                    "unit_price",
                    "order_date",
                ]
            },
        }
        for rule_id in (
            "required_fields",
            "unique_business_key",
            "valid_quantity",
            "valid_unit_price",
            "valid_order_date",
        )
    ]
    failures = evaluate_rules([corrected_record], validation_rules)
    failed_rules = [rule_id for rule_id, rows in failures.items() if rows]
    rows, rejected = transform_records([corrected_record])
    if rejected or failed_rules:
        reason = rejected[0]["reason"] if rejected else ", ".join(failed_rules)
        raise HTTPException(status_code=422, detail=f"Corrected record is still invalid: {reason}")

    elapsed_start = time.perf_counter()
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_name,reprocessed_at FROM rejected_sales WHERE id=%s FOR UPDATE",
                (rejected_id,),
            )
            original = cursor.fetchone()
            if original is None:
                raise HTTPException(status_code=404, detail="Quarantined record not found")
            if original["reprocessed_at"] is not None:
                raise HTTPException(status_code=409, detail="Record has already been reprocessed")

            source_name = original["source_name"]
            cursor.execute(
                "INSERT INTO pipeline_runs(accepted,rejected,source_name,state,started_at,"
                "rows_read) VALUES (1,0,%s,'running',now(),1) RETURNING id",
                (f"quarantine:{source_name}",),
            )
            run_id = cursor.fetchone()["id"]
            detect_schema_drift(cursor, f"quarantine:{source_name}", [corrected_record], run_id)
            load_silver_rows(cursor, rows)
            persist_quality_results(
                cursor,
                run_id,
                f"quarantine:{source_name}",
                [corrected_record],
                rows,
                [],
            )
            persist_lineage(cursor, run_id, f"quarantine:{source_name}", 1, 1)
            cursor.execute(
                "UPDATE rejected_sales SET corrected_record=%s::jsonb,reprocessed_at=now() "
                "WHERE id=%s",
                (json.dumps(corrected_record, default=str), rejected_id),
            )
            cursor.execute(
                "UPDATE pipeline_runs SET state='succeeded',finished_at=now() WHERE id=%s",
                (run_id,),
            )
            record_stage(cursor, run_id, "quarantine_reprocessing", 1, 1)

    parquet = write_parquet_snapshot()
    elapsed = time.perf_counter() - elapsed_start
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO processing_executions(pipeline_run_id,engine,dataset_name,input_rows,"
                "accepted_rows,rejected_rows,duration_seconds,throughput_rows_per_second,"
                "output_bytes,state) VALUES (%s,'pandas','quarantine_reprocess',1,1,0,%s,%s,%s,'succeeded')",
                (
                    run_id,
                    round(elapsed, 4),
                    round(1 / elapsed, 4) if elapsed else 0,
                    sum(
                        path.stat().st_size
                        for path in Path(os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet"))).glob("*.parquet")
                    ),
                ),
            )
    return {"pipeline_run_id": run_id, "accepted": 1, "parquet": parquet}


@app.get("/lineage")
def lineage():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT lineage_id,pipeline_run_id,source_name,from_dataset,to_dataset,"
                "from_layer,to_layer,rows_written,created_at,metadata "
                "FROM lineage_events ORDER BY created_at DESC,lineage_id DESC LIMIT 500"
            )
            return cursor.fetchall()


@app.get("/processing")
def processing():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT execution_id,benchmark_id,pipeline_run_id,engine,dataset_name,input_rows,"
                "accepted_rows,rejected_rows,duration_seconds,throughput_rows_per_second,"
                "output_bytes,state,resource_metrics,created_at "
                "FROM processing_executions ORDER BY created_at DESC LIMIT 100"
            )
            executions = cursor.fetchall()
    spark_available, spark_error = spark_runner_status()
    return {
        "engines": [
            {"name": "pandas", "available": True},
            {"name": "pyspark", "available": spark_available, "error": spark_error},
        ],
        "executions": executions,
    }


@app.get("/inventory")
def inventory():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT p.product_name,c.category_name AS category,"
                "coalesce(sum(s.quantity),0) AS quantity_sold,"
                "coalesce(sum(s.revenue),0) AS revenue "
                "FROM silver_sales s "
                "JOIN dim_category c ON c.category_name=s.category "
                "JOIN dim_product p ON p.product_name=s.product "
                "AND p.category_key=c.category_key "
                "GROUP BY p.product_name,c.category_name ORDER BY revenue DESC"
            )
            products = cursor.fetchall()
    return {"products": products}


@app.get("/dimensions")
def dimensions():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT (SELECT count(*) FROM dim_customer) AS customers,"
                "(SELECT count(*) FROM dim_product) AS products,"
                "(SELECT count(*) FROM dim_date) AS dates,"
                "(SELECT count(*) FROM dim_region) AS regions,"
                "(SELECT count(*) FROM dim_supplier) AS suppliers,"
                "(SELECT count(*) FROM gold_sales_fact) AS sales_facts,"
                "(SELECT count(*) FROM inventory_fact) AS inventory_facts"
            )
            return cursor.fetchone()


@app.get("/medallion")
def medallion_status():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT (SELECT count(*) FROM bronze_sales) AS bronze,"
                "(SELECT count(*) FROM silver_sales) AS silver,"
                "(SELECT count(*) FROM gold_sales_fact) AS gold"
            )
            database_counts = cursor.fetchone()
            cursor.execute(
                "SELECT lineage_id,pipeline_run_id,source_name,from_dataset,to_dataset,"
                "from_layer,to_layer,rows_written,created_at,metadata "
                "FROM lineage_events ORDER BY created_at DESC,lineage_id DESC LIMIT 100"
            )
            lineage_events = cursor.fetchall()
    output_dir = Path(os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")))
    files = parquet_layer_status(output_dir)
    return {
        "database": database_counts,
        "parquet": files,
        "lineage": lineage_events,
    }


@app.get("/analytics")
def analytics(
    category: str | None = None,
    region: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    conditions = []
    filters = []
    if category is not None:
        conditions.append("category = %s")
        filters.append(category)
    if region is not None:
        conditions.append("region = %s")
        filters.append(region)
    if start_date is not None:
        conditions.append("order_date >= %s")
        filters.append(start_date)
    if end_date is not None:
        conditions.append("order_date <= %s")
        filters.append(end_date)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    filter_params = tuple(filters)
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT coalesce(sum(revenue),0) AS revenue,"
                f"coalesce(sum(quantity),0) AS units,count(*) AS records "
                f"FROM silver_sales {where}",
                filter_params,
            )
            metrics = cursor.fetchone()
            cursor.execute(
                f"SELECT region,sum(revenue) AS revenue FROM silver_sales {where} "
                "GROUP BY region ORDER BY revenue DESC",
                filter_params,
            )
            regions = cursor.fetchall()
            cursor.execute(
                f"SELECT category,sum(revenue) AS revenue FROM silver_sales {where} "
                "GROUP BY category ORDER BY revenue DESC",
                filter_params,
            )
            categories = cursor.fetchall()
            cursor.execute(
                f"SELECT date_trunc('month',order_date)::date AS month,sum(revenue) AS revenue "
                f"FROM silver_sales {where} GROUP BY month ORDER BY month",
                filter_params,
            )
            monthly = cursor.fetchall()
            cursor.execute("SELECT DISTINCT category FROM silver_sales ORDER BY category")
            category_options = [row["category"] for row in cursor.fetchall()]
            cursor.execute("SELECT DISTINCT region FROM silver_sales ORDER BY region")
            region_options = [row["region"] for row in cursor.fetchall()]
    return {
        "metrics": metrics,
        "regions": regions,
        "categories": categories,
        "monthly": monthly,
        "filters": {"categories": category_options, "regions": region_options},
    }


BI_CORE_DATASETS = (
    {
        "name": "fact_sales",
        "schema": "bi",
        "relation": "fact_sales",
        "description": "Gold sales fact; one row per order line (order_id, line_number).",
        "refresh_column": "updated_at",
        "revenue_column": "revenue",
    },
    {
        "name": "fact_inventory",
        "schema": "bi",
        "relation": "fact_inventory",
        "description": "Inventory snapshot; one row per product and date.",
        "refresh_column": None,
    },
    {
        "name": "fact_pipeline_execution",
        "schema": "bi",
        "relation": "fact_pipeline_execution",
        "description": "One row per persisted pipeline run.",
        "refresh_column": "finished_at",
    },
    {
        "name": "fact_processing_execution",
        "schema": "bi",
        "relation": "fact_processing_execution",
        "description": "One row per persisted engine execution or benchmark.",
        "refresh_column": "created_at",
    },
    {
        "name": "fact_data_quality",
        "schema": "bi",
        "relation": "fact_data_quality",
        "description": "One row per pipeline run and evaluated quality rule.",
        "refresh_column": "checked_at",
    },
    {
        "name": "fact_quality_run",
        "schema": "bi",
        "relation": "fact_quality_run",
        "description": "One row per pipeline run with overall quality metrics.",
        "refresh_column": "checked_at",
    },
    {
        "name": "dim_customer",
        "schema": "bi",
        "relation": "dim_customer",
        "description": "Customer dimension keyed by customer_key.",
        "refresh_column": None,
    },
    {
        "name": "dim_product",
        "schema": "bi",
        "relation": "dim_product",
        "description": "Product dimension with category attributes.",
        "refresh_column": None,
    },
    {
        "name": "dim_date",
        "schema": "bi",
        "relation": "dim_date",
        "description": "Calendar dimension keyed by date_key.",
        "refresh_column": None,
    },
    {
        "name": "dim_region",
        "schema": "bi",
        "relation": "dim_region",
        "description": "Region dimension keyed by region_key.",
        "refresh_column": None,
    },
    {
        "name": "dim_supplier",
        "schema": "bi",
        "relation": "dim_supplier",
        "description": "Supplier dimension keyed by supplier_key.",
        "refresh_column": None,
    },
)
BI_DBT_MODELS = (
    "fct_sales_line",
    "customer_lifetime_value",
    "daily_sales",
    "monthly_sales",
    "regional_revenue",
    "product_performance",
    "product_category_performance",
    "category_revenue",
    "inventory_turnover",
    "low_stock_products",
)
BI_EXPORT_RELATIONS = {
    item["name"]: (item["schema"], item["relation"])
    for item in BI_CORE_DATASETS
}
BI_EXPORT_RELATIONS.update(
    {
        f"dbt_{model}": ("analytics", model)
        for model in BI_DBT_MODELS
    }
)


@app.get("/bi/datasets")
def bi_datasets():
    datasets = []
    with connect() as conn:
        with conn.cursor() as cursor:
            for item in BI_CORE_DATASETS:
                refresh = item["refresh_column"]
                refresh_sql = f', max("{refresh}") AS last_refreshed' if refresh else (
                    ", NULL::timestamptz AS last_refreshed"
                )
                revenue = item.get("revenue_column")
                revenue_sql = f', coalesce(sum("{revenue}"), 0)::text AS revenue_total' if revenue else (
                    ", NULL::text AS revenue_total"
                )
                cursor.execute(
                    f'SELECT count(*)::bigint AS row_count{refresh_sql}{revenue_sql} '
                    f'FROM "{item["schema"]}"."{item["relation"]}"'
                )
                metrics = cursor.fetchone()
                datasets.append({**item, **metrics, "kind": "warehouse"})

            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='analytics' AND table_type IN ('BASE TABLE','VIEW') "
                "AND table_name = ANY(%s) ORDER BY table_name",
                (list(BI_DBT_MODELS),),
            )
            available_dbt_models = {row["table_name"] for row in cursor.fetchall()}
            for model in BI_DBT_MODELS:
                if model not in available_dbt_models:
                    continue
                refresh_sql = (
                    ", max(source_updated_at) AS last_refreshed"
                    if model == "fct_sales_line"
                    else ", NULL::timestamptz AS last_refreshed"
                )
                revenue_sql = (
                    ", coalesce(sum(revenue), 0)::text AS revenue_total"
                    if model == "fct_sales_line"
                    else ", NULL::text AS revenue_total"
                )
                cursor.execute(
                    f'SELECT count(*)::bigint AS row_count{refresh_sql}{revenue_sql} '
                    f'FROM "analytics"."{model}"'
                )
                metrics = cursor.fetchone()
                datasets.append(
                    {
                        "name": f"dbt_{model}",
                        "schema": "analytics",
                        "relation": model,
                        "description": f"Optional dbt analytical model: {model}.",
                        "kind": "dbt",
                        **metrics,
                    }
                )
    return {"datasets": datasets}


@app.get("/bi/model")
def bi_model():
    return {
        "primary_source": "PostgreSQL Gold warehouse; dbt analytics marts are optional reporting sources.",
        "grain": {
            "fact_sales": "One row per order_id and line_number.",
            "fact_inventory": "One row per product_key and date_key.",
            "fact_pipeline_execution": "One row per pipeline_runs.id.",
            "fact_processing_execution": "One row per processing_executions.execution_id.",
            "fact_data_quality": "One row per quality result/rule and pipeline run.",
            "fact_quality_run": "One row per quality_run_summaries.pipeline_run_id.",
        },
        "relationships": [
            {"many": "fact_sales", "many_key": "customer_key", "one": "dim_customer", "one_key": "customer_key"},
            {"many": "fact_sales", "many_key": "product_key", "one": "dim_product", "one_key": "product_key"},
            {"many": "fact_sales", "many_key": "date_key", "one": "dim_date", "one_key": "date_key"},
            {"many": "fact_sales", "many_key": "region_key", "one": "dim_region", "one_key": "region_key"},
            {"many": "fact_sales", "many_key": "supplier_key", "one": "dim_supplier", "one_key": "supplier_key"},
            {"many": "fact_inventory", "many_key": "product_key", "one": "dim_product", "one_key": "product_key"},
            {"many": "fact_inventory", "many_key": "date_key", "one": "dim_date", "one_key": "date_key"},
            {"many": "fact_inventory", "many_key": "region_key", "one": "dim_region", "one_key": "region_key"},
            {"many": "fact_inventory", "many_key": "supplier_key", "one": "dim_supplier", "one_key": "supplier_key"},
            {"many": "fact_pipeline_execution", "many_key": "started_date", "one": "dim_date", "one_key": "full_date"},
            {"many": "fact_processing_execution", "many_key": "created_date", "one": "dim_date", "one_key": "full_date"},
            {"many": "fact_data_quality", "many_key": "checked_date", "one": "dim_date", "one_key": "full_date"},
            {"many": "fact_quality_run", "many_key": "checked_date", "one": "dim_date", "one_key": "full_date"},
        ],
        "filter_direction": "Single direction from each dimension to its fact; do not enable bidirectional filtering.",
        "dbt_models": [f"analytics.{model}" for model in BI_DBT_MODELS],
    }


@app.get("/bi/exports/{dataset}.csv")
def export_bi_dataset(dataset: str):
    relation = BI_EXPORT_RELATIONS.get(dataset)
    if relation is None:
        raise HTTPException(status_code=404, detail="Unknown BI dataset")
    schema, table = relation
    if schema == "analytics":
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT to_regclass(%s) AS relation", (f"{schema}.{table}",))
                if cursor.fetchone()["relation"] is None:
                    raise HTTPException(status_code=404, detail="dbt model is not currently available")

    def rows():
        with connect() as conn:
            with conn.cursor(name=f"bi_export_{dataset}", row_factory=dict_row) as cursor:
                cursor.execute(f'SELECT * FROM "{schema}"."{table}"')
                columns = [column.name for column in cursor.description]
                output = io.StringIO(newline="")
                writer = csv.writer(output)
                writer.writerow(columns)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
                while batch := cursor.fetchmany(1000):
                    writer.writerows(
                        [[row[column] for column in columns] for row in batch]
                    )
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)

    return StreamingResponse(
        rows(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="dataflow360-{dataset}.csv"',
            "Cache-Control": "no-store",
        },
    )
