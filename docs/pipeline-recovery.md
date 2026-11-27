# Pipeline execution and recovery

The frontend trigger submits an Airflow DAG run and returns its real run ID.
The Airflow task calls the authenticated backend execution endpoint. Pipeline
execution is asynchronous with respect to the browser request; the dashboard
polls Airflow for actual DAG and task states. Airflow task retries are bounded
and failed tasks can be cleared through the Airflow API retry action.

The backend persists run state, per-stage row counts, errors, quality results,
and ingestion watermarks in PostgreSQL. Warehouse writes and watermark
advancement occur in one database transaction. A quality-gate failure rolls
back warehouse writes to a savepoint, keeps Bronze and quarantine audit data,
marks the execution failed, and does not publish replacement Parquet files or
advance the source watermark.

Repeated unchanged inputs are skipped using a source hash. Warehouse sales
lines are upserted at `(order_id, line_number)` grain, so retries update an
existing line rather than removing legitimate order items or creating a second
fact. Spark output directories are correlated with the Airflow run ID.

## Operational checks

```powershell
docker compose ps
docker compose logs --tail 100 airflow-webserver
docker compose logs --tail 100 airflow-scheduler
docker compose logs --tail 100 backend
curl.exe http://localhost:8000/health
curl.exe http://localhost:8080/health
```

Do not delete PostgreSQL volumes to recover from an application failure.
Inspect the relevant container logs and database state first. Current local
verification found that Airflow metadata initialization completed but the
webserver exited during Gunicorn startup with `No response from gunicorn
master`. Thus live DAG triggering, polling, task retries, and end-to-end
recovery remain unverified in this workspace. The code does not simulate
success when Airflow is unavailable.
