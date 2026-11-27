# DataFlow360 v0.3

> An end-to-end enterprise data engineering platform featuring automated ETL
> pipelines, Apache Airflow orchestration, medallion architecture, PostgreSQL
> data warehousing, data quality monitoring, and interactive business analytics.

Local retail data platform with a Next.js dashboard, FastAPI API, PostgreSQL
warehouse, Airflow orchestration, optional dbt and PySpark services, synthetic
transaction generator, quality monitoring, and Parquet persistence. The sample
dataset is synthetic. Compose uses development-only credentials and is not
production hardened.

Screenshots are not included yet.

## Architecture

```mermaid
flowchart LR
    Sources[CSV / JSON / REST / PostgreSQL] --> Worker[FastAPI ETL worker and adapters]
    UI[Next.js dashboard] --> API[FastAPI]
    API -->|submit run and read status| Airflow
    Airflow[Apache Airflow DAG] -->|authenticated execute request| Worker
    Airflow -->|optional dbt build| DBT[dbt analytics models]
    Airflow -->|optional PySpark / benchmark| Spark[PySpark runner]
    Worker -->|ingest and transform| Warehouse[(PostgreSQL: Bronze, Silver, Gold, star schema)]
    Worker --> Parquet[(Bronze / Silver / Gold Parquet)]
    DBT --> Warehouse
    Spark --> Parquet
    Spark --> Warehouse
    API -->|query warehouse| Warehouse
    UI -->|metrics, filters, and actions| API
```

The small `data/sales.csv` fixture is tracked. The 50,000-row generated CSV and
JSON files and runtime Parquet outputs are excluded; keep
`scripts/generate_synthetic_data.py` to regenerate the scale-test inputs.

## Run from a fresh checkout

Requirements: Docker Desktop, Node.js 22+, and npm.

```powershell
cd C:\path\to\DataFlow360
Copy-Item .env.example .env
# Replace the placeholder values in .env with local development credentials.
docker compose up --build -d
cd frontend
npm ci
npm run dev
```

Services:

- Dashboard: http://localhost:3000
- API docs: http://localhost:8000/docs
- Airflow UI: http://localhost:8080 (username and password from `.env`)
- PostgreSQL: `localhost:5432`

Compose starts PostgreSQL, backend, and Airflow services. Optional profiles
enable dbt and Spark:

```powershell
docker compose --profile dbt up --build -d
docker compose --profile spark up --build -d
```

The one-time
`airflow-init` container migrates Airflow metadata before the webserver and
scheduler start. The backend applies checked-in warehouse SQL and migrations on
startup. Existing named PostgreSQL volumes are retained by normal `up` and
`down` commands; do not use `down -v` if you need to preserve local data.

## Implemented pipeline flow

1. `POST /pipeline/run` asks the Airflow REST API to create a DAG run and returns
   HTTP 202 with its real `dag_run_id`. Airflow API failures are surfaced; there
   is no direct-execution success fallback.
2. The Airflow task calls the authenticated internal `/pipeline/execute`
   endpoint. The long-running load executes in the Airflow worker, not in the
   user-facing trigger request.
3. `GET /pipeline/runs/{dag_run_id}` retrieves the current DAG and task states
   from Airflow. The `/airflow` page polls this endpoint and renders those
   states.
4. Successful loads upsert Bronze, Silver, dimensions, and Gold in PostgreSQL,
   then atomically replace the corresponding Parquet snapshots in the mounted
   `data/parquet/` directory.
5. Silver and Gold use `(order_id, line_number)` grain. Repeated order IDs with
   different line numbers are separate sale lines; repeated line keys are
   rejected. Updated line values propagate to Gold and its aggregates.

The local CSV and JSON files are in `data/`. Regenerate the 12-month synthetic
dataset from the project root with:

```powershell
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 50000
```

Supported base dataset sizes are `50000`, `100000`, `500000`, and `1000000`.
Larger files are generated only when explicitly requested. Each size adds
controlled missing, duplicate, malformed-price, and invalid-quantity records:

```powershell
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 100000
```

This step is required before running the scaled CSV/JSON pipeline or the
scaled-dataset tests from a fresh checkout. Regenerating data does not ingest it;
queue a run from the Sources page or use:

```powershell
curl.exe -X POST "http://localhost:8000/pipeline/run?source=csv&incremental=false"
```

Adapters available via `source=csv|json|rest|postgres`. REST reads the local
sample API; PostgreSQL reads Silver. Parquet outputs are
`data/parquet/bronze_sales.parquet`, `silver_sales.parquet`, and
`gold_sales.parquet`.

## API endpoints

- `GET /health`, `GET /overview`, `GET /analytics`
- `GET /quality`, `GET /medallion`, `GET /dimensions`, `GET /inventory`
- `GET /adapters`, `GET /adapters/rest/sample`
- `POST /pipeline/run?source=csv&incremental=true` (asynchronous Airflow trigger)
- `POST /pipeline/run?source=csv&engine=pyspark` (optional Spark profile)
- `POST /processing/benchmark` (Pandas/PySpark result-equivalence benchmark)
- `GET /processing`, `GET /lineage`
- `GET /pipeline/runs/{dag_run_id}` (actual Airflow run/task states)
- `POST /pipeline/execute` (internal token required; called by Airflow only)

The dashboard has nine pages: Overview, Sources, Data Quality, Medallion,
Star Schema, Analytics, Inventory, Processing, and Airflow. Analytics filters
are sent to the backend rather than computed from browser-side demo values.
The Processing page reads engine availability and persisted execution metrics
from the backend and can submit real Airflow requests for Pandas, optional
PySpark, and an equivalence benchmark.

## Optional dbt and Spark

- dbt reads the existing PostgreSQL Silver, dimensions, and inventory tables
  and builds analytics-schema models. See [docs/dbt-guide.md](docs/dbt-guide.md).
- PySpark runs only in the optional `spark` Compose profile and writes
  run-correlated Bronze, Silver, rejected, and Gold Parquet outputs. See
  [docs/spark-guide.md](docs/spark-guide.md).
- Benchmark results are persisted and displayed only after real runs; no
  performance advantage is claimed without measurements. See
  [docs/benchmark-results.md](docs/benchmark-results.md).
- Quality rules, thresholds, quarantine, schema drift, and lineage are
  described in [docs/data-quality.md](docs/data-quality.md) and
  [docs/pipeline-recovery.md](docs/pipeline-recovery.md). SQL examples are in
  [docs/sql-examples.md](docs/sql-examples.md).

## Tests and checks

```powershell
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 50000
docker compose exec backend python -m unittest discover -s tests
cd frontend
npm run build
```

Useful checks:

```powershell
curl.exe http://localhost:8000/health
curl.exe http://localhost:8080/health
curl.exe "http://localhost:8000/analytics?region=North"
docker compose exec db psql -U dataflow -d dataflow -c "select count(*) from silver_sales;"
```

## Verification status and known limitations

- Local verification completed: the backend suite passed **21 tests**,
  including the PostgreSQL Gold upsert integration test; the frontend
  production build/type check passed; Docker Compose validation passed; and all
  nine dashboard routes returned HTTP 200.
- dbt 1.8.8 / adapter 1.8.2: `dbt debug` passed, `dbt build` passed **41/41**,
  `dbt test` passed **19/19**, and docs generation succeeded. Incremental
  insert/update and idempotency checks passed. These commands were run
  separately; the optional Airflow dbt task remains disabled by default.
- Spark: Java 17 and Spark/PySpark 3.5.6 were verified. The 50,000-row
  equivalence benchmark passed. The recorded runs processed 52,750 rows
  (50,000 accepted, 2,750 rejected): Pandas completed in 5.71s
  (9,236 rows/s) and PySpark in 91.63s (576 rows/s). These are local,
  environment-specific measurements; PySpark was slower in this benchmark.
- Airflow: a real full ETL DAG run succeeded after retry, reading 52,750 rows
  and accepting 50,000 while rejecting 2,750; backend status polling returned
  actual task states. The downstream `dbt_build` task was skipped because
  `DBT_ENABLED` is false. Local runs currently use Airflow's `webserver
  --debug` mode after the standard Gunicorn startup exceeded its timeout; the
  underlying Gunicorn startup cause remains unresolved, so this is not a
  production webserver configuration.
- The current verified PostgreSQL layer counts are 155,603 Bronze,
  50,002 Silver, and 50,002 Gold rows. The two extra Silver/Gold rows are
  existing records; the latest full run itself accepted 50,000 rows. The
  `quantity_range` rule allows a 2% fixture failure rate; the observed 750 of
  52,750 rows (1.4218%) passed that configured threshold.
- Parquet snapshots are full-table rewrites after each successful ETL run and
  can become expensive as data grows. CSV/JSON are local-file adapters; the
  REST and PostgreSQL adapters target local demonstration services.
- The Airflow DAG has one ETL task and an optional downstream dbt task; it does
  not expose each ETL stage as a separate Airflow task. Backend stage-level
  execution metadata does not replace that DAG decomposition.
- `.env.example` contains placeholders. Keep local secrets in the ignored
  `.env`, never commit it, and use a secrets manager outside local development.
  Cloud deployment and BI integrations are not part of this implementation.
