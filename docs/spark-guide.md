# Optional PySpark processing

The Spark profile is optional; the default Compose stack does not start Spark.
The runner is a token-protected FastAPI service that Apache Airflow calls for a
PySpark run or a Pandas/PySpark equivalence benchmark. It reads the generated
CSV, writes a Bronze Parquet run dataset, validates and transforms it with
PySpark, and produces Silver, rejected, and Gold Parquet outputs. A successful
PySpark DAG run then loads its Silver and rejected rows into the existing
PostgreSQL warehouse. The warehouse remains the source of truth.

## Start the optional services

Set the local variables in `.env`, including `PIPELINE_INTERNAL_TOKEN`, then:

```powershell
docker compose --profile spark up --build -d
docker compose --profile spark ps
curl.exe http://localhost:8083/health
```

The Spark UI is at http://localhost:8082. The internal runner listens on port
8083. The profile uses Java 17, Python 3.12, and PySpark 3.5.6. Resource limits
are set on the Spark master and worker; adjust them for the resources available
on the local machine.

## Generate and process datasets

From the project root, choose one of the supported base transaction counts:

```powershell
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 50000
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 100000
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 500000
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 1000000
```

Each dataset includes controlled missing, duplicate, malformed-price, and
invalid-quantity rows. Generation is opt-in; the million-row dataset is not
created automatically. With the optional Spark services available, use the
Processing page or API:

```powershell
curl.exe -X POST "http://localhost:8000/processing/run?engine=pyspark"
curl.exe -X POST "http://localhost:8000/processing/benchmark"
```

The endpoints submit real Airflow DAG runs; they do not execute synchronously
or return simulated success. The Processing page reads execution statistics
from `/processing`. Run state and task details are read from Airflow.

## Processing and benchmark details

The transformation uses an explicit input schema, Spark DataFrames, a
window-based business-key duplicate check, aggregations, caching, partitioned
Parquet output, and actions that materialize counts and revenue. A benchmark
uses the same CSV and compares Pandas and PySpark input, accepted, rejected,
and revenue values. It fails if those results differ. The app persists
duration, throughput, output bytes, engine, row counts, and available output
paths. It makes no claim that Spark is faster.

The Spark runner and optional Compose profile have not yet been successfully
built or executed in the current workspace. Consult
[benchmark-results.md](./benchmark-results.md) for verified measurements and
remaining validation status.
