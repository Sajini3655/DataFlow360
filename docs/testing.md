# Testing guide

## Backend

Install backend and test dependencies in a dedicated environment:

```powershell
python -m pip install -r backend/requirements-test.txt
Set-Location backend
python -m unittest discover -s tests -v
```

The complete suite includes unit tests and database-backed cases guarded by
`DATABASE_URL`. For database integration, point to an isolated disposable
PostgreSQL database, not the normal development volume:

```powershell
$env:DATABASE_URL = "postgresql://dataflow:ci-password@localhost:5432/dataflow_test"
python -c "import main"
python -m unittest discover -s tests -v
```

Importing `main` applies the checked-in warehouse schema and migrations. Use a
throwaway database because schema initialization is not a test rollback.
`test_warehouse_upsert_integration.py` uses a transaction savepoint to clean
up its own row changes.

Inside Docker, the test directory is not copied into the runtime backend image
by the current Dockerfile. For local focused test execution, mount or copy the
tests into a development/test image rather than treating a missing in-image
test directory as a test pass.

## Frontend

```powershell
Set-Location frontend
npm ci
npm run typecheck
npm run build
```

ESLint is not configured in this repository and is therefore not reported as
a passing check.

## ETL, quality, and synthetic fixture

```powershell
python scripts/generate_synthetic_data.py --records 100 --output-dir $env:TEMP\dataflow-fixture
Set-Location backend
python -m unittest discover -s tests -p "test_pipeline.py" -v
python -m unittest discover -s tests -p "test_data_quality.py" -v
python -m unittest discover -s tests -p "test_warehouse_upsert_integration.py" -v
```

Generator outputs are isolated in the temporary directory. A full-size
dataset and 50,000-row Pandas/Spark benchmark are intentionally not part of
ordinary commit checks.

## dbt and optional integrations

Use an isolated PostgreSQL service. In the dbt folder, create a local
`profiles.yml` from `profiles.yml.example`, set `DBT_*` environment values,
then run `dbt deps`, `dbt parse`, and `dbt build`. GitHub data-engineering CI
does the latter against its ephemeral database. Airflow DAG import and small
PySpark equivalence checks are configured as manually dispatched workflows.
No live Azure, Fabric, or Power BI service is required or tested here.

## Reports and CI

Backend CI emits JUnit XML as a workflow artifact. All other job output is
available in the Actions logs. Remote status remains unverified until the
workflows have executed on GitHub.
