# Processing benchmark status

No measured Pandas-versus-PySpark benchmark result is reported here because the
optional Spark profile has not yet been built and executed in the current
workspace. The application stores benchmark executions in
`processing_executions` and provides the Processing page to display actual
results after a run. It does not claim that Spark is faster.

## Reproducible benchmark

Configure `.env`, generate a dataset, and start the optional Spark profile:

```powershell
docker compose exec backend python /app/scripts/generate_synthetic_data.py --records 50000
docker compose --profile spark up --build -d
curl.exe -X POST "http://localhost:8000/processing/benchmark"
```

Wait for the real Airflow run to finish, then inspect the Processing page or:

```powershell
curl.exe http://localhost:8000/processing
```

The benchmark compares identical input and verifies input, accepted, rejected,
and revenue equivalence. The persisted metrics include execution duration,
throughput, and output size. Record actual measured values and the host resource
configuration here after the run; do not copy estimates or results from a
different machine.
