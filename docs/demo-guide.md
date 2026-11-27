# Portfolio demonstration guide

Use the local development stack on a machine with the existing sample data.
Do not expose the demo publicly without HTTPS and access controls.

## Start-up

```powershell
Copy-Item .env.example .env
# Replace all placeholder credentials locally.
docker compose up --build -d
Set-Location frontend
npm ci
npm run dev
```

Wait for PostgreSQL, backend, and Airflow health before running a pipeline.
The dashboard is `http://localhost:3000`, API docs are
`http://localhost:8000/docs`, and local Airflow is `http://localhost:8080`.

## Five-minute guided walkthrough

1. **Overview:** explain that dashboard metrics are API/database-derived.
2. **Sources:** describe CSV/JSON/REST/PostgreSQL adapters; show the current
   Airflow run path only if Airflow health is green.
3. **Data Quality:** explain completeness, uniqueness at order-line grain,
   validation, quality thresholds, and quarantine.
4. **Medallion and Star Schema:** show persisted Bronze/Silver/Gold and
   customer/product/date/region/supplier dimensions. Explain that order IDs
   can have multiple legitimate lines.
5. **Analytics and Inventory:** show actual revenue and units, filters, and
   the inventory snapshot grain.
6. **Processing:** present persisted Pandas/PySpark metrics with the caveat
   that the recorded 50k benchmark was environment-specific and PySpark was
   slower.
7. **Airflow / Cloud / BI:** explain actual Airflow state polling, local
   default cloud storage with unverified external Azure/Fabric execution, and
   BI reporting views/CSV assets. Power BI Desktop is not embedded.

For the Power BI page, use `http://localhost:8000/bi/datasets` and select a
CSV export if demonstrating without Power BI Desktop.

## Limitations to state explicitly

- Airflow uses a local debug webserver configuration and unresolved Gunicorn
  startup issue.
- dbt is optional and its Airflow task is disabled unless configured.
- The local Spark benchmark showed Pandas faster for the measured 50k case.
- Azure/ADF/Fabric assets are not proof of successful cloud execution.
- Power BI Desktop, PBIX/PBIP, M/DAX execution, DirectQuery, and report
  rendering are unverified.
- This is a portfolio demonstration, not production-deployed software.
