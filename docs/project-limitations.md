# Current project limitations

## Deployment and security

- No application has been deployed to a cloud or production environment.
- FastAPI does not provide end-user authentication or role authorization.
  Public demonstration requires an external TLS and authentication gateway,
  protected administrative routes, and strict network rules.
- The backend container does not yet run as a dedicated non-root user.
- Backup automation, monitoring/alerting, tested disaster recovery, rate
  limiting, and production capacity planning are not included.
- Airflow's local webserver runs with `--debug`; the Gunicorn startup problem
  remains unresolved.

## Optional services

- dbt and Spark require their optional profiles and local configuration.
- Azure storage, ADF, and Fabric notebook assets are optional; no live Azure
  account, factory, or Fabric workspace has been used for verification.
- Power BI Desktop, PBIX/PBIP generation, M and DAX execution, DirectQuery,
  Power BI Service refresh, and report rendering remain unverified.
- GitHub Actions and Dependabot are defined in this branch but have not yet
  been executed remotely.
- Gitleaks, pip-audit, and npm audit results are pending their first workflow
  run. Do not infer a clean vulnerability state before scanning.

## Data and performance

- The sample and generated data are synthetic, not representative customer
  data.
- Parquet snapshots are full-table rewrites on successful runs.
- The 50,000-record benchmark was environment-specific: Pandas 5.71s /
  9,236 rows/s versus PySpark 91.63s / 576 rows/s. It does not show a Spark
  speed advantage.
- The warehouse inventory schema stores unit snapshots, not cost valuation or
  supplier reorder points.
- Airflow has one ETL task and an optional dbt task; individual ETL stages are
  recorded in PostgreSQL rather than mapped to separate DAG tasks.

These are known boundaries to communicate in a portfolio demonstration, not
claims that the software is production-ready.
