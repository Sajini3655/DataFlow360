# Portfolio materials

## CV project description

- Built a retail data-engineering portfolio application with Python/FastAPI,
  PostgreSQL, Apache Airflow, Docker Compose, and a Next.js dashboard.
- Implemented an order-line-grain ETL flow with Bronze/Silver/Gold persistence,
  data-quality checks and quarantine, incremental warehouse upserts, dbt
  marts, and Parquet outputs.
- Added optional Spark/Azure/Fabric/Power BI integration assets and CI/deploy
  definitions; clearly documented that external cloud/BI execution and
  production deployment remain unverified.

## LinkedIn description

DataFlow360 is a portfolio project exploring a complete retail data
engineering lifecycle: source ingestion, validation and quarantine, medallion
storage, PostgreSQL dimensional modeling, Airflow orchestration, dbt
analytics, and a backend-driven dashboard. It includes optional integration
assets for Spark, Azure Data Lake, Microsoft Fabric, and Power BI. Local
features and external integration limitations are documented separately.

## GitHub repository description

Retail data-engineering portfolio project with Airflow ETL, PostgreSQL
warehouse, dbt, data quality, Parquet, and optional analytics integrations.

## Recommended topics

`data-engineering`, `etl`, `apache-airflow`, `postgresql`, `dbt`, `pyspark`,
`data-quality`, `medallion-architecture`, `nextjs`, `fastapi`, `docker-compose`,
`power-bi`, `azure-data-lake`

## Three-minute demonstration

1. **0:00–0:25 — Problem:** Explain the need to make raw transactions reliable
   and analytically useful.
2. **0:25–0:55 — Architecture:** Show sources, FastAPI, Airflow, PostgreSQL,
   and the Next.js dashboard.
3. **0:55–1:25 — Data quality:** Demonstrate validation outcomes, persisted
   rejected rows, and quality metrics.
4. **1:25–1:55 — Warehouse:** Explain Bronze/Silver/Gold and the
   `(order_id, line_number)` fact grain.
5. **1:55–2:25 — Orchestration and analytics:** Show a real Airflow execution
   state if services are healthy, then database-backed analytics/BI dataset
   counts.
6. **2:25–3:00 — Limits and next steps:** State the benchmark result and that
   Azure, Fabric, Power BI Desktop, and production deployment are unverified.

## Five-minute technical explanation

1. **Business case (0:00–0:40):** Synthetic retail records include intentional
   quality issues so the flow can demonstrate validation and observability.
2. **Ingestion/ETL (0:40–1:35):** CSV, JSON, REST, and PostgreSQL adapters feed
   Bronze payload persistence, validation, rejection/quarantine, and Silver
   normalization.
3. **Warehouse (1:35–2:15):** Explain dimensions, generated revenue, order-line
   grain, Gold facts, and incremental upserts.
4. **Orchestration (2:15–2:55):** Airflow triggers backend execution and
   supplies the internal token; status polling reads actual run state.
5. **Analytics (2:55–3:40):** dbt builds analytics models; PostgreSQL BI views
   power CSV exports and the dashboard. Parquet snapshots persist separately.
6. **Processing/quality (3:40–4:20):** Compare Pandas and Spark results and
   discuss persisted quality checks; report the local 50k benchmark as
   environment-specific with Pandas faster.
7. **Integrations and limits (4:20–5:00):** Describe Azure/Fabric/Power BI
   assets without claiming external execution; identify authentication,
   security, backups, and monitoring as production gaps.

## Technologies with execution evidence

Previously reported local execution/verification includes Python 3.12,
FastAPI, PostgreSQL 16, Docker Compose, Node.js/Next.js, Apache Airflow 2.10.5,
dbt 1.8.8 with dbt-postgres 1.8.2, Java 17, PySpark 3.5.6, Pandas 2.2.3, and
PyArrow 18.1.0. The v0.5 BI change also passed local frontend build/type
checks and focused database tests. Refer to README verification notes for
dated counts and measurements; prior results are not a guarantee of current
environment behavior.

## Integration assets not externally verified

- Azure Data Lake Storage Gen2 adapter tests use mocked clients; no live
  storage account was connected.
- Azure Data Factory templates were not deployed to a live factory.
- Fabric notebook assets were not executed in a real Fabric workspace.
- Power BI Desktop, PBIX/PBIP, M/DAX, DirectQuery, Power BI Service refresh,
  and rendered report pages were not verified.
- GitHub Actions/Dependabot in the production-readiness branch require
  publication and remote execution.
- The lightweight deployment Compose file is a configuration asset; no
  portfolio host or production environment has been deployed.
