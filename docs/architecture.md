# DataFlow360 architecture

This describes the checked-in implementation. Azure Data Factory and Fabric
components are deployment assets, not live integrations. The dashboard reads
the FastAPI service; PostgreSQL is the operational and analytical source of
truth.

## 1. Overall system

```mermaid
flowchart LR
  Sources[CSV / JSON / REST / PostgreSQL] --> Airflow[Airflow DAG]
  UI[Next.js dashboard] --> API[FastAPI]
  API -->|authenticated trigger and status| Airflow
  Airflow -->|internal token| API
  API --> PG[(PostgreSQL)]
  API --> Files[Bronze / Silver / Gold Parquet]
  API -->|optional| Storage[Local adapter / optional ADLS]
  PG --> dbt[Optional dbt analytics schema]
  PG --> BI[BI reporting views and CSV exports]
  BI --> Desktop[Power BI Desktop assets; not rendered]
  Spark[Optional Spark runner] --> PG
  Spark --> Files
  ADF[ADF templates] -. configured separately .-> Storage
  Fabric[Fabric notebook assets] -. import/run separately .-> OneLake[OneLake]
```

## 2. ETL flow

```mermaid
flowchart LR
  Input[Source adapter] --> Bronze[Persist raw payload and source row]
  Bronze --> Validate[Schema drift and configured quality rules]
  Validate -->|accepted| Transform[Normalize and derive revenue]
  Validate -->|rejected| Quarantine[Rejected sales / quarantine]
  Transform --> Silver[Upsert Silver on order_id + line_number]
  Silver --> Dimensions[Upsert dimensions and date keys]
  Dimensions --> Gold[Upsert Gold sales facts]
  Gold --> Publish[Refresh Parquet snapshots]
  Publish --> Metrics[Persist stage, quality, lineage, and processing metrics]
```

## 3. Medallion layers

```mermaid
flowchart LR
  Source[Source records] --> Bronze[(Bronze raw JSON payloads)]
  Bronze --> Accepted[Validation and transformation]
  Bronze --> Rejected[(Rejected / quarantine)]
  Accepted --> Silver[(Silver normalized sales lines)]
  Silver --> Gold[(Gold star-schema facts and dimensions)]
  Bronze --> P1[bronze_sales.parquet]
  Silver --> P2[silver_sales.parquet]
  Gold --> P3[gold_sales.parquet]
```

The database and Parquet outputs are both persisted. A successful ETL execution
publishes current full-table Parquet snapshots; these are not partitioned
incremental files.

## 4. PostgreSQL warehouse star schema

```mermaid
erDiagram
  DIM_CUSTOMER ||--o{ GOLD_SALES_FACT : customer_key
  DIM_PRODUCT ||--o{ GOLD_SALES_FACT : product_key
  DIM_DATE ||--o{ GOLD_SALES_FACT : date_key
  DIM_REGION ||--o{ GOLD_SALES_FACT : region_key
  DIM_SUPPLIER ||--o{ GOLD_SALES_FACT : supplier_key
  DIM_PRODUCT ||--o{ INVENTORY_FACT : product_key
  DIM_DATE ||--o{ INVENTORY_FACT : date_key
  DIM_REGION ||--o{ INVENTORY_FACT : region_key
  DIM_SUPPLIER ||--o{ INVENTORY_FACT : supplier_key
```

Sales grain is `(order_id, line_number)`. Inventory is one record per
`(product_key, date_key)`. The optional BI relationship design is documented
in [`../powerbi/semantic-model/star-schema.md`](../powerbi/semantic-model/star-schema.md).

## 5. Airflow and backend execution

```mermaid
sequenceDiagram
  participant Browser
  participant API as FastAPI
  participant AF as Airflow REST API
  participant Task as Airflow PythonOperator
  participant DB as PostgreSQL
  Browser->>API: POST /pipeline/run
  API->>AF: authenticated create DAG run
  AF-->>API: real dag_run_id
  API-->>Browser: HTTP 202 queued
  Task->>API: POST /pipeline/execute + internal token
  API->>DB: ETL and warehouse upserts
  Browser->>API: GET /pipeline/runs/{dag_run_id}
  API->>AF: retrieve DAG and task states
  AF-->>API: current execution state
  API-->>Browser: actual state and persisted execution details
```

The DAG contains one ETL task and an optional downstream dbt task; ETL stages
are persisted in PostgreSQL but are not individual Airflow tasks.

## 6. dbt lineage

```mermaid
flowchart LR
  Silver[(public.silver_sales)] --> Staging[staging views]
  Customer[(public.dim_customer)] --> Staging
  Product[(public.dim_product)] --> Staging
  Supplier[(public.dim_supplier)] --> Staging
  Region[(public.dim_region)] --> Staging
  Inventory[(public.inventory_fact)] --> Staging
  Staging --> Intermediate[intermediate enrichment]
  Intermediate --> Sales[analytics.fct_sales_line]
  Sales --> CustomerValue[customer_lifetime_value]
  Sales --> Monthly[monthly_sales]
  Sales --> Daily[daily_sales]
  Sales --> Regional[regional_revenue]
  Sales --> ProductMetrics[product and category performance]
  Intermediate --> InventoryMetrics[inventory turnover and low stock]
```

## 7. Pandas and Spark processing

```mermaid
flowchart LR
  Request[Airflow DAG run] --> Engine{engine}
  Engine -->|pandas| API[FastAPI ETL worker]
  Engine -->|pyspark / benchmark| Runner[Optional Spark runner]
  Runner --> Master[Spark master and worker]
  API --> PG[(PostgreSQL)]
  Runner --> PG
  API --> Parquet[(Parquet output)]
  Runner --> Parquet
  API --> Metrics[processing_executions]
  Runner --> Metrics
```

Spark is an optional profile. The previously recorded local 50k equivalence
benchmark was slower on PySpark than Pandas; see
[`benchmark-results.md`](./benchmark-results.md).

## 8. Data quality and quarantine

```mermaid
flowchart TD
  Rows[Incoming rows] --> Drift[Infer schema and detect drift]
  Drift --> Rules[Evaluate enabled configured rules]
  Rules --> Threshold{Threshold exceeded?}
  Threshold -->|no| Accept[Transform and load accepted rows]
  Threshold -->|yes| Fail[Fail run and rollback protected writes]
  Rules --> Reject[Persist row failures in rejected_sales]
  Reject --> Correct[Corrected-record API]
  Correct --> Revalidate[Validate corrected record]
  Revalidate -->|valid| Replay[Load once and mark reprocessed]
  Revalidate -->|invalid| Keep[Keep quarantined]
```

## 9. Optional Azure and Fabric

```mermaid
flowchart LR
  Local[ETL-produced Parquet] --> Adapter[StorageAdapter]
  Adapter --> LocalStore[Local adapter default]
  Adapter -. credentials and account required .-> ADLS[ADLS Gen2]
  ADF[ADF JSON templates] -. deploy to configured factory .-> ADLS
  Notebook[Fabric PySpark notebook] -. import and execute in workspace .-> Lakehouse[OneLake Delta Lakehouse]
```

The Azure SDK integration, templates, and notebook have local source/test
coverage, but no real subscription, ADF factory, or Fabric workspace has been
connected for this release.

## 10. Power BI semantic model

```mermaid
erDiagram
  DIM_CUSTOMER ||--o{ FACT_SALES : customer_key
  DIM_PRODUCT ||--o{ FACT_SALES : product_key
  DIM_DATE ||--o{ FACT_SALES : date_key
  DIM_REGION ||--o{ FACT_SALES : region_key
  DIM_SUPPLIER ||--o{ FACT_SALES : supplier_key
  DIM_PRODUCT ||--o{ FACT_INVENTORY : product_key
  DIM_DATE ||--o{ FACT_INVENTORY : date_key
  DIM_REGION ||--o{ FACT_INVENTORY : region_key
  DIM_SUPPLIER ||--o{ FACT_INVENTORY : supplier_key
  DIM_DATE ||--o{ FACT_PIPELINE_EXECUTION : started_date
  DIM_DATE ||--o{ FACT_PROCESSING_EXECUTION : created_date
  DIM_DATE ||--o{ FACT_DATA_QUALITY : checked_date
  DIM_DATE ||--o{ FACT_QUALITY_RUN : checked_date
```

Power BI Desktop, PBIX/PBIP, M/DAX evaluation, DirectQuery, and report
rendering remain unverified.

## 11. CI/CD

```mermaid
flowchart LR
  Change[Push or pull request] --> Backend[Backend + isolated PostgreSQL tests]
  Change --> Frontend[TypeScript and Next.js build]
  Change --> Data[Small fixture, ETL, dbt parse/build]
  Change --> Docker[Compose validation and image builds]
  Change --> Security[Gitleaks and dependency audits]
  Manual[Manual dispatch] --> Optional[Airflow DAG import / Spark smoke]
  Tag[Version tag] --> Release[Tests, image build, metadata artifact]
  ManualPublish[Explicit manual image-publish input] --> GHCR[GHCR; no automatic deployment]
```

GitHub Actions definitions are locally authored but have not yet executed on
GitHub for this branch.
