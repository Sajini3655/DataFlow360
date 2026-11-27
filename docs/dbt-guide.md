# dbt analytics engineering

The dbt project is an optional analytics-engineering layer over the existing
PostgreSQL warehouse. It reads `public.silver_sales` and the existing dimension
and inventory tables; these remain the system of record. dbt models live in the
`analytics` schema and do not replace the FastAPI warehouse or its dashboard
queries.

## Configuration and commands

From the repository root, copy `.env.example` to `.env` and set local values.
The Compose profile passes the database settings to dbt as environment
variables. `dbt/profiles.yml.example` contains only environment references;
the ignored local `dbt/profiles.yml` is not used by the container.

```powershell
docker compose --profile dbt build dbt
docker compose --profile dbt run --rm dbt debug
docker compose --profile dbt run --rm dbt build
docker compose --profile dbt run --rm dbt build --full-refresh
docker compose --profile dbt run --rm dbt docs generate
docker compose --profile dbt run --rm --service-ports dbt docs serve --host 0.0.0.0 --port 8080
```

The docs server is available at http://localhost:8081 while its command is
running. `dbt build` executes models and data tests; incremental line facts use
`(order_id, line_number)` as their business key and include changed Silver rows
based on `transformed_at`. Daily and monthly outputs aggregate this line-grain
fact.

## Model layout

- `models/staging/`: standardized projections of warehouse sales, dimensions,
  and inventory.
- `models/intermediate/`: conformed sales joins, customer history, category
  performance, and inventory movement calculations.
- `models/marts/facts/`: incremental sales-line mart.
- `models/marts/dimensions/`: customer lifetime value and segmentation.
- `models/marts/analytics/`: daily/monthly sales, product/category/region
  performance, inventory turnover, and low-stock products.
- `seeds/`: editable customer lifetime-value segment thresholds.
- `snapshots/`: customer attribute history.
- `tests/`: positive-value and source-to-mart revenue reconciliation checks.
- `macros/`: composite-key uniqueness test and custom schema naming.

dbt artifacts and packages are generated under ignored `dbt/target/`,
`dbt/logs/`, and `dbt/dbt_packages/` directories. The dbt 1.8.8 project parses
with the PostgreSQL adapter 1.8.2. Connection checks, model execution, tests,
incremental reruns, and docs generation have not been verified against the
current database; run the commands above after configuring `.env`.
