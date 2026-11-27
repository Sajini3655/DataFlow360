# DataFlow360 interview guide

Use these as project-grounded answer notes, not claims about personal
employment or experience. The benchmark values are from a single local
50,000-record comparison and are not universal performance claims.

## Project and architecture

1. **What problem does the project address?**

   It demonstrates how raw retail sales can be validated, transformed,
   warehoused, monitored, and exposed to analytics through one repeatable flow.

2. **Why use separate frontend, API, and database services?**

   Each has a clear responsibility: interaction, orchestration/query logic,
   and durable relational storage.

3. **What is the source of truth?**

   PostgreSQL, especially the normalized Silver layer and Gold warehouse facts.

4. **How does the frontend get metrics?**

   It calls FastAPI endpoints that query PostgreSQL; the BI page also
   downloads streamed CSVs from reporting views.

5. **How are services run locally?**

   Docker Compose runs PostgreSQL, FastAPI, and Airflow; dbt and Spark are
   opt-in profiles. Next.js runs with npm locally.

## ETL, medallion, and quality

6. **What is ETL versus ELT here?**

   The primary pipeline transforms and validates before loading Silver/Gold
   (ETL-like). dbt then performs analytical SQL transformations in PostgreSQL
   (ELT).

7. **What does Bronze store?**

   Persisted source payloads and source row identifiers for traceability.

8. **What does Silver represent?**

   Validated and normalized sales lines with the `(order_id, line_number)`
   grain.

9. **What does Gold represent?**

   Sales and inventory facts joined to customer, product, date, region, and
   supplier dimensions.

10. **How are multiple lines for one order handled?**

    Each line number is its own legitimate record. Duplicate prevention uses
    the composite order/line key, not order ID alone.

11. **How is incremental processing made idempotent?**

    Source hashes/watermarks avoid reprocessing unchanged inputs, and
    warehouse upserts use stable business keys.

12. **What happens to invalid input?**

    Validation rejects invalid rows into a quarantine table with reason and
    source metadata; corrected rows are validated before reprocessing.

13. **How is data quality measured?**

    Configurable rules evaluate missing fields, key uniqueness, value ranges,
    dates, referential integrity, and revenue consistency. Per-rule outcomes
    and run summaries are persisted.

14. **What is schema drift?**

    The pipeline compares observed field/type signatures with stored source
    snapshots and records detected changes for review.

15. **How are quality thresholds used?**

    Rule failures are compared with configured threshold percentages; a
    threshold breach can fail the run rather than publishing a success result.

## PostgreSQL, dbt, and analytics

16. **Why use a star schema?**

    It gives analytical queries a clear fact grain and reusable descriptive
    dimensions.

17. **What is the sales fact grain?**

    One row per `(order_id, line_number)`.

18. **How is revenue calculated?**

    It is quantity multiplied by unit price; the schema generates revenue.
    There are no cost or margin columns.

19. **What does dbt add?**

    Versioned SQL models, dependency lineage, incremental sales marts, and
    tests in the analytics schema.

20. **How does the incremental dbt sales fact work?**

    It uses order ID plus line number as the unique key and filters by source
    update timestamp; it is configured for delete-and-insert behavior.

21. **What SQL features support analytics?**

    The project uses joins, aggregates, date truncation, window functions,
    conflict handling, generated columns, and filtered views.

22. **What is the purpose of Parquet?**

    It provides persisted columnar snapshots for local analytical/transfer
    workflows; current ETL outputs are full-table rewrites.

## Airflow, Pandas, and Spark

23. **What does Airflow do?**

    It triggers and schedules the pipeline, and exposes DAG/task execution
    states that the API and frontend poll.

24. **How does Airflow call the backend?**

    A DAG task sends an internal token with the request to `/pipeline/execute`.

25. **Are ETL stages separate Airflow tasks?**

    No. There is one main ETL task and an optional dbt task; stage-level
    details are persisted in PostgreSQL.

26. **Why compare Pandas and Spark?**

    The project checks that both implementations produce equivalent results
    for representative rows, duplicates, and invalid input.

27. **What did the local 50k benchmark show?**

    Pandas processed 52,750 total rows in 5.71 seconds (9,236 rows/s); PySpark
    took 91.63 seconds (576 rows/s). The local PySpark run was slower.

28. **Does that prove Pandas is always faster?**

    No. It is an environment-specific small-scale result. Spark startup and
    distributed overhead may matter more than parallelism at that size.

29. **How is inventory turnover calculated?**

    The BI measure uses units sold divided by average opening/closing unit
    stock; it is not accounting turnover by cost.

## Cloud, BI, and operations

30. **What is implemented for Azure?**

    An optional ADLS adapter, environment-based configuration, ADF templates,
    and tests. No real subscription execution is claimed.

31. **What is implemented for Fabric?**

    A notebook asset demonstrating OneLake/Lakehouse Delta processing.
    Execution in a real Fabric workspace is unverified.

32. **What is the Power BI deliverable?**

    PostgreSQL views, CSV endpoints, M scripts, DAX definitions, a semantic
    model guide, report specifications, and a theme—not a rendered report.

33. **Was Power BI Desktop tested?**

    No. Desktop, PBIX/PBIP creation, M/DAX execution, DirectQuery, refresh, and
    report rendering remain unverified.

34. **What does the CI design cover?**

    Isolated PostgreSQL backend tests, frontend type/build checks,
    data-engineering/dbt checks, Docker builds/smoke tests, and dependency and
    secret scans.

35. **Have those workflows passed on GitHub?**

    Not yet for this branch. Their definitions are present but remote execution
    must be observed after publishing.

36. **What remains before production?**

    End-user authentication/authorization, TLS and proxy configuration,
    least-privilege containers, tested backups/restores, monitoring, rate
    limiting, and resolution of the Airflow webserver limitation.

37. **Why keep Azure and Fabric optional?**

    Local development must remain usable without cloud credentials or paid
    resources; real cloud connection requires account setup and permissions.

38. **What does idempotency protect against?**

    Repeated source runs should not duplicate the same sales line, while
    changed values for that key can update the warehouse fact.

39. **Why not report profit?**

    The available schema has price and quantity but no reliable cost field.
    Profit would therefore be invented.

40. **How would you explain the current status to a stakeholder?**

    It is a locally verified portfolio data-engineering application with
    optional integration assets; external cloud, Power BI, and production
    deployment outcomes have not been verified.
