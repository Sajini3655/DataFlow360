# Business Intelligence phase

Phase 13 adds database-backed PostgreSQL reporting views, CSV export API
routes, reusable Power Query and DAX assets, a semantic-model reference, five
Power BI report-page designs, a JSON theme, and the Next.js Business
Intelligence page.

The warehouse remains the primary source of truth. Optional dbt marts are
listed when they exist in `analytics`; they are not unioned with the Gold fact
because that would duplicate sales. The frontend page reads real dataset
counts, revenue totals, timestamps, and model metadata from `/bi/datasets`
and `/bi/model`.

Power BI Desktop, Power Query, DAX execution, report rendering, DirectQuery,
Power BI Service, and gateway refresh were not available for verification.
Follow the manual assembly steps in [`powerbi-guide.md`](./powerbi-guide.md).
The dashboard is not an embedded Power BI report.

No Power BI PBIX/PBIP file or database snapshot is included. CSV exports are
generated from the current database only when requested.
