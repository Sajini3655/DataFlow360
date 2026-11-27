# Power Query M assets

The reusable M assets are in `../powerbi/power-query/`. They cover the sales
line fact, five dimensions, inventory, pipeline executions, data-quality
results, quality-run summaries, an optional dbt monthly-sales model, and a
sales-line duplicate audit. `fnGetBiView` is the shared PostgreSQL connector
helper; `fnReadCsvExport` supports live CSV downloads saved to disk.

## Parameters and loading

Create Power Query parameters named `pServer` (text, for example
`localhost:5432`), `pDatabase` (text, for example `dataflow`),
`pExportFolder` (text, local folder), `pCsvSalesFile` (text, a CSV file name),
`RangeStart` (Date/Time), and `RangeEnd` (Date/Time). Import
`fnGetBiView.pq` as a function named `fnGetBiView`, and the CSV helper as
`fnReadCsvExport`. Import the source queries with names matching their file
names so the merge/append examples can reference them.

`SalesTransactions.pq` filters by the half-open interval
`[RangeStart, RangeEnd)` and removes duplicate rows only by the actual
`(order_id, line_number)` sales-line key. Repeated order IDs with distinct
line numbers remain legitimate sales lines. The append example is for
non-overlapping source partitions; do not append a full CSV export of the same
warehouse rows to the PostgreSQL source.

The M sources demonstrate typed columns, required-key filters, missing-value
handling, duplicate detection at the correct grain, a dimension merge,
append of partitioned sources, and aggregation. PostgreSQL view navigation
and straightforward date filters may fold to SQL; transformations are
connector- and version-dependent. Query folding has not been verified in
Power BI Desktop. Inspect the folding indicator or query plan after each
transformation before relying on incremental refresh.

Power BI incremental refresh requires the exact, case-sensitive `RangeStart`
and `RangeEnd` parameters, a date filter using them, and a policy configured
in Desktop. The provided M is a starting point, not evidence that a refresh
policy has been deployed.
