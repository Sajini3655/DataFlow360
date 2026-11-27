# Sample exports

No database rows are checked into this directory. The warehouse is generated
and environment-specific, so committing a snapshot could expose data or become
stale.

Download current CSV exports from the Business Intelligence dashboard or
`GET /bi/exports/{dataset}.csv`. These downloads include actual reporting-view
rows and can be used as Power BI file sources with `fnReadCsvExport.pq`.

The migration and `/bi/datasets` endpoint provide the authoritative schemas
and row counts.
