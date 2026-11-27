# DataFlow360 Power BI assets

These are source assets and assembly instructions for Power BI Desktop, not a
rendered report or a fabricated PBIX/PBIP project. Power BI Desktop was not
found in the checked Windows installation paths, so Desktop refresh, M
evaluation, DAX evaluation, and report rendering have not been verified.

The PostgreSQL Gold warehouse is the source of truth. Connect to the `bi`
schema for conformed reporting views. Optional dbt marts in the `analytics`
schema are available as separate sources; do not append or relate duplicate
sales facts from both schemas.

## Contents

- `power-query/`: 11 data-source queries, a sales-line duplicate audit, 2
  transformation examples, and 2 helper functions for PostgreSQL and CSV
  sources.
- `dax/`: 35 measures organized by business subject.
- `semantic-model/`: relationship, grain, key, and data-dictionary reference.
- `reports/`: page-by-page visual, slicer, and navigation specifications.
- `themes/dataflow360-theme.json`: importable Power BI JSON theme.
- `sample-exports/README.md`: how to download real CSV exports without
  committing database contents.

## Connect to PostgreSQL

1. Start the local DataFlow360 stack and confirm `GET /bi/datasets` responds.
2. In Power BI Desktop choose **Get data > PostgreSQL database**.
3. Use server `localhost:5432` and database `dataflow` (or the values in the
   local `.env`). Authenticate with the PostgreSQL credentials configured for
   the local project; never save them in this repository.
4. Choose **Import** for the recommended initial model. DirectQuery is
   supported by the PostgreSQL connector for compatible models, but has not
   been performance-tested here.
5. Select the `bi` views and load the tables listed in
   `semantic-model/star-schema.md`. Optionally load selected `analytics`
   dbt marts for specific pre-aggregated analyses.
6. Create the single-direction, one-to-many relationships in that model
   reference. Mark `Dim Date[full_date]` as the date table.
7. Create `RangeStart` and `RangeEnd` Date/Time parameters before applying
   incremental refresh to `SalesTransactions.pq`. Verify query folding in
   Desktop before enabling a production refresh policy.
8. Import the definitions in `dax/` as measures, set the documented formats,
   and assemble the five pages from `reports/report-pages.md`.
9. Import `themes/dataflow360-theme.json` using **View > Themes > Browse for
   themes**.

The connector, server version, network access, and PostgreSQL account
permissions determine whether a given query folds or supports DirectQuery.
Check the Power Query folding indicator and query plan in Desktop rather than
assuming every M step folds.

## CSV alternative

The Next.js Business Intelligence page links to CSV downloads from the live
backend at `/bi/exports/{dataset}.csv`. These are generated from the same
reporting views. For a local file-based Power BI source, download the required
dataset, set `pExportFolder`, and use `fnReadCsvExport.pq`. CSV refresh is a
file refresh, not a live PostgreSQL query.

No Power BI Desktop validation, Power BI Service publication, credential
configuration, or cloud refresh has been performed.
