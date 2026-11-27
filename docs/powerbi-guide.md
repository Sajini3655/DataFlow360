# Power BI Desktop guide

## Requirements and credentials

Install Power BI Desktop on Windows and use the PostgreSQL connector included
with the current Desktop release. Start the local DataFlow360 PostgreSQL
service and backend. The backend BI API is available at
`http://localhost:8000/bi/datasets`. Use the host and database values from
`.env`; keep the password in the Power BI credential store, never in M source
or this repository.

The local database role must be able to connect to the configured database
and select the `bi` reporting views (and `analytics` schema if using dbt
marts). No paid Azure, Fabric, or Power BI Service resource is required for
Desktop against local PostgreSQL.

## Connect and refresh

1. Choose **Get data > PostgreSQL database** in Power BI Desktop.
2. Set server `localhost:5432` and database `dataflow`, adjusting for `.env`.
3. Choose Import mode to begin. DirectQuery is supported by the connector for
   compatible views, but it has not been tested or performance-tuned here.
4. Load the `bi` dimensions/facts listed in
   `../powerbi/semantic-model/star-schema.md`. The optional dbt models appear
   in `analytics` only after they have been built.
5. Create the documented single-direction relationships, mark `Dim Date` as
   the date table, and import the DAX measures from `../powerbi/dax/`.
6. Build report pages from `../powerbi/reports/report-pages.md` and import the
   JSON theme in `../powerbi/themes/dataflow360-theme.json`.
7. Use **Refresh** in Desktop. For scheduled refresh, publish to Power BI
   Service only after separately configuring a supported gateway and
   credentials; service publication is outside this phase.

## CSV fallback

Use the dashboard download controls or API endpoint
`http://localhost:8000/bi/exports/fact_sales.csv` (replace dataset name as
needed). In Desktop choose **Get data > Text/CSV**. The download is a point-in-
time export; it does not refresh from PostgreSQL automatically.

## Validation and limitations

Power BI Desktop was not found in the checked installation paths in the
development environment. Thus report rendering, connector login, actual
refresh, DirectQuery, and Power BI Service gateway behavior remain
unverified. Do not treat the Next.js Business Intelligence page as an
embedded Power BI report.
