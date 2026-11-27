# Five-page report specification

All pages use the semantic model documented in `../semantic-model/`. Use
single-direction dimension slicers and avoid connecting fact tables. Revenue
uses the persisted Gold `revenue` field; no profit or margin visuals are
specified.

## 1. Executive Overview

- KPI cards: Total Revenue, Total Orders, Total Customers, Average Order
  Value.
- Monthly revenue line chart; drill from year to quarter, month, then day.
- Revenue by product category bar chart.
- Revenue by region bar chart/map only if the installed Power BI map visual is
  available and approved.
- Slicers: Dim Date year/month, Dim Region, Dim Product category.
- Navigation buttons to Sales and Product Analytics, Customer Analytics,
  Inventory Analytics, and Data Engineering Monitoring.

## 2. Sales and Product Analytics

- Revenue by month line chart, quantity sold by month, category revenue
  stacked column chart, and top-ten product revenue table.
- Product table: product, category, revenue, quantity sold, revenue rank.
- Regional comparison matrix with month columns.
- Slicers: date hierarchy, category, product, region, supplier.
- Drill-through from a product to product detail; use order ID only as a
  grouping field, not as the sales-line grain.

## 3. Customer Analytics

- KPI cards: observed Customer Lifetime Value, Repeat Customers, Customer
  Purchase Frequency, Revenue per Customer.
- Customer revenue ranking table with customer ID/name, region, order count,
  lifetime revenue, and Customer Segmentation.
- Customer revenue distribution and monthly customer activity visuals.
- Slicers: date, customer region, customer segment.
- Explain that lifetime revenue is limited to loaded warehouse history and
  segmentation is a relative 20/60/20 rank grouping.

## 4. Inventory Analytics

- Opening, sold, and closing unit stock by date; low-stock count uses the
  current `closing_stock <= 25` rule.
- Product/category inventory table with quantity sold, closing stock, and
  unit-based Inventory Turnover.
- Supplier and regional inventory comparisons.
- Slicers: date, category, product, supplier, region.
- Label turnover as a unit-based proxy; no inventory cost or reorder-point
  data is available.

## 5. Data Engineering Monitoring

- Pipeline Success Rate, Average Pipeline Duration Seconds, processed,
  accepted, and rejected records, Data Acceptance Rate, Data Quality Score,
  and Processing Throughput.
- Pipeline status/duration trend, acceptance/rejection trend, and quality
  score trend.
- Pandas/PySpark benchmark duration cards only when persisted benchmark rows
  exist; show no-data state otherwise.
- Detail tables: pipeline run source/state/engine/duration/counts and quality
  rule failures/severity/threshold.
- Slicers: execution date, engine, source, state, and quality rule.
- The records are database-backed execution history; do not imply that a
  benchmark ran merely because a measure exists.

## Interaction, accessibility, and layout

Use a consistent 16:9 canvas, accessible contrast, descriptive visual titles,
tooltips, keyboard-visible buttons, and page navigation. Sync only the
intended date/region/category slicers across pages. Configure drill-down on
the date and product hierarchies. Validate every visual and cross-filter in
Power BI Desktop after connecting to a real local database.
