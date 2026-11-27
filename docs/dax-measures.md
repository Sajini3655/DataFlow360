# DAX measures

The definitions are in `../powerbi/dax/` and contain 35 measures across sales,
time intelligence, customer, product/inventory, and data engineering topics.
They use only fields supplied by the existing Gold warehouse and persisted
execution/quality tables.

Use the Power BI display names in the semantic-model guide. Format revenue,
average order value, and customer revenue as currency; growth and acceptance/
success rates as percentages; counts as whole numbers; durations in seconds;
and inventory turnover as a decimal ratio. Quality scores are stored on a
0–100 scale.

Important semantics:

- Orders are distinct `order_id` values, while sales lines are uniquely
  identified by `(order_id, line_number)`.
- Customer lifetime value is observed lifetime revenue in the loaded data;
  it does not predict future value.
- Customer segmentation dynamically ranks customers into top 20%, middle
  60%, and bottom 20% within the current selection.
- Inventory turnover is a unit-based ratio using the available stock
  snapshots. It is not a cost-based accounting measure.
- Low stock uses `closing_stock <= 25`, matching the current dbt low-stock
  model; the warehouse has no supplier reorder-level column.
- Profit, margin, cost, and inventory valuation measures are intentionally
  absent because their required fields do not exist.

The DAX has been reviewed against the current view column names but cannot be
executed or type-checked without Power BI Desktop or another DAX engine.
