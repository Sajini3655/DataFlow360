# Semantic model and data dictionary

The Power BI model definition, table grains, relationships, and full reporting
view dictionary are maintained in
[`../powerbi/semantic-model/star-schema.md`](../powerbi/semantic-model/star-schema.md)
and
[`../powerbi/semantic-model/data-dictionary.md`](../powerbi/semantic-model/data-dictionary.md).

Use PostgreSQL views in the `bi` schema for the conformed model. The sales
fact grain is an order line, `(order_id, line_number)`, and its revenue is
generated from quantity and unit price. Inventory is one row per product/date.
Pipeline, processing, quality-rule, and quality-run facts retain their
respective persisted execution grains.

Relationships are one-to-many and single-direction from dimensions to facts.
Date relationships use the date-only fields exposed by the reporting views.
Do not relate fact tables to one another or enable bidirectional filtering.
Category remains a product attribute to avoid an unnecessary snowflake.

Power BI Desktop was not available for interactive model validation; the
relationship definitions are a manual assembly specification, not a claimed
rendered PBIP project.
