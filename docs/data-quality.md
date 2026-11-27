# Data quality, quarantine, and lineage

Data quality rules are stored in PostgreSQL and can be enabled, disabled, or
given a failure-rate threshold from the Data Quality page. Each result records
the source, pipeline run, dataset, layer, rule, checked count, failure count,
failure rate, and threshold. A row failing multiple rules is counted once in
the overall row score; rule-level failure counts remain separate.

The current framework checks required values, the `(order_id, line_number)`
business key, positive integer quantities, non-negative numeric prices, ISO
dates, dimension references, and revenue consistency. Schema snapshots record
missing columns, unexpected columns, and observed type changes. Rejected rows
retain their original JSON, source row number, reason, and run reference.
Quarantined records can be filtered, exported as CSV, corrected, and
reprocessed without changing the original record. Rule JSON configuration is
editable alongside each rule's enabled state and failure-rate threshold. The
supported rule types include numeric ranges, accepted values, optional
discount ranges, and configurable date ranges.

## Threshold behavior

The generated development dataset deliberately contains approximately 1.5%
missing order IDs, 1.5% duplicate rows, 1% malformed prices, and 1.5% invalid
quantities. The corresponding base quality rules default to a 2% failure-rate
threshold so this fixture can demonstrate quarantine while allowing the valid
subset to proceed. Referential-integrity and revenue-reconciliation rules
remain strict. Thresholds should be reviewed for each real source rather than
copied as a production policy.

If an enabled rule exceeds its threshold, the pipeline records the failed
execution and quality summary, retains Bronze and quarantine audit rows,
rolls back Silver, dimensions, Gold, and inventory changes, and does not
advance the ingestion watermark or publish new Parquet snapshots. Configure
the rule threshold deliberately before retrying.

The quality score formula is `100 * (checked rows - distinct rows with at
least one failed rule) / checked rows`. Acceptance and rejection rates are
computed independently from accepted and quarantined counts. Rule failure rate
is `100 * failing rows for that rule / checked rows`.

Lineage events associate successful pipeline runs with source, Bronze, Silver,
and Gold datasets. A failed quality gate records only the committed source to
Bronze and Bronze to quarantine edges; its stage error explains why no
successful downstream publication occurred.

The UI and APIs are implemented, but database-backed rule behavior, quality
threshold rollback, and quarantine recovery have not been integration-tested
in the current workspace. See the final verification report for the current
test status.
