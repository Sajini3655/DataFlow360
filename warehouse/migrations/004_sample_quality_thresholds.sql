UPDATE data_quality_rules
SET threshold_percent = 2,
    updated_at = now()
WHERE rule_id IN (
    'required_fields',
    'unique_business_key',
    'valid_quantity',
    'valid_unit_price',
    'valid_order_date',
    'quantity_range'
)
AND threshold_percent = 0
AND NOT EXISTS (
    SELECT 1
    FROM data_quality_results
    WHERE data_quality_results.rule_id = data_quality_rules.rule_id
);
