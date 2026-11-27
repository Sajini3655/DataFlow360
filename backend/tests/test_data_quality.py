import unittest

from data_quality import (
    evaluate_rules,
    infer_schema,
    summarize_quality,
    validate_rule_config,
)


class TestDataQuality(unittest.TestCase):
    def setUp(self):
        self.records = [
            {
                "order_id": "O1",
                "line_number": "1",
                "product": "Desk",
                "category": "Home",
                "quantity": "2",
                "unit_price": "10.00",
                "order_date": "2026-03-01",
            },
            {
                "order_id": "O1",
                "line_number": "1",
                "product": "",
                "category": "Home",
                "quantity": "0",
                "unit_price": "invalid",
                "order_date": "not-a-date",
            },
        ]

    def test_quality_rules_identify_row_level_failures(self):
        rules = [
            {
                "rule_id": rule_id,
                "config": {"columns": ["order_id", "product", "order_date"]},
            }
            for rule_id in (
                "required_fields",
                "unique_business_key",
                "valid_quantity",
                "valid_unit_price",
                "valid_order_date",
            )
        ]
        failures = evaluate_rules(self.records, rules)
        self.assertEqual(failures["required_fields"], {1})
        self.assertEqual(failures["unique_business_key"], {1})
        self.assertEqual(failures["valid_quantity"], {1})
        self.assertEqual(failures["valid_unit_price"], {1})
        self.assertEqual(failures["valid_order_date"], {1})

    def test_overall_score_counts_each_bad_record_once(self):
        summary = summarize_quality(
            4,
            2,
            2,
            {"completeness": {1}, "validity": {1, 2}},
        )
        self.assertEqual(summary["overall_score"], 50.0)
        self.assertEqual(summary["acceptance_rate"], 50.0)
        self.assertEqual(summary["rejection_rate"], 50.0)

    def test_configurable_ranges_categories_and_discounts(self):
        records = [
            {"quantity": "4", "category": "Electronics", "discount": "0.1", "order_date": "2026-03-01"},
            {"quantity": "0", "category": "Unknown", "discount": "1.5", "order_date": "2027-01-01"},
        ]
        rules = [
            {"rule_id": "quantity_range", "config": {"column": "quantity", "min": 1, "max": 10}},
            {"rule_id": "accepted_values", "config": {"column": "category", "values": ["Electronics"]}},
            {"rule_id": "valid_discount", "config": {"column": "discount", "min": 0, "max": 1}},
            {"rule_id": "date_range", "config": {
                "column": "order_date",
                "start_date": "2026-01-01",
                "end_date": "2026-12-31",
            }},
        ]
        failures = evaluate_rules(records, rules)
        self.assertEqual(failures["quantity_range"], {1})
        self.assertEqual(failures["accepted_values"], {1})
        self.assertEqual(failures["valid_discount"], {1})
        self.assertEqual(failures["date_range"], {1})

    def test_invalid_rule_configuration_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "minimum cannot exceed"):
            validate_rule_config(
                "numeric_range",
                {"column": "quantity", "min": 10, "max": 1},
            )
        with self.assertRaisesRegex(ValueError, "string list"):
            validate_rule_config(
                "accepted_values",
                {"column": "category", "values": "Electronics"},
            )

    def test_schema_inference_detects_type_changes(self):
        first = infer_schema([{"quantity": "3", "order_date": "2026-03-01"}])
        changed = infer_schema([{"quantity": "3.5", "order_date": "2026-03-01"}])
        self.assertEqual(first["quantity"], ["integer"])
        self.assertEqual(changed["quantity"], ["decimal"])
