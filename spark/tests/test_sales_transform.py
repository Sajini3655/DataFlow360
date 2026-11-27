import csv
import tempfile
import unittest
from pathlib import Path

from spark.sales_transform import create_spark_session, run_benchmark


class TestSparkSalesTransform(unittest.TestCase):
    def test_pandas_and_spark_match_for_lines_duplicates_and_invalid_rows(self):
        rows = [
            ["order_id", "line_number", "customer_id", "customer_name", "product_id",
             "product", "category", "supplier_id", "region", "order_date", "quantity", "unit_price"],
            ["O-1", "1", "C-1", "Customer", "P-1", "Desk", "Home", "S-1", "North", "2026-01-03", "1", "100.00"],
            ["O-1", "2", "C-1", "Customer", "P-2", "Chair", "Home", "S-1", "North", "2026-01-03", "2", "25.00"],
            ["O-1", "2", "C-1", "Customer", "P-2", "Chair", "Home", "S-1", "North", "2026-01-03", "2", "25.00"],
            ["O-2", "1", "C-2", "Customer 2", "P-3", "Pen", "Office", "S-2", "South", "2026-01-04", "0", "2.00"],
            ["O-3", "1", "C-3", "Customer 3", "P-4", "Book", "Office", "S-2", "South", "2026-01-05", "1", "bad"],
        ]
        spark = create_spark_session("local[2]")
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                input_path = root / "sales.csv"
                with input_path.open("w", newline="", encoding="utf-8") as file:
                    csv.writer(file).writerows(rows)
                result = run_benchmark(spark, input_path, root / "results", "unit-test")
            self.assertTrue(result["equivalent"])
            self.assertEqual(result["pandas"]["input_rows"], 5)
            self.assertEqual(result["pandas"]["accepted_rows"], 2)
            self.assertEqual(result["pandas"]["rejected_rows"], 3)
            self.assertEqual(result["pandas"]["revenue"], result["pyspark"]["revenue"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
