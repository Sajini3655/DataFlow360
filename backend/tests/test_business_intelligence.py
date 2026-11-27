import asyncio
import csv
import io
import os
import unittest
from decimal import Decimal


@unittest.skipUnless(os.getenv("DATABASE_URL"), "DATABASE_URL is not configured")
class TestBusinessIntelligenceApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from main import bi_datasets, bi_model, connect, export_bi_dataset

        cls.bi_datasets = staticmethod(bi_datasets)
        cls.bi_model = staticmethod(bi_model)
        cls.connect = staticmethod(connect)
        cls.export_bi_dataset = staticmethod(export_bi_dataset)

    def test_dataset_counts_and_gold_dbt_revenue_reconcile(self):
        datasets = {item["name"]: item for item in self.bi_datasets()["datasets"]}
        self.assertIn("fact_sales", datasets)
        self.assertGreater(datasets["fact_sales"]["row_count"], 0)

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) AS row_count,coalesce(sum(revenue),0) AS revenue "
                    "FROM gold_sales_fact"
                )
                gold = cursor.fetchone()

        self.assertEqual(datasets["fact_sales"]["row_count"], gold["row_count"])
        self.assertEqual(Decimal(datasets["fact_sales"]["revenue_total"]), gold["revenue"])
        if "dbt_fct_sales_line" in datasets:
            with self.connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) AS row_count,coalesce(sum(revenue),0) AS revenue "
                        "FROM analytics.fct_sales_line"
                    )
                    dbt_sales = cursor.fetchone()
            self.assertEqual(datasets["dbt_fct_sales_line"]["row_count"], dbt_sales["row_count"])
            self.assertEqual(Decimal(datasets["dbt_fct_sales_line"]["revenue_total"]), dbt_sales["revenue"])
            self.assertEqual(gold["revenue"], dbt_sales["revenue"])

    def test_csv_export_has_view_columns_and_exact_row_count(self):
        response = self.export_bi_dataset("dim_region")
        self.assertIn("text/csv", response.headers["content-type"])
        self.assertIn("attachment;", response.headers["content-disposition"])

        async def read_csv():
            return "".join([chunk async for chunk in response.body_iterator])

        rows = list(csv.reader(io.StringIO(asyncio.run(read_csv()))))
        self.assertEqual(rows[0], ["region_key", "region_name"])
        self.assertEqual(len(rows) - 1, len({row[0] for row in rows[1:]}))
        datasets = self.bi_datasets()["datasets"]
        counts = {item["name"]: item["row_count"] for item in datasets}
        self.assertEqual(len(rows) - 1, counts["dim_region"])

    def test_unknown_csv_export_is_not_executed(self):
        from fastapi import HTTPException

        with self.assertRaises(HTTPException) as error:
            self.export_bi_dataset("unknown")
        self.assertEqual(error.exception.status_code, 404)

    def test_semantic_model_endpoint_describes_order_line_grain(self):
        body = self.bi_model()
        self.assertIn("order_id and line_number", body["grain"]["fact_sales"])
        self.assertIn("Single direction", body["filter_direction"])
