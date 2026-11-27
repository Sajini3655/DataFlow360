import os
import unittest
import uuid

import psycopg
from psycopg.rows import dict_row

from warehouse_loader import upsert_gold_facts


@unittest.skipUnless(os.getenv("DATABASE_URL"), "DATABASE_URL is not configured")
class TestGoldUpsertIntegration(unittest.TestCase):
    def test_update_propagates_without_duplicate_fact(self):
        order_id = f"REG-{uuid.uuid4().hex[:12]}"
        with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SAVEPOINT regression_case")
                try:
                    cursor.execute(
                        "INSERT INTO dim_customer(customer_id,customer_name,region) "
                        "VALUES (%s,%s,%s) ON CONFLICT(customer_id) DO UPDATE "
                        "SET customer_name=EXCLUDED.customer_name RETURNING customer_key",
                        (f"C-{order_id}", "Regression Customer", "North"),
                    )
                    customer_key = cursor.fetchone()["customer_key"]
                    cursor.execute(
                        "INSERT INTO dim_category(category_name) VALUES ('Regression') "
                        "ON CONFLICT(category_name) DO UPDATE SET category_name=EXCLUDED.category_name "
                        "RETURNING category_key"
                    )
                    category_key = cursor.fetchone()["category_key"]
                    cursor.execute(
                        "INSERT INTO dim_product(product_name,category_key) VALUES (%s,%s) "
                        "ON CONFLICT(product_name,category_key) DO UPDATE "
                        "SET category_key=EXCLUDED.category_key RETURNING product_key",
                        (f"P-{order_id}", category_key),
                    )
                    product_key = cursor.fetchone()["product_key"]
                    cursor.execute(
                        "INSERT INTO dim_region(region_name) VALUES ('North') "
                        "ON CONFLICT(region_name) DO UPDATE SET region_name=EXCLUDED.region_name "
                        "RETURNING region_key"
                    )
                    region_key = cursor.fetchone()["region_key"]
                    cursor.execute(
                        "INSERT INTO dim_supplier(supplier_id,supplier_name,region) "
                        "VALUES (%s,%s,'North') ON CONFLICT(supplier_id) DO UPDATE "
                        "SET supplier_name=EXCLUDED.supplier_name RETURNING supplier_key",
                        (f"S-{order_id}", "Regression Supplier"),
                    )
                    supplier_key = cursor.fetchone()["supplier_key"]
                    cursor.execute(
                        "INSERT INTO dim_date(date_key,full_date,year,month,month_name,quarter) "
                        "VALUES (20991231,'2099-12-31',2099,12,'December',4) "
                        "ON CONFLICT(date_key) DO UPDATE SET full_date=EXCLUDED.full_date"
                    )
                    cursor.execute(
                        "INSERT INTO silver_sales(order_id,line_number,customer_id,customer_name,"
                        "product_id,product,category,supplier_id,region,order_date,quantity,unit_price) "
                        "VALUES (%s,1,%s,'Regression Customer',%s,%s,'Regression',%s,'North',"
                        "'2099-12-31',2,10)",
                        (order_id, f"C-{order_id}", f"P-{order_id}", f"P-{order_id}", f"S-{order_id}"),
                    )
                    upsert_gold_facts(cursor, order_id)
                    cursor.execute(
                        "UPDATE silver_sales SET quantity=7,unit_price=15 "
                        "WHERE order_id=%s AND line_number=1",
                        (order_id,),
                    )
                    upsert_gold_facts(cursor, order_id)
                    cursor.execute(
                        "SELECT count(*) AS rows,sum(quantity) AS units,sum(revenue) AS revenue "
                        "FROM gold_sales_fact WHERE order_id=%s",
                        (order_id,),
                    )
                    result = cursor.fetchone()
                    self.assertEqual(result["rows"], 1)
                    self.assertEqual(result["units"], 7)
                    self.assertEqual(float(result["revenue"]), 105.0)
                finally:
                    cursor.execute("ROLLBACK TO SAVEPOINT regression_case")
                    cursor.execute("RELEASE SAVEPOINT regression_case")
