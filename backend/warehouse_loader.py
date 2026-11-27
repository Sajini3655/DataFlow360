GOLD_UPSERT_SQL = """
INSERT INTO gold_sales_fact(
    order_id, customer_key, product_key, date_key, region_key, supplier_key,
    quantity, unit_price, line_number
)
SELECT s.order_id, c.customer_key, p.product_key, d.date_key,
       r.region_key, su.supplier_key, s.quantity, s.unit_price, s.line_number
FROM silver_sales s
JOIN dim_customer c ON c.customer_id = s.customer_id
JOIN dim_category category ON category.category_name = s.category
JOIN dim_product p
  ON p.product_name = s.product AND p.category_key = category.category_key
JOIN dim_date d ON d.full_date = s.order_date
JOIN dim_region r ON r.region_name = s.region
JOIN dim_supplier su ON su.supplier_id = s.supplier_id
ON CONFLICT(order_id, line_number) DO UPDATE SET
    customer_key = EXCLUDED.customer_key,
    product_key = EXCLUDED.product_key,
    date_key = EXCLUDED.date_key,
    region_key = EXCLUDED.region_key,
    supplier_key = EXCLUDED.supplier_key,
    quantity = EXCLUDED.quantity,
    unit_price = EXCLUDED.unit_price,
    updated_at = now()
"""


def upsert_gold_facts(cursor, order_id=None):
    if order_id is None:
        cursor.execute(GOLD_UPSERT_SQL)
    else:
        cursor.execute(
            GOLD_UPSERT_SQL.replace(
                "FROM silver_sales s",
                "FROM (SELECT * FROM silver_sales WHERE order_id = %s) s",
            ),
            (order_id,),
        )
