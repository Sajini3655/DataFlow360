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


def load_silver_rows(cursor, rows):
    if not rows:
        return 0

    cursor.executemany(
        "INSERT INTO silver_sales(order_id,line_number,customer_id,customer_name,product_id,"
        "product,category,supplier_id,region,order_date,quantity,unit_price) "
        "VALUES (%(order_id)s,%(line_number)s,%(customer_id)s,%(customer_name)s,%(product_id)s,"
        "%(product)s,%(category)s,%(supplier_id)s,%(region)s,"
        "NULLIF(%(order_date)s,'')::date,%(quantity)s,%(unit_price)s) "
        "ON CONFLICT(order_id,line_number) DO UPDATE SET "
        "customer_id=EXCLUDED.customer_id,customer_name=EXCLUDED.customer_name,"
        "product_id=EXCLUDED.product_id,product=EXCLUDED.product,"
        "category=EXCLUDED.category,supplier_id=EXCLUDED.supplier_id,"
        "region=EXCLUDED.region,order_date=EXCLUDED.order_date,"
        "quantity=EXCLUDED.quantity,unit_price=EXCLUDED.unit_price,"
        "transformed_at=now()",
        rows,
    )
    cursor.execute(
        "INSERT INTO dim_customer(customer_id,customer_name,region) "
        "SELECT DISTINCT ON (coalesce(customer_id,'UNKNOWN')) "
        "coalesce(customer_id,'UNKNOWN'),coalesce(customer_name,'Unknown'),"
        "coalesce(region,'Unknown') FROM silver_sales "
        "ON CONFLICT(customer_id) DO UPDATE SET "
        "customer_name=EXCLUDED.customer_name,region=EXCLUDED.region"
    )
    cursor.execute(
        "INSERT INTO dim_category(category_name) "
        "SELECT DISTINCT category FROM silver_sales "
        "ON CONFLICT(category_name) DO UPDATE SET category_name=EXCLUDED.category_name"
    )
    cursor.execute(
        "INSERT INTO dim_product(product_name,category_key) "
        "SELECT DISTINCT ON (s.product,c.category_key) s.product,c.category_key "
        "FROM silver_sales s JOIN dim_category c ON c.category_name=s.category "
        "ON CONFLICT(product_name,category_key) "
        "DO UPDATE SET category_key=EXCLUDED.category_key"
    )
    cursor.execute(
        "INSERT INTO dim_region(region_name) "
        "SELECT DISTINCT coalesce(region,'Unknown') FROM silver_sales "
        "ON CONFLICT(region_name) DO UPDATE SET region_name=EXCLUDED.region_name"
    )
    cursor.execute(
        "INSERT INTO dim_supplier(supplier_id,supplier_name,region) "
        "SELECT DISTINCT ON (coalesce(supplier_id,'UNKNOWN')) "
        "coalesce(supplier_id,'UNKNOWN'),"
        "'Supplier '||coalesce(supplier_id,'UNKNOWN'),coalesce(region,'Unknown') "
        "FROM silver_sales ON CONFLICT(supplier_id) DO UPDATE SET "
        "supplier_name=EXCLUDED.supplier_name,region=EXCLUDED.region"
    )
    cursor.execute(
        "INSERT INTO dim_date(date_key,full_date,year,month,month_name,quarter) "
        "SELECT distinct (extract(year from order_date)::text||"
        "lpad(extract(month from order_date)::text,2,'0')||"
        "lpad(extract(day from order_date)::text,2,'0'))::integer,"
        "order_date,extract(year from order_date)::integer,"
        "extract(month from order_date)::integer,to_char(order_date,'FMMonth'),"
        "extract(quarter from order_date)::integer "
        "FROM silver_sales WHERE order_date IS NOT NULL "
        "ON CONFLICT(date_key) DO UPDATE SET full_date=EXCLUDED.full_date,"
        "year=EXCLUDED.year,month=EXCLUDED.month,month_name=EXCLUDED.month_name,"
        "quarter=EXCLUDED.quarter"
    )
    upsert_gold_facts(cursor)
    cursor.execute(
        "INSERT INTO inventory_fact(product_key,supplier_key,region_key,date_key,"
        "opening_stock,quantity_sold,closing_stock) "
        "SELECT p.product_key,max(s.supplier_key),max(r.region_key),d.date_key,"
        "1000,coalesce(sum(g.quantity),0),"
        "GREATEST(1000-coalesce(sum(g.quantity),0),0) "
        "FROM gold_sales_fact g "
        "JOIN dim_product p ON p.product_key=g.product_key "
        "JOIN dim_supplier s ON s.supplier_key=g.supplier_key "
        "JOIN dim_region r ON r.region_key=g.region_key "
        "JOIN dim_date d ON d.date_key=g.date_key "
        "GROUP BY p.product_key,d.date_key "
        "ON CONFLICT(product_key,date_key) DO UPDATE SET "
        "supplier_key=EXCLUDED.supplier_key,region_key=EXCLUDED.region_key,"
        "opening_stock=EXCLUDED.opening_stock,quantity_sold=EXCLUDED.quantity_sold,"
        "closing_stock=EXCLUDED.closing_stock"
    )
    return len(rows)
