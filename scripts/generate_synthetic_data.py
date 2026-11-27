import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

parser = argparse.ArgumentParser(description="Generate reproducible synthetic sales datasets.")
parser.add_argument(
    "--records",
    type=int,
    choices=(50000, 100000, 500000, 1000000),
    default=50000,
    help="Number of valid base transactions before controlled quality variants.",
)
record_count = parser.parse_args().records
random.seed(360)
start = date(2026, 1, 1)

customers = [
    (f"C{i:04d}", f"Customer {i:04d}", ["North","South","East","West","Central"][i % 5])
    for i in range(1, 501)
]
suppliers = [
    ("S001","Nova Logistics","North"),("S002","Atlas Supply","South"),
    ("S003","Pioneer Goods","East"),("S004","Meridian Trade","West"),
    ("S005","Central Source","Central"),
]
products = [
    ("P001","Laptop","Electronics",850,120,"S001"),
    ("P002","Monitor","Electronics",220,180,"S001"),
    ("P003","Keyboard","Electronics",45,300,"S002"),
    ("P004","Mouse","Electronics",30,400,"S002"),
    ("P005","Desk","Furniture",180,90,"S003"),
    ("P006","Chair","Furniture",95,140,"S003"),
    ("P007","Bookshelf","Furniture",210,60,"S004"),
    ("P008","Notebook","Office Supplies",4.5,1000,"S005"),
    ("P009","Pen","Office Supplies",1.5,2000,"S005"),
]
regions = ["North","South","East","West","Central"]

def valid_transaction(index):
    customer = customers[random.randrange(len(customers))]
    product = random.choice(products)
    month = random.randrange(12)
    transaction_date = start + timedelta(days=month * 30 + random.randrange(28))
    quantity = random.randint(1, 25)
    unit_price = product[3] * random.choice([1.0,0.95,0.90,1.05])
    order_id = f"S{index:07d}"
    return {
        "order_id": order_id,
        "customer_id": customer[0],
        "customer_name": customer[1],
        "product_id": product[0],
        "product": product[1],
        "category": product[2],
        "supplier_id": product[5],
        "region": random.choice(regions),
        "order_date": transaction_date.isoformat(),
        "quantity": quantity,
        "unit_price": round(unit_price, 2),
    }

rows = [valid_transaction(i) for i in range(1, record_count + 1)]

# Controlled quality variants are scaled with the base dataset size.
missing_count = int(record_count * 0.015)
duplicate_count = int(record_count * 0.015)
malformed_count = int(record_count * 0.01)
invalid_count = int(record_count * 0.015)
quality = []
next_id = record_count + 1
quality.extend(
    {**valid_transaction(next_id + i), "order_id": ""}
    for i in range(missing_count)
)
quality.extend({**rows[i]} for i in range(duplicate_count))
next_id += missing_count
quality.extend(
    {**valid_transaction(next_id + i), "unit_price": "not-a-price"}
    for i in range(malformed_count)
)
next_id += malformed_count
quality.extend(
    {**valid_transaction(next_id + i), "quantity": 0}
    for i in range(invalid_count)
)

all_rows = rows + quality
random.shuffle(all_rows)

with (DATA / "sales_50000.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
    writer.writeheader()
    writer.writerows(all_rows)
with (DATA / "sales_50000.json").open("w", encoding="utf-8") as f:
    json.dump(all_rows, f)

with (DATA / "customers.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["customer_id","customer_name","region"])
    writer.writerows(customers)
with (DATA / "suppliers.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["supplier_id","supplier_name","region"])
    writer.writerows(suppliers)
with (DATA / "products.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["product_id","product_name","category","unit_price","opening_stock","supplier_id"])
    writer.writerows(products)

print(json.dumps({
    "raw_transactions": len(all_rows),
    "valid_base_transactions": len(rows),
    "missing": missing_count,
    "duplicates": duplicate_count,
    "malformed": malformed_count,
    "invalid": invalid_count,
}, indent=2))
