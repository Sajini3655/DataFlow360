import csv

path = "/app/data/sales_50000.csv"
with open(path, newline="", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    fieldnames = reader.fieldnames
    rows = list(reader)

rows[0]["quantity"] = "99"
new_row = dict(rows[1])
new_row["order_id"] = "S9999999"
new_row["customer_id"] = "C9999"
new_row["customer_name"] = "Incremental Customer"
new_row["quantity"] = "7"
rows.append(new_row)

with open(path, "w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print({"updated": rows[0]["order_id"], "new": "S9999999", "raw_rows": len(rows)})
