from decimal import Decimal, InvalidOperation
import csv
from datetime import date
from pathlib import Path


def transform(path):
    with Path(path).open(newline='', encoding='utf-8') as f:
        return transform_records(list(csv.DictReader(f)))


def transform_records(records, require_order_date=False):
    accepted = []
    rejected = []
    seen = set()
    for row_number, row in enumerate(records, start=2):
        try:
            key = str(row.get("order_id") or "").strip()
            line_number = int(row.get("line_number") or 1)
            product = str(row.get("product") or "").strip()
            category = str(row.get("category") or "").strip()
            quantity = int(row["quantity"])
            price = Decimal(str(row["unit_price"]))
            order_date = str(row.get("order_date") or "").strip()
            if not key or not product or not category:
                raise ValueError("Missing required fields")
            if require_order_date and not order_date:
                raise ValueError("Missing required field: order_date")
            if order_date:
                date.fromisoformat(order_date)
            if quantity <= 0:
                raise ValueError("Quantity must be positive")
            if not price.is_finite() or price < 0:
                raise ValueError("Unit price must be non-negative")
            if line_number < 1:
                raise ValueError("Line number must be positive")
            grain = (key, line_number)
            if grain in seen:
                raise ValueError("Duplicate order line")
            seen.add(grain)
            accepted.append({
                "order_id": key,
                "line_number": line_number,
                "customer_id": str(row.get("customer_id") or "").strip(),
                "customer_name": str(row.get("customer_name") or "").strip(),
                "product_id": str(row.get("product_id") or "").strip(),
                "product": product,
                "category": category,
                "supplier_id": str(row.get("supplier_id") or "").strip(),
                "region": str(row.get("region") or "").strip(),
                "order_date": order_date,
                "quantity": quantity,
                "unit_price": price,
            })
        except (ValueError, InvalidOperation, KeyError, AttributeError, TypeError) as exc:
            rejected.append({
                "record": row,
                "reason": str(exc),
                "source_row_number": row_number,
            })
    return accepted, rejected
