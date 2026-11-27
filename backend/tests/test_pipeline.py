import unittest
from pathlib import Path
from pipeline import transform, transform_records
class TestPipeline(unittest.TestCase):
    def test_sample(self):
        rows, rejected = transform(Path(__file__).resolve().parents[2]/'data'/'sales.csv')
        self.assertEqual(len(rows),8)
        self.assertEqual(len(rejected),2)
        self.assertEqual(len({r['order_id'] for r in rows}),8)
        self.assertEqual(sum(r['quantity']*r['unit_price'] for r in rows),8135)

    def test_duplicate_non_positive_and_missing_fields_are_rejected(self):
        import tempfile
        csv_text = (
            'order_id,product,category,quantity,unit_price\n'
            'A1,Widget,Office,1,2.50\n'
            'A1,Widget,Office,2,3.00\n'
            'A2,,Office,1,1.00\n'
            'A3,Gadget,Office,0,1.00\n'
            'A4,Gadget,Office,1,-1\n'
        )
        with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, newline='') as f:
            f.write(csv_text)
            path = Path(f.name)
        try:
            rows, rejected = transform(path)
        finally:
            path.unlink()
        self.assertEqual([row['order_id'] for row in rows], ['A1'])
        self.assertEqual(len(rejected), 4)

    def test_scaled_dataset(self):
        path = Path(__file__).resolve().parents[2] / 'data' / 'sales_50000.csv'
        if not path.exists():
            self.skipTest('Scaled dataset not generated')
        import csv
        with path.open(newline='', encoding='utf-8') as f:
            count = sum(1 for _ in csv.DictReader(f))
        self.assertGreaterEqual(count, 50000)

    def test_multiple_line_items_for_one_order_are_not_duplicates(self):
        rows = [
            {"order_id":"ORDER-1","line_number":"1","product":"Desk","category":"Home","quantity":"1","unit_price":"100"},
            {"order_id":"ORDER-1","line_number":"2","product":"Chair","category":"Home","quantity":"2","unit_price":"25"},
        ]
        accepted, rejected = transform_records(rows)
        self.assertEqual(len(accepted), 2)
        self.assertEqual(len(rejected), 0)

    def test_same_order_line_is_rejected_as_duplicate(self):
        row = {"order_id":"ORDER-1","line_number":"1","product":"Desk","category":"Home","quantity":"1","unit_price":"100"}
        accepted, rejected = transform_records([row, row])
        self.assertEqual(len(accepted), 1)
        self.assertEqual(rejected[0]["reason"], "Duplicate order line")
