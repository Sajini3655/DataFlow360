import unittest
import json
import tempfile
from pathlib import Path

from adapters import csv_adapter, json_adapter


class TestAdapters(unittest.TestCase):
    def test_csv_adapter(self):
        with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, newline='') as f:
            f.write('order_id,quantity\nA1,2\n')
            path = f.name
        try:
            rows = csv_adapter(path)
            self.assertEqual(rows, [{'order_id': 'A1', 'quantity': '2'}])
        finally:
            Path(path).unlink()

    def test_json_adapter_list_and_object(self):
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
            json.dump([{'order_id': 'A1'}], f)
            list_path = f.name
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
            json.dump({'records': [{'order_id': 'A2'}]}, f)
            object_path = f.name
        try:
            self.assertEqual(json_adapter(list_path)[0]['order_id'], 'A1')
            self.assertEqual(json_adapter(object_path)[0]['order_id'], 'A2')
        finally:
            Path(list_path).unlink()
            Path(object_path).unlink()
