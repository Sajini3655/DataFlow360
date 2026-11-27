import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from medallion import write_parquet_files


class TestParquetGeneration(unittest.TestCase):
    def test_writes_and_refreshes_three_layers(self):
        with tempfile.TemporaryDirectory() as directory:
            written = write_parquet_files(
                [{"row": 1}],
                [{"order_id": "O1", "quantity": 1}],
                [{"order_id": "O1", "revenue": 5}],
                directory,
            )
            self.assertEqual(written["bronze_sales"]["rows"], 1)
            self.assertEqual(written["silver_sales"]["rows"], 1)
            self.assertEqual(written["gold_sales"]["rows"], 1)

            write_parquet_files(
                [{"row": 1}, {"row": 2}],
                [{"order_id": "O1", "quantity": 2}],
                [{"order_id": "O1", "revenue": 10}],
                directory,
            )
            self.assertEqual(
                pq.read_table(Path(directory) / "bronze_sales.parquet").num_rows,
                2,
            )
