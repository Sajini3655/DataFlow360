import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from medallion import parquet_layer_status, write_parquet_files


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

    def test_layer_status_counts_current_files_not_archived_spark_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            write_parquet_files(
                [{"row": 1}],
                [{"order_id": "O1", "quantity": 1}],
                [{"order_id": "O1", "revenue": 5}],
                output,
            )
            write_parquet_files(
                [{"row": 1}, {"row": 2}],
                [
                    {"order_id": "O1", "quantity": 1},
                    {"order_id": "O2", "quantity": 2},
                ],
                [
                    {"order_id": "O1", "revenue": 5},
                    {"order_id": "O2", "revenue": 10},
                ],
                output / "spark" / "runs" / "previous",
            )

            layers = parquet_layer_status(output)

            self.assertEqual([layer["rows"] for layer in layers], [1, 1, 1])
            self.assertEqual(
                [layer["files"] for layer in layers],
                [
                    ["bronze_sales.parquet"],
                    ["silver_sales.parquet"],
                    ["gold_sales.parquet"],
                ],
            )
