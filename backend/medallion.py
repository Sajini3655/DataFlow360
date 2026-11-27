import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet_files(bronze, silver, gold, output_dir):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    datasets = {
        "bronze_sales": bronze,
        "silver_sales": silver,
        "gold_sales": gold,
    }
    written = {}
    for name, records in datasets.items():
        target = output / f"{name}.parquet"
        temporary = output / f".{name}.parquet.tmp"
        table = pa.Table.from_pylist(records)
        pq.write_table(table, temporary)
        os.replace(temporary, target)
        written[name] = {"path": str(target), "rows": table.num_rows}
    return written
