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


def parquet_layer_status(output_dir):
    output = Path(output_dir)
    layers = []
    for layer in ("bronze_sales", "silver_sales", "gold_sales"):
        path = output / f"{layer}.parquet"
        exists = path.is_file()
        file_stat = path.stat() if exists else None
        layers.append(
            {
                "layer": layer.removesuffix("_sales"),
                "exists": exists,
                "files": [str(path.relative_to(output))] if exists else [],
                "rows": pq.read_metadata(path).num_rows if exists else 0,
                "bytes": file_stat.st_size if file_stat else 0,
                "last_updated": file_stat.st_mtime if file_stat else None,
            }
        )
    return layers
