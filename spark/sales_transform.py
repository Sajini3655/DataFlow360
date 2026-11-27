import argparse
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

backend_dir = Path("/app/backend")
if not backend_dir.is_dir():
    backend_dir = Path(__file__).resolve().parents[1] / "backend"
sys.path.insert(0, str(backend_dir))
from pipeline import transform_records


SALES_SCHEMA = StructType(
    [
        StructField(name, StringType(), True)
        for name in (
            "order_id",
            "line_number",
            "customer_id",
            "customer_name",
            "product_id",
            "product",
            "category",
            "supplier_id",
            "region",
            "order_date",
            "quantity",
            "unit_price",
        )
    ]
)


def _raw_bronze(spark, input_path, output_dir):
    source = Path(input_path)
    if source.suffix.lower() == ".parquet":
        bronze_path = source
    elif source.suffix.lower() == ".csv":
        raw_csv = spark.read.option("header", True).option("inferSchema", False).csv(str(source))
        bronze_path = Path(output_dir) / "bronze_sales"
        bronze = raw_csv.select(
            (F.monotonically_increasing_id() + F.lit(1)).cast("long").alias("source_row_number"),
            F.lit(str(source.name)).alias("source_name"),
            F.to_json(F.struct(*[F.col(name) for name in raw_csv.columns])).alias("payload"),
        )
        bronze.write.mode("overwrite").parquet(str(bronze_path))
    else:
        raise ValueError("Spark currently supports CSV sources or persisted Bronze Parquet")

    bronze_frame = spark.read.parquet(str(bronze_path))
    if "payload" not in bronze_frame.columns:
        if set(SALES_SCHEMA.fieldNames()).issubset(bronze_frame.columns):
            return bronze_frame
        raise ValueError(f"Bronze Parquet at {bronze_path} has no supported payload column")

    return (
        bronze_frame.select(
            F.col("source_row_number"),
            F.from_json(F.col("payload"), SALES_SCHEMA).alias("record"),
        )
        .select("source_row_number", "record.*")
    )


def transform_bronze(spark, input_path, output_dir):
    raw = _raw_bronze(spark, input_path, output_dir)
    return _transform_raw(raw)


def _transform_raw(raw):
    columns = set(raw.columns)
    if "order_id" not in columns:
        raise ValueError("Bronze dataset is missing order_id")
    if "line_number" not in columns:
        raw = raw.withColumn("line_number", F.lit("1"))
    for column in SALES_SCHEMA.fieldNames():
        if column not in raw.columns:
            raw = raw.withColumn(column, F.lit(None).cast("string"))

    normalized = (
        raw.withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("line_number", F.coalesce(F.col("line_number").cast("int"), F.lit(1)))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("product", F.trim(F.col("product")))
        .withColumn("category", F.trim(F.col("category")))
        .withColumn("supplier_id", F.trim(F.col("supplier_id")))
        .withColumn("region", F.trim(F.col("region")))
        .withColumn("order_date", F.to_date(F.col("order_date")))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    )
    duplicate_window = Window.partitionBy("order_id", "line_number").orderBy(
        F.col("source_row_number")
    )
    checked = normalized.withColumn(
        "_duplicate_rank", F.row_number().over(duplicate_window)
    )
    reason = (
        F.when(
            F.col("order_id").isNull()
            | (F.length(F.trim(F.col("order_id"))) == 0)
            | F.col("product").isNull()
            | (F.length(F.trim(F.col("product"))) == 0)
            | F.col("category").isNull()
            | (F.length(F.trim(F.col("category"))) == 0)
            | F.col("order_date").isNull(),
            F.lit("Missing required fields or invalid order date"),
        )
        .when(F.col("line_number") < 1, F.lit("Line number must be positive"))
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("Quantity must be positive"))
        .when(
            F.col("unit_price").isNull() | (F.col("unit_price") < 0),
            F.lit("Unit price must be non-negative and numeric"),
        )
        .when(F.col("_duplicate_rank") > 1, F.lit("Duplicate order line"))
    )
    checked = checked.withColumn("_rejection_reason", reason)
    accepted = checked.filter(F.col("_rejection_reason").isNull()).select(
        "order_id",
        "line_number",
        "customer_id",
        "customer_name",
        "product_id",
        "product",
        "category",
        "supplier_id",
        "region",
        "order_date",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)").alias("revenue"),
    )
    rejected = checked.filter(F.col("_rejection_reason").isNotNull()).select(
        "source_row_number",
        F.to_json(
            F.struct(*[F.col(name) for name in SALES_SCHEMA.fieldNames()])
        ).alias("record_json"),
        F.col("_rejection_reason").alias("reason"),
    )
    gold = accepted.groupBy("order_date", "region", "category").agg(
        F.count("*").alias("line_count"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum("revenue").alias("revenue"),
    )
    return raw, accepted, rejected, gold


def _directory_size(path):
    return sum(file.stat().st_size for file in Path(path).rglob("*") if file.is_file())


def run_spark(spark, input_path, output_dir, benchmark_id=None):
    started = time.perf_counter()
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    raw = _raw_bronze(spark, input_path, output)
    input_rows = raw.count()
    shuffle_partitions = max(4, min(200, (input_rows + 24_999) // 25_000))
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    raw, accepted, rejected, gold = _transform_raw(raw)
    accepted.cache()
    rejected.cache()
    accepted_rows = accepted.count()
    rejected_rows = rejected.count()
    revenue_row = accepted.agg(F.coalesce(F.sum("revenue"), F.lit(0)).alias("revenue")).first()
    revenue = str(revenue_row["revenue"])
    accepted_path = output / "silver_sales"
    rejected_path = output / "rejected_sales"
    gold_path = output / "gold_sales"
    accepted.repartition(shuffle_partitions).write.mode("overwrite").parquet(str(accepted_path))
    rejected.coalesce(shuffle_partitions).write.mode("overwrite").parquet(str(rejected_path))
    gold.coalesce(shuffle_partitions).write.mode("overwrite").parquet(str(gold_path))
    duration = time.perf_counter() - started
    result = {
        "engine": "pyspark",
        "input_rows": input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "duration_seconds": round(duration, 4),
        "throughput_rows_per_second": round(input_rows / duration, 4) if duration else 0,
        "output_bytes": _directory_size(output),
        "revenue": revenue,
        "silver_path": str(accepted_path),
        "rejected_path": str(rejected_path),
        "gold_path": str(gold_path),
        "benchmark_id": benchmark_id,
    }
    accepted.unpersist()
    rejected.unpersist()
    return result


def run_pandas(input_path, output_dir, benchmark_id=None):
    from pipeline import transform_records

    started = time.perf_counter()
    input_path = Path(input_path)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    if input_path.suffix.lower() == ".parquet":
        bronze = pq.read_table(input_path).to_pylist()
        records = [json.loads(row["payload"]) for row in bronze]
    else:
        records = pd.read_csv(input_path, dtype=str, keep_default_na=False).to_dict("records")
    rows, rejected = transform_records(records, require_order_date=True)
    accepted_frame = pd.DataFrame(rows)
    if not accepted_frame.empty:
        accepted_frame["quantity"] = accepted_frame["quantity"].astype("int64")
        accepted_frame["unit_price"] = accepted_frame["unit_price"].map(Decimal)
        accepted_frame["revenue"] = accepted_frame["quantity"].map(Decimal) * accepted_frame["unit_price"]
        accepted_frame["order_date"] = pd.to_datetime(accepted_frame["order_date"]).dt.date
    rejected_frame = pd.DataFrame(
        [
            {
                "source_row_number": item["source_row_number"],
                "record_json": json.dumps(item["record"], default=str),
                "reason": item["reason"],
            }
            for item in rejected
        ]
    )
    if accepted_frame.empty:
        gold_frame = pd.DataFrame(
            columns=["order_date", "region", "category", "line_count", "order_count", "units_sold", "revenue"]
        )
        revenue = "0"
    else:
        gold_frame = (
            accepted_frame.groupby(["order_date", "region", "category"], dropna=False)
            .agg(
                line_count=("order_id", "size"),
                order_count=("order_id", "nunique"),
                units_sold=("quantity", "sum"),
                revenue=("revenue", "sum"),
            )
            .reset_index()
        )
        revenue = str(accepted_frame["revenue"].sum())
    silver_path = output / "silver_sales.parquet"
    rejected_path = output / "rejected_sales.parquet"
    gold_path = output / "gold_sales.parquet"
    accepted_frame.to_parquet(silver_path, index=False)
    rejected_frame.to_parquet(rejected_path, index=False)
    gold_frame.to_parquet(gold_path, index=False)
    duration = time.perf_counter() - started
    return {
        "engine": "pandas",
        "input_rows": len(records),
        "accepted_rows": len(rows),
        "rejected_rows": len(rejected),
        "duration_seconds": round(duration, 4),
        "throughput_rows_per_second": round(len(records) / duration, 4) if duration else 0,
        "output_bytes": _directory_size(output),
        "revenue": revenue,
        "silver_path": str(silver_path),
        "rejected_path": str(rejected_path),
        "gold_path": str(gold_path),
        "benchmark_id": benchmark_id,
    }


def run_benchmark(spark, input_path, output_dir, benchmark_id):
    output = Path(output_dir) / benchmark_id
    pandas_result = run_pandas(input_path, output / "pandas", benchmark_id)
    spark_result = run_spark(spark, input_path, output / "pyspark", benchmark_id)
    comparable = ("input_rows", "accepted_rows", "rejected_rows", "revenue")
    mismatches = {
        key: {"pandas": pandas_result[key], "pyspark": spark_result[key]}
        for key in comparable
        if str(pandas_result[key]) != str(spark_result[key])
    }
    if mismatches:
        raise ValueError(f"Pandas and PySpark results differ: {json.dumps(mismatches)}")
    return {
        "benchmark_id": benchmark_id,
        "equivalent": True,
        "pandas": pandas_result,
        "pyspark": spark_result,
    }


def create_spark_session(master=None):
    builder = SparkSession.builder.appName("DataFlow360SalesTransform")
    selected_master = master or "local[*]"
    builder = builder.master(selected_master)
    if selected_master.startswith("local"):
        builder = builder.config("spark.ui.enabled", "false")
    else:
        builder = (
            builder.config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "spark-runner"))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.driver.port", "37891")
            .config("spark.blockManager.port", "37892")
        )
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="/app/data/sales_50000.csv")
    parser.add_argument("--output-dir", default="/app/data/parquet/spark/manual")
    parser.add_argument("--master", default=None)
    parser.add_argument("--benchmark", action="store_true")
    args = parser.parse_args()
    spark = create_spark_session(args.master)
    try:
        if args.benchmark:
            result = run_benchmark(spark, args.input, args.output_dir, "manual-benchmark")
        else:
            result = run_spark(spark, args.input, args.output_dir)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
