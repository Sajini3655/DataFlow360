from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("DataFlow360SalesTransform")
        .getOrCreate()
    )
    source = "/workspace/data/sales.csv"
    raw = spark.read.option("header", True).csv(source)
    clean = (
        raw.withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
        .filter(
            F.col("order_id").isNotNull()
            & (F.length(F.trim(F.col("order_id"))) > 0)
            & F.col("product").isNotNull()
            & F.col("category").isNotNull()
            & (F.col("quantity") > 0)
            & (F.col("unit_price") >= 0)
        )
        .dropDuplicates(["order_id"])
        .withColumn(
            "revenue",
            F.col("quantity") * F.col("unit_price"),
        )
    )
    clean.write.mode("overwrite").parquet("/workspace/output/sales_clean")


if __name__ == "__main__":
    main()
