import os
import json
import hashlib
from pathlib import Path
from datetime import date

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from pipeline import transform_records
from adapters import csv_adapter, json_adapter, rest_adapter
from airflow_client import AirflowError, get_dag_run, trigger_dag
from medallion import write_parquet_files
from warehouse_loader import upsert_gold_facts

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "data" / "sales_50000.csv"

app = FastAPI(title="DataFlow 360")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def ensure_schema():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute((ROOT / "warehouse" / "init.sql").read_text(encoding="utf-8"))
            migrations = ROOT / "warehouse" / "migrations"
            for migration in sorted(migrations.glob("*.sql")):
                cursor.execute(migration.read_text(encoding="utf-8"))


ensure_schema()


@app.get("/health")
def health():
    return {"status": "ok"}


def execute_pipeline(incremental: bool, source: str):
    source_name = "data/sales_50000.csv"
    source_path = SOURCE

    if source == "json":
        records = json_adapter(ROOT / "data" / "sales_50000.json")
        source_name = "data/sales_50000.json"
        source_path = ROOT / "data" / "sales_50000.json"
    elif source == "rest":
        records = rest_adapter("http://backend:8000/adapters/rest/sample")
        source_name = "rest://sales"
        source_path = None
    elif source == "postgres":
        with connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT order_id,line_number,customer_id,customer_name,product_id,product,"
                    "category,supplier_id,region,order_date,quantity,unit_price "
                    "FROM silver_sales"
                )
                records = cursor.fetchall()
        source_name = "postgres://silver_sales"
        source_path = None
    else:
        records = csv_adapter(source_path)

    source_bytes = source_path.read_bytes() if source_path else json.dumps(records, default=str).encode("utf-8")
    source_hash = hashlib.sha256(source_bytes).hexdigest()
    rows, rejected = transform_records(records)

    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_hash FROM ingestion_watermarks WHERE source_name=%s",
                (source_name,),
            )
            prior = cursor.fetchone()
            if incremental and prior and prior["source_hash"] == source_hash:
                cursor.execute(
                    "INSERT INTO pipeline_runs(accepted,rejected) VALUES (0,0) RETURNING id",
                )
                run_id = cursor.fetchone()["id"]
                return {
                    "accepted": 0,
                    "rejected": 0,
                    "skipped": True,
                    "run_id": run_id,
                    "source_hash": source_hash,
                    "bronze": len(records),
                    "silver": len(rows),
                    "gold": len(rows),
                    "parquet": write_parquet_snapshot(),
                }

            cursor.execute(
                "INSERT INTO pipeline_runs(accepted,rejected) VALUES (%s,%s) RETURNING id",
                (len(rows), len(rejected)),
            )
            run_id = cursor.fetchone()["id"]

            cursor.executemany(
                "INSERT INTO bronze_sales(source_row_number,source_name,payload) "
                "VALUES (%s,%s,%s::jsonb) "
                "ON CONFLICT(source_name,source_row_number) "
                "DO UPDATE SET payload=EXCLUDED.payload,ingested_at=now()",
                [
                    (row_number, source_name, json.dumps(record, default=str))
                    for row_number, record in enumerate(records, start=1)
                ],
            )
            cursor.executemany(
                "INSERT INTO silver_sales(order_id,line_number,customer_id,customer_name,product_id,"
                "product,category,supplier_id,region,order_date,quantity,unit_price) "
                "VALUES (%(order_id)s,%(line_number)s,%(customer_id)s,%(customer_name)s,%(product_id)s,"
                "%(product)s,%(category)s,%(supplier_id)s,%(region)s,"
                "NULLIF(%(order_date)s,'')::date,%(quantity)s,%(unit_price)s) "
                "ON CONFLICT(order_id,line_number) DO UPDATE SET "
                "customer_id=EXCLUDED.customer_id,customer_name=EXCLUDED.customer_name,"
                "product_id=EXCLUDED.product_id,product=EXCLUDED.product,"
                "category=EXCLUDED.category,supplier_id=EXCLUDED.supplier_id,"
                "region=EXCLUDED.region,order_date=EXCLUDED.order_date,"
                "quantity=EXCLUDED.quantity,unit_price=EXCLUDED.unit_price,"
                "transformed_at=now()",
                rows,
            )

            cursor.execute(
                "INSERT INTO dim_customer(customer_id,customer_name,region) "
                "SELECT DISTINCT ON (coalesce(customer_id,'UNKNOWN')) "
                "coalesce(customer_id,'UNKNOWN'),"
                "coalesce(customer_name,'Unknown'),coalesce(region,'Unknown') "
                "FROM silver_sales "
                "ON CONFLICT(customer_id) DO UPDATE SET "
                "customer_name=EXCLUDED.customer_name,region=EXCLUDED.region"
            )
            cursor.execute(
                "INSERT INTO dim_category(category_name) "
                "SELECT DISTINCT category FROM silver_sales "
                "ON CONFLICT(category_name) DO UPDATE SET category_name=EXCLUDED.category_name"
            )
            cursor.execute(
                "INSERT INTO dim_product(product_name,category_key) "
                "SELECT DISTINCT ON (s.product,c.category_key) s.product,c.category_key FROM silver_sales s "
                "JOIN dim_category c ON c.category_name=s.category "
                "ON CONFLICT(product_name,category_key) "
                "DO UPDATE SET category_key=EXCLUDED.category_key"
            )
            cursor.execute(
                "INSERT INTO dim_region(region_name) "
                "SELECT DISTINCT coalesce(region,'Unknown') FROM silver_sales "
                "ON CONFLICT(region_name) DO UPDATE SET region_name=EXCLUDED.region_name"
            )
            cursor.execute(
                "INSERT INTO dim_supplier(supplier_id,supplier_name,region) "
                "SELECT DISTINCT ON (coalesce(supplier_id,'UNKNOWN')) "
                "coalesce(supplier_id,'UNKNOWN'),"
                "'Supplier '||coalesce(supplier_id,'UNKNOWN'),coalesce(region,'Unknown') "
                "FROM silver_sales "
                "ON CONFLICT(supplier_id) DO UPDATE SET "
                "supplier_name=EXCLUDED.supplier_name,region=EXCLUDED.region"
            )
            cursor.execute(
                "INSERT INTO dim_date(date_key,full_date,year,month,month_name,quarter) "
                "SELECT distinct (extract(year from order_date)::text||lpad(extract(month from order_date)::text,2,'0')||"
                "lpad(extract(day from order_date)::text,2,'0'))::integer,"
                "order_date,extract(year from order_date)::integer,"
                "extract(month from order_date)::integer,to_char(order_date,'FMMonth'),"
                "extract(quarter from order_date)::integer "
                "FROM silver_sales WHERE order_date IS NOT NULL "
                "ON CONFLICT(date_key) DO UPDATE SET "
                "full_date=EXCLUDED.full_date,year=EXCLUDED.year,month=EXCLUDED.month,"
                "month_name=EXCLUDED.month_name,quarter=EXCLUDED.quarter"
            )
            upsert_gold_facts(cursor)

            cursor.execute(
                "INSERT INTO inventory_fact(product_key,supplier_key,region_key,date_key,"
                "opening_stock,quantity_sold,closing_stock) "
                "SELECT p.product_key,max(s.supplier_key),max(r.region_key),d.date_key,"
                "1000,coalesce(sum(g.quantity),0),"
                "GREATEST(1000-coalesce(sum(g.quantity),0),0) "
                "FROM gold_sales_fact g "
                "JOIN dim_product p ON p.product_key=g.product_key "
                "JOIN dim_supplier s ON s.supplier_key=g.supplier_key "
                "JOIN dim_region r ON r.region_key=g.region_key "
                "JOIN dim_date d ON d.date_key=g.date_key "
                "GROUP BY p.product_key,d.date_key "
                "ON CONFLICT(product_key,date_key) DO UPDATE SET "
                "supplier_key=EXCLUDED.supplier_key,region_key=EXCLUDED.region_key,"
                "opening_stock=EXCLUDED.opening_stock,"
                "quantity_sold=EXCLUDED.quantity_sold,"
                "closing_stock=EXCLUDED.closing_stock"
            )

            cursor.executemany(
                "INSERT INTO rejected_sales(pipeline_run_id,source_name,source_row_number,record,reason) "
                "VALUES (%s,%s,%s,%s::jsonb,%s)",
                [
                    (run_id, source_name, item["source_row_number"], json.dumps(item["record"], default=str), item["reason"])
                    for item in rejected
                ],
            )

            cursor.execute(
                "INSERT INTO ingestion_watermarks(source_name,source_hash,rows_read,"
                "rows_accepted,rows_rejected) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT(source_name) DO UPDATE SET "
                "source_hash=EXCLUDED.source_hash,last_processed_at=now(),"
                "rows_read=EXCLUDED.rows_read,rows_accepted=EXCLUDED.rows_accepted,"
                "rows_rejected=EXCLUDED.rows_rejected",
                (source_name, source_hash, len(records), len(rows), len(rejected)),
            )

    parquet = write_parquet_snapshot()
    return {
        "accepted": len(rows),
        "rejected": len(rejected),
        "skipped": False,
        "run_id": run_id,
        "source_hash": source_hash,
        "parquet": parquet,
        "bronze": len(records),
        "silver": len(rows),
        "gold": len(rows),
    }


def write_parquet_snapshot():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT source_row_number,source_name,payload::text AS payload,ingested_at "
                "FROM bronze_sales ORDER BY source_name,source_row_number"
            )
            bronze = cursor.fetchall()
            cursor.execute("SELECT * FROM silver_sales ORDER BY order_id,line_number")
            silver = cursor.fetchall()
            cursor.execute("SELECT * FROM gold_sales_fact ORDER BY order_id,line_number")
            gold = cursor.fetchall()
    return write_parquet_files(
        bronze,
        silver,
        gold,
        os.getenv("PARQUET_DIR", str(ROOT / "data" / "parquet")),
    )


@app.post("/pipeline/run", status_code=status.HTTP_202_ACCEPTED)
def run(incremental: bool = True, source: str = "csv"):
    if source not in {"csv", "json", "rest", "postgres"}:
        raise HTTPException(status_code=400, detail="Unsupported source adapter")
    try:
        payload = trigger_dag(source, incremental)
    except AirflowError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    dag_run_id = payload.get("dag_run_id")
    if not dag_run_id:
        raise HTTPException(status_code=502, detail="Airflow did not return a DAG run ID")
    job_id = str(payload.get("run_id") or dag_run_id)
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO pipeline_jobs(job_id,dag_run_id,source_name) "
                "VALUES (%s,%s,%s) ON CONFLICT(dag_run_id) DO NOTHING",
                (job_id, dag_run_id, source),
            )
    return {
        "job_id": job_id,
        "dag_id": "dataflow360_daily_pipeline",
        "dag_run_id": dag_run_id,
        "state": payload.get("state", "queued"),
        "source": source,
    }


@app.get("/pipeline/runs/{dag_run_id}")
def pipeline_run_status(dag_run_id: str):
    try:
        return get_dag_run(dag_run_id)
    except AirflowError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/pipeline/execute")
def execute_from_airflow(
    source: str = "csv",
    incremental: bool = True,
    x_pipeline_token: str | None = Header(default=None),
):
    expected = os.getenv("PIPELINE_INTERNAL_TOKEN", "")
    if not expected or x_pipeline_token != expected:
        raise HTTPException(status_code=401, detail="Invalid internal pipeline token")
    if source not in {"csv", "json", "rest", "postgres"}:
        raise HTTPException(status_code=400, detail="Unsupported source adapter")
    try:
        return execute_pipeline(incremental=incremental, source=source)
    except (OSError, ValueError, psycopg.Error) as exc:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {exc}") from exc


@app.get("/adapters")
def adapters():
    return {
        "csv": {"status": "available", "sample": "sales_50000.csv"},
        "json": {"status": "available", "sample": "sales_50000.json"},
        "rest": {"status": "available", "sample": "/adapters/rest/sample"},
        "postgres": {"status": "available", "source": "silver_sales"},
    }


@app.get("/adapters/rest/sample")
def rest_sample():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT order_id,line_number,customer_id,customer_name,product_id,product,"
                "category,supplier_id,region,order_date,quantity,unit_price "
                "FROM silver_sales LIMIT 100"
            )
            return {"records": cursor.fetchall()}


@app.get("/overview")
def overview():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) AS records,coalesce(sum(quantity*unit_price),0) AS revenue,"
                "coalesce(sum(quantity),0) AS units FROM silver_sales"
            )
            metrics = cursor.fetchone()
            cursor.execute(
                "SELECT category,coalesce(sum(revenue),0) AS revenue "
                "FROM silver_sales GROUP BY category ORDER BY revenue DESC"
            )
            categories = cursor.fetchall()
            cursor.execute(
                "SELECT region,coalesce(sum(revenue),0) AS revenue "
                "FROM silver_sales GROUP BY region ORDER BY revenue DESC"
            )
            regions = cursor.fetchall()
            cursor.execute("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT 20")
            runs = cursor.fetchall()
            cursor.execute(
                "SELECT count(*) AS rows,count(DISTINCT pipeline_run_id) AS runs "
                "FROM rejected_sales"
            )
            rejected = cursor.fetchone()
            cursor.execute(
                "SELECT (SELECT count(*) FROM bronze_sales) AS bronze,"
                "(SELECT count(*) FROM silver_sales) AS silver,"
                "(SELECT count(*) FROM gold_sales_fact) AS gold,"
                "(SELECT count(*) FROM inventory_fact) AS inventory"
            )
            layers = cursor.fetchone()
            cursor.execute(
                "SELECT source_name,source_hash,last_processed_at,rows_read,"
                "rows_accepted,rows_rejected FROM ingestion_watermarks "
                "ORDER BY last_processed_at DESC"
            )
            watermarks = cursor.fetchall()
    return {
        "metrics": metrics,
        "categories": categories,
        "regions": regions,
        "runs": runs,
        "rejected": rejected,
        "layers": layers,
        "watermarks": watermarks,
    }


@app.get("/quality")
def quality():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT reason,count(*) AS rows FROM rejected_sales "
                "GROUP BY reason ORDER BY rows DESC"
            )
            reasons = cursor.fetchall()
            cursor.execute("SELECT count(*) AS rows FROM bronze_sales")
            bronze = cursor.fetchone()
            cursor.execute("SELECT count(*) AS rows FROM silver_sales")
            silver = cursor.fetchone()
    return {"reasons": reasons, "bronze": bronze["rows"], "silver": silver["rows"]}


@app.get("/inventory")
def inventory():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT p.product_name,c.category_name AS category,"
                "coalesce(sum(s.quantity),0) AS quantity_sold,"
                "coalesce(sum(s.revenue),0) AS revenue "
                "FROM silver_sales s "
                "JOIN dim_category c ON c.category_name=s.category "
                "JOIN dim_product p ON p.product_name=s.product "
                "AND p.category_key=c.category_key "
                "GROUP BY p.product_name,c.category_name ORDER BY revenue DESC"
            )
            products = cursor.fetchall()
    return {"products": products}


@app.get("/dimensions")
def dimensions():
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT (SELECT count(*) FROM dim_customer) AS customers,"
                "(SELECT count(*) FROM dim_product) AS products,"
                "(SELECT count(*) FROM dim_date) AS dates,"
                "(SELECT count(*) FROM dim_region) AS regions,"
                "(SELECT count(*) FROM dim_supplier) AS suppliers,"
                "(SELECT count(*) FROM gold_sales_fact) AS sales_facts,"
                "(SELECT count(*) FROM inventory_fact) AS inventory_facts"
            )
            return cursor.fetchone()


@app.get("/analytics")
def analytics(
    category: str | None = None,
    region: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
):
    conditions = []
    filters = []
    if category is not None:
        conditions.append("category = %s")
        filters.append(category)
    if region is not None:
        conditions.append("region = %s")
        filters.append(region)
    if start_date is not None:
        conditions.append("order_date >= %s")
        filters.append(start_date)
    if end_date is not None:
        conditions.append("order_date <= %s")
        filters.append(end_date)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    filter_params = tuple(filters)
    with connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT coalesce(sum(revenue),0) AS revenue,"
                f"coalesce(sum(quantity),0) AS units,count(*) AS records "
                f"FROM silver_sales {where}",
                filter_params,
            )
            metrics = cursor.fetchone()
            cursor.execute(
                f"SELECT region,sum(revenue) AS revenue FROM silver_sales {where} "
                "GROUP BY region ORDER BY revenue DESC",
                filter_params,
            )
            regions = cursor.fetchall()
            cursor.execute(
                f"SELECT category,sum(revenue) AS revenue FROM silver_sales {where} "
                "GROUP BY category ORDER BY revenue DESC",
                filter_params,
            )
            categories = cursor.fetchall()
            cursor.execute(
                f"SELECT date_trunc('month',order_date)::date AS month,sum(revenue) AS revenue "
                f"FROM silver_sales {where} GROUP BY month ORDER BY month",
                filter_params,
            )
            monthly = cursor.fetchall()
            cursor.execute("SELECT DISTINCT category FROM silver_sales ORDER BY category")
            category_options = [row["category"] for row in cursor.fetchall()]
            cursor.execute("SELECT DISTINCT region FROM silver_sales ORDER BY region")
            region_options = [row["region"] for row in cursor.fetchall()]
    return {
        "metrics": metrics,
        "regions": regions,
        "categories": categories,
        "monthly": monthly,
        "filters": {"categories": category_options, "regions": region_options},
    }
