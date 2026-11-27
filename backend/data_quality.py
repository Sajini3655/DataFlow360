from datetime import date
from decimal import Decimal, InvalidOperation
import json
from typing import Any


def _value_type(value):
    if value is None or value == "":
        return "null"
    text = str(value).strip()
    if not text:
        return "null"
    try:
        int(text)
        return "integer"
    except ValueError:
        pass
    try:
        number = Decimal(text)
        if number.is_finite():
            return "decimal"
    except InvalidOperation:
        pass
    try:
        date.fromisoformat(text)
        return "date"
    except ValueError:
        return "text"


def infer_schema(records):
    fields = {}
    for record in records:
        for name, value in record.items():
            fields.setdefault(name, set()).add(_value_type(value))
    return {
        name: sorted(types - {"null"}) or ["null"]
        for name, types in sorted(fields.items())
    }


def detect_schema_drift(cursor, source_name, records, pipeline_run_id):
    if not records:
        return None
    observed = infer_schema(records)
    cursor.execute(
        "SELECT observed_schema FROM source_schema_snapshots WHERE source_name=%s",
        (source_name,),
    )
    prior = cursor.fetchone()
    if prior is None:
        cursor.execute(
            "INSERT INTO source_schema_snapshots(source_name,observed_schema) "
            "VALUES (%s,%s::jsonb)",
            (source_name, json.dumps(observed)),
        )
        return None

    previous = prior["observed_schema"]
    changes = {
        "missing_columns": sorted(set(previous) - set(observed)),
        "unexpected_columns": sorted(set(observed) - set(previous)),
        "changed_types": {
            name: {"previous": previous[name], "observed": observed[name]}
            for name in set(previous).intersection(observed)
            if previous[name] != observed[name]
        },
    }
    if any(changes.values()):
        cursor.execute(
            "INSERT INTO schema_drift_events(pipeline_run_id,source_name,previous_schema,"
            "observed_schema,changes) VALUES (%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)",
            (
                pipeline_run_id,
                source_name,
                json.dumps(previous),
                json.dumps(observed),
                json.dumps(changes),
            ),
        )
    cursor.execute(
        "UPDATE source_schema_snapshots SET observed_schema=%s::jsonb,observed_at=now() "
        "WHERE source_name=%s",
        (json.dumps(observed), source_name),
    )
    return changes if any(changes.values()) else None


def _is_missing(value):
    return value is None or not str(value).strip()


def _valid_quantity(value):
    try:
        quantity = Decimal(str(value))
        return quantity.is_finite() and quantity > 0 and quantity == quantity.to_integral_value()
    except (InvalidOperation, ValueError, TypeError):
        return False


def _valid_price(value):
    try:
        price = Decimal(str(value))
        return price.is_finite() and price >= 0
    except (InvalidOperation, ValueError, TypeError):
        return False


def _valid_date(value):
    try:
        date.fromisoformat(str(value).strip())
        return True
    except (ValueError, TypeError):
        return False


def _within_numeric_range(value, config):
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            return False
        minimum = config.get("min")
        maximum = config.get("max")
        return (minimum is None or number >= Decimal(str(minimum))) and (
            maximum is None or number <= Decimal(str(maximum))
        )
    except (InvalidOperation, ValueError, TypeError):
        return False


def _within_date_range(value, config):
    try:
        observed = date.fromisoformat(str(value).strip())
        minimum = date.fromisoformat(config["start_date"]) if config.get("start_date") else None
        maximum = date.fromisoformat(config["end_date"]) if config.get("end_date") else None
        return (minimum is None or observed >= minimum) and (
            maximum is None or observed <= maximum
        )
    except (ValueError, TypeError):
        return False


def validate_rule_config(rule_id: str, config: dict[str, Any]) -> None:
    if rule_id == "required_fields":
        columns = config.get("columns")
        if not isinstance(columns, list) or not columns or any(
            not isinstance(column, str) or not column.strip() for column in columns
        ):
            raise ValueError("Required-field configuration needs a non-empty columns list")
    elif rule_id == "accepted_values":
        if not isinstance(config.get("column"), str) or not config["column"].strip():
            raise ValueError("Accepted-values configuration needs a column")
        values = config.get("values")
        if not isinstance(values, list) or not values or any(
            not isinstance(value, str) for value in values
        ):
            raise ValueError("Accepted-values configuration needs a non-empty string list")
    elif rule_id in {"numeric_range", "quantity_range", "unit_price_range", "valid_discount"}:
        if "column" in config and (
            not isinstance(config["column"], str) or not config["column"].strip()
        ):
            raise ValueError("Numeric-range configuration column must be a non-empty string")
        try:
            minimum = Decimal(str(config["min"])) if "min" in config else None
            maximum = Decimal(str(config["max"])) if "max" in config else None
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError("Numeric-range bounds must be finite numbers") from exc
        if any(value is not None and not value.is_finite() for value in (minimum, maximum)):
            raise ValueError("Numeric-range bounds must be finite numbers")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("Numeric-range minimum cannot exceed its maximum")
    elif rule_id == "date_range":
        if "column" in config and (
            not isinstance(config["column"], str) or not config["column"].strip()
        ):
            raise ValueError("Date-range configuration column must be a non-empty string")
        try:
            minimum = date.fromisoformat(config["start_date"]) if config.get("start_date") else None
            maximum = date.fromisoformat(config["end_date"]) if config.get("end_date") else None
        except (ValueError, TypeError) as exc:
            raise ValueError("Date-range bounds must use ISO date values") from exc
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ValueError("Date-range start cannot follow its end")


def evaluate_rules(records, rules):
    failures = {}
    for rule in rules:
        rule_id = rule["rule_id"]
        config = rule.get("config") or {}
        if isinstance(config, str):
            config = json.loads(config)
        failed_rows = set()

        if rule_id == "required_fields":
            required = config.get(
                "columns",
                ["order_id", "product", "category", "quantity", "unit_price"],
            )
            failed_rows = {
                index
                for index, record in enumerate(records)
                if any(_is_missing(record.get(field)) for field in required)
            }
        elif rule_id == "unique_business_key":
            seen = set()
            for index, record in enumerate(records):
                key = (
                    str(record.get("order_id") or "").strip(),
                    str(record.get("line_number") or "1").strip(),
                )
                if not key[0]:
                    continue
                if key in seen:
                    failed_rows.add(index)
                seen.add(key)
        elif rule_id == "valid_quantity":
            failed_rows = {
                index for index, record in enumerate(records)
                if not _valid_quantity(record.get("quantity"))
            }
        elif rule_id == "valid_unit_price":
            failed_rows = {
                index for index, record in enumerate(records)
                if not _valid_price(record.get("unit_price"))
            }
        elif rule_id == "valid_order_date":
            failed_rows = {
                index for index, record in enumerate(records)
                if not _valid_date(record.get("order_date"))
            }
        elif rule_id in {"numeric_range", "quantity_range", "unit_price_range"}:
            column = config.get("column", "value")
            failed_rows = {
                index for index, record in enumerate(records)
                if not _within_numeric_range(record.get(column), config)
            }
        elif rule_id == "accepted_values":
            column = config.get("column")
            accepted_values = {str(value) for value in config.get("values", [])}
            if column and accepted_values:
                failed_rows = {
                    index for index, record in enumerate(records)
                    if _is_missing(record.get(column))
                    or str(record.get(column)).strip() not in accepted_values
                }
        elif rule_id == "date_range":
            column = config.get("column", "order_date")
            failed_rows = {
                index for index, record in enumerate(records)
                if not _within_date_range(record.get(column), config)
            }
        elif rule_id == "valid_discount":
            column = config.get("column", "discount")
            failed_rows = {
                index for index, record in enumerate(records)
                if column in record
                and not _is_missing(record.get(column))
                and not _within_numeric_range(
                    record.get(column),
                    {"min": config.get("min", 0), "max": config.get("max", 1)},
                )
            }

        failures[rule_id] = failed_rows

    return failures


def summarize_quality(rows_checked, rows_accepted, rows_rejected, failures):
    failed_any = set().union(*failures.values()) if failures else set()
    overall_score = (
        100.0
        if rows_checked == 0
        else round(
            100.0
            * (rows_checked - min(rows_checked, len(failed_any)))
            / rows_checked,
            4,
        )
    )
    acceptance_rate = (
        0.0 if rows_checked == 0 else 100.0 * rows_accepted / rows_checked
    )
    rejection_rate = (
        0.0 if rows_checked == 0 else 100.0 * rows_rejected / rows_checked
    )
    return {
        "rows_checked": rows_checked,
        "rows_accepted": rows_accepted,
        "rows_rejected": rows_rejected,
        "acceptance_rate": round(acceptance_rate, 4),
        "rejection_rate": round(rejection_rate, 4),
        "overall_score": overall_score,
    }


def persist_quality_results(
    cursor, pipeline_run_id, source_name, records, accepted, rejected
):
    cursor.execute(
        "SELECT rule_id,threshold_percent,enabled,config FROM data_quality_rules "
        "ORDER BY rule_id"
    )
    configured_rules = cursor.fetchall()
    enabled_rules = [rule for rule in configured_rules if rule["enabled"]]
    failures = evaluate_rules(records, enabled_rules)

    accepted_order_ids = sorted(
        {str(row["order_id"]) for row in accepted if row.get("order_id")}
    )
    raw_indices_by_key = {}
    for index, record in enumerate(records):
        key = (
            str(record.get("order_id") or "").strip(),
            str(record.get("line_number") or "1").strip(),
        )
        raw_indices_by_key.setdefault(key, index)
    for rule_id in ("referential_integrity", "revenue_consistency"):
        if not any(rule["rule_id"] == rule_id for rule in enabled_rules):
            continue
        if not accepted_order_ids:
            failures[rule_id] = set()
            continue
        if rule_id == "referential_integrity":
            cursor.execute(
                "SELECT s.order_id,s.line_number FROM silver_sales s "
                "LEFT JOIN dim_customer c ON c.customer_id=s.customer_id "
                "LEFT JOIN dim_category ca ON ca.category_name=s.category "
                "LEFT JOIN dim_product p ON p.product_name=s.product AND p.category_key=ca.category_key "
                "LEFT JOIN dim_date d ON d.full_date=s.order_date "
                "LEFT JOIN dim_region r ON r.region_name=s.region "
                "LEFT JOIN dim_supplier su ON su.supplier_id=s.supplier_id "
                "WHERE s.order_id=ANY(%s) AND "
                "(c.customer_key IS NULL OR ca.category_key IS NULL OR p.product_key IS NULL "
                "OR d.date_key IS NULL OR r.region_key IS NULL OR su.supplier_key IS NULL)",
                (accepted_order_ids,),
            )
        else:
            cursor.execute(
                "SELECT order_id,line_number FROM silver_sales "
                "WHERE order_id=ANY(%s) AND revenue <> quantity * unit_price",
                (accepted_order_ids,),
            )
        failures[rule_id] = {
            raw_indices_by_key.get((row["order_id"], str(row["line_number"])), -1)
            for row in cursor.fetchall()
        }
        failures[rule_id].discard(-1)

    rows_checked = len(records)
    summary = summarize_quality(rows_checked, len(accepted), len(rejected), failures)
    overall_score = summary["overall_score"]
    for rule in configured_rules:
        rule_id = rule["rule_id"]
        enabled = rule["enabled"]
        failed_count = len(failures.get(rule_id, set()))
        threshold = float(rule["threshold_percent"])
        failure_rate = 0.0 if rows_checked == 0 else 100.0 * failed_count / rows_checked
        state = (
            "skipped"
            if not enabled
            else "passed"
            if failure_rate <= threshold
            else "failed"
        )
        cursor.execute(
            "INSERT INTO data_quality_results(pipeline_run_id,source_name,dataset_name,"
            "data_layer,rule_id,state,checked_count,failure_count,failure_rate,threshold_percent) "
            "VALUES (%s,%s,'sales','silver',%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(pipeline_run_id,rule_id) DO UPDATE SET "
            "state=EXCLUDED.state,checked_count=EXCLUDED.checked_count,"
            "failure_count=EXCLUDED.failure_count,failure_rate=EXCLUDED.failure_rate,"
            "threshold_percent=EXCLUDED.threshold_percent,checked_at=now()",
            (
                pipeline_run_id,
                source_name,
                rule_id,
                state,
                rows_checked,
                failed_count,
                round(failure_rate, 4),
                threshold,
            ),
        )

    cursor.execute(
        "INSERT INTO quality_run_summaries(pipeline_run_id,source_name,rows_checked,"
        "rows_accepted,rows_rejected,acceptance_rate,rejection_rate,overall_score) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT(pipeline_run_id) DO UPDATE SET "
        "source_name=EXCLUDED.source_name,rows_checked=EXCLUDED.rows_checked,"
        "rows_accepted=EXCLUDED.rows_accepted,rows_rejected=EXCLUDED.rows_rejected,"
        "acceptance_rate=EXCLUDED.acceptance_rate,rejection_rate=EXCLUDED.rejection_rate,"
        "overall_score=EXCLUDED.overall_score,checked_at=now()",
        (
            pipeline_run_id,
            source_name,
            rows_checked,
            len(accepted),
            len(rejected),
            summary["acceptance_rate"],
            summary["rejection_rate"],
            overall_score,
        ),
    )
    return summary


def persist_lineage(cursor, pipeline_run_id, source_name, rows_read, rows_accepted):
    edges = [
        (source_name, "bronze_sales", "source", "bronze", rows_read),
        ("bronze_sales", "silver_sales", "bronze", "silver", rows_accepted),
        ("silver_sales", "gold_sales_fact", "silver", "gold", rows_accepted),
    ]
    cursor.executemany(
        "INSERT INTO lineage_events(pipeline_run_id,source_name,from_dataset,to_dataset,"
        "from_layer,to_layer,rows_written) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        [
            (
                pipeline_run_id,
                source_name,
                from_dataset,
                to_dataset,
                from_layer,
                to_layer,
                count,
            )
            for from_dataset, to_dataset, from_layer, to_layer, count in edges
        ],
    )
