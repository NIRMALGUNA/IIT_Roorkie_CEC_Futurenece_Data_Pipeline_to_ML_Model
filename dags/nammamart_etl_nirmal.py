"""NammaMart validated ETL: parallel extract, quarantine, transform, DuckDB load."""
from __future__ import annotations

import json
import sys
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pendulum
from airflow.sdk import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_common import required_tasks_succeeded, assert_run_success

from nammamart_common import (
    DATA_DIR,
    SOURCE_FILES,
    WAREHOUSE,
    ensure_metadata_tables,
    insert_frame,
    mask_email,
    mask_phone,
    normalize_mode,
    source_rows,
    utc_now,
    validate_datasets,
    warehouse_lock,
    check_record,
)

DAG_ID = "nammamart_etl_nirmal"
DEFAULT_ARGS = {
    "owner": "nirmal",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "nirmal", "assignment"],
    description="Validated NammaMart ETL with quarantine, reconciled facts and run audit.",
)
def nammamart_etl_nirmal():
    @task
    def extract_orders() -> dict[str, Any]:
        rows = source_rows(DATA_DIR / SOURCE_FILES["orders"])
        print(f"[Extract] baseline orders={len(rows)}")
        return {"name": "orders", "rows": len(rows), "path": str(DATA_DIR / SOURCE_FILES["orders"])}

    @task
    def extract_payments() -> dict[str, Any]:
        rows = source_rows(DATA_DIR / SOURCE_FILES["payments"])
        print(f"[Extract] payments={len(rows)}")
        return {"name": "payments", "rows": len(rows), "path": str(DATA_DIR / SOURCE_FILES["payments"])}

    @task
    def extract_customers() -> dict[str, Any]:
        rows = source_rows(DATA_DIR / SOURCE_FILES["customers"])
        print(f"[Extract] customers={len(rows)}")
        return {"name": "customers", "rows": len(rows), "path": str(DATA_DIR / SOURCE_FILES["customers"])}

    @task
    def extract_products() -> dict[str, Any]:
        rows = source_rows(DATA_DIR / SOURCE_FILES["products"])
        print(f"[Extract] products={len(rows)}")
        return {"name": "products", "rows": len(rows), "path": str(DATA_DIR / SOURCE_FILES["products"])}

    @task
    def extract_stores() -> dict[str, Any]:
        rows = source_rows(DATA_DIR / SOURCE_FILES["stores"])
        print(f"[Extract] stores={len(rows)}")
        return {"name": "stores", "rows": len(rows), "path": str(DATA_DIR / SOURCE_FILES["stores"])}

    @task
    def extract_increment() -> dict[str, Any]:
        path = DATA_DIR / "orders_2026-10-01_increment.csv"
        rows = source_rows(path) if path.exists() else []
        print(f"[Extract] increment orders={len(rows)} (empty means not supplied)")
        return {"name": "orders_increment", "rows": len(rows), "path": str(path), "present": path.exists()}

    @task
    def validate_and_quarantine(*extract_inputs: dict[str, Any], **context: Any) -> dict[str, Any]:
        print(f"[Validate] Extract task manifests received: {len(extract_inputs)}")
        run_id = context["run_id"]
        try:
            result = validate_datasets(run_id, DAG_ID, include_increment=True)
        except Exception as exc:
            message = str(exc)
            source_name = next((name for name in [*SOURCE_FILES.values()] if name in message), "source_files")
            record = check_record(run_id, DAG_ID, f"{source_name}_schema", "Validity", "CRITICAL", 1, 1)
            with warehouse_lock():
                con = duckdb.connect(str(WAREHOUSE))
                try:
                    ensure_metadata_tables(con)
                    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name=?", [run_id, DAG_ID, record["check_name"]])
                    insert_frame(con, "etl_dq_failure", pd.DataFrame([record]), replace=True)
                    con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) SELECT * FROM etl_dq_failure")
                    con.execute("DROP TABLE etl_dq_failure")
                finally:
                    con.close()
            raise
        print(f"[Validate] checks={len(result['checks'])}; quarantined={result['quarantined']}")
        return result

    @task
    def transform(validated: dict[str, Any]) -> dict[str, str]:
        def read_pass(name: str) -> pd.DataFrame:
            path = validated["clean_paths"].get(name)
            return pd.read_csv(path, dtype=str, keep_default_na=False) if path else pd.DataFrame()

        orders = pd.concat([read_pass("orders"), read_pass("orders_increment")], ignore_index=True)
        if orders.empty:
            raise ValueError("No passed order rows are available")
        orders["_source_file"] = orders.get("_source_file", "orders.csv")
        if "_source_row_number" not in orders:
            orders["_source_row_number"] = range(2, len(orders) + 2)
        orders = orders.sort_values(["_source_file", "_source_row_number"], kind="stable")
        orders = orders.drop_duplicates("order_id", keep="last").copy()
        for column in ("order_status",):
            orders[column] = orders[column].astype(str).str.strip().str.upper()
        orders["payment_mode"] = orders["payment_mode"].map(normalize_mode)
        orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce", utc=True)
        for column in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"):
            orders[column] = pd.to_numeric(orders[column], errors="coerce")
        orders["order_date"] = orders["order_ts"].dt.date
        orders["order_amount"] = orders["order_amount"].map(
            lambda value: float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            if pd.notna(value) else value
        )
        orders["net_revenue"] = orders["order_amount"].where(orders["order_status"].eq("DELIVERED"), 0.0)
        orders["within_15_min"] = orders["delivery_minutes"].le(15).fillna(False) & orders["order_status"].eq("DELIVERED")

        customers = read_pass("customers").drop_duplicates("customer_id", keep="last")
        products = read_pass("products").drop_duplicates("product_id", keep="last")
        stores = read_pass("stores").drop_duplicates("store_id", keep="last")
        payments = read_pass("payments")
        if not customers.empty:
            customers = customers.assign(
                email=customers["email"].map(mask_email),
                phone=customers["phone"].map(mask_phone),
                loyalty_tier=customers["loyalty_tier"].astype(str).str.strip().str.upper(),
            )
            customers = customers.drop(columns=["full_name", "city", "area", "pincode", "_source_row_number", "failure_reason", "_severity"], errors="ignore")
        if not products.empty:
            products["category"] = products["category"].astype(str).str.strip().str.title()
            products["cost_price"] = pd.to_numeric(products["cost_price"], errors="coerce")
        if not stores.empty:
            stores["zone"] = stores["zone"].astype(str).str.strip().str.title()
            stores = stores.drop(columns=["manager_name", "area", "pincode", "_source_row_number", "failure_reason", "_severity"], errors="ignore")
        if not payments.empty:
            payments["payment_status"] = payments["payment_status"].astype(str).str.strip().str.upper()
            payments["payment_mode"] = payments["payment_mode"].map(normalize_mode)
            payments["amount_paid"] = pd.to_numeric(payments["amount_paid"], errors="coerce").round(2)

        if customers.empty or products.empty or stores.empty:
            raise ValueError("A required dimension has no passed rows")
        joined = orders.merge(customers, on="customer_id", how="inner", validate="many_to_one", suffixes=("", "_customer"))
        joined = joined.merge(products, on="product_id", how="inner", validate="many_to_one", suffixes=("", "_product"))
        joined = joined.merge(stores, on="store_id", how="inner", validate="many_to_one", suffixes=("", "_store"))
        joined["gross_margin"] = (joined["net_revenue"] - joined["quantity"] * joined["cost_price"]).round(2).where(joined["order_status"].eq("DELIVERED"), 0.0)
        fact = joined.drop(columns=["_source_row_number", "failure_reason", "_severity", "_source_file"], errors="ignore")
        all_orders = pd.concat([
            pd.read_csv(DATA_DIR / SOURCE_FILES["orders"], dtype=str, keep_default_na=False).assign(_source_file=SOURCE_FILES["orders"]),
            pd.read_csv(DATA_DIR / "orders_2026-10-01_increment.csv", dtype=str, keep_default_na=False).assign(_source_file="orders_2026-10-01_increment.csv")
            if (DATA_DIR / "orders_2026-10-01_increment.csv").exists() else pd.DataFrame(),
        ], ignore_index=True)
        all_orders["_source_order"] = range(len(all_orders))
        all_orders["order_status"] = all_orders["order_status"].astype(str).str.strip().str.upper()
        all_orders["payment_mode"] = all_orders["payment_mode"].map(normalize_mode)
        all_orders["order_amount"] = pd.to_numeric(all_orders["order_amount"], errors="coerce").round(2)
        all_orders["order_ts"] = pd.to_datetime(all_orders["order_ts"], errors="coerce", utc=True)
        all_orders["_increment_priority"] = all_orders["_source_file"].eq("orders_2026-10-01_increment.csv").astype(int)
        all_orders = all_orders.sort_values(["_increment_priority", "_source_order"], kind="stable").drop_duplicates("order_id", keep="last")
        manifest = {}
        for name, frame in (("fact", fact), ("customer", customers), ("product", products), ("store", stores), ("payment", payments), ("all_orders", all_orders)):
            path = Path(validated["clean_paths"]["orders"]).parent / f"etl_{name}.csv"
            frame.to_csv(path, index=False)
            manifest[name] = str(path)
        print(f"[Transform] current orders={len(fact)}; finance revenue orders={int(fact['order_status'].eq('DELIVERED').sum())}")
        return manifest

    @task
    def load(validated: dict[str, Any], manifest: dict[str, str]) -> dict[str, int]:
        frames = {name: pd.read_csv(path) for name, path in manifest.items()}
        con = None
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                con.execute("BEGIN TRANSACTION")
                for table, frame in (("etl_dim_customer", frames["customer"]), ("etl_dim_product", frames["product"]), ("etl_dim_store", frames["store"])):
                    insert_frame(con, table, frame, replace=True)
                # The validated current-state snapshot is an idempotent upsert:
                # increment corrections replace the previous order_id row.
                insert_frame(con, "etl_fact_orders", frames["fact"], replace=True)
                con.execute("CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (run_id VARCHAR, order_id VARCHAR, payment_ids VARCHAR, expected_amount DOUBLE, successful_amount DOUBLE, reason VARCHAR, checked_at TIMESTAMPTZ)")
                con.register("incoming_payments", frames["payment"])
                con.execute("CREATE OR REPLACE TEMP TABLE current_payments AS SELECT * FROM incoming_payments")
                con.register("all_current_orders", frames["all_orders"])
                con.execute("DELETE FROM etl_payment_reconciliation WHERE run_id = ?", [validated["run_id"]])
                con.execute("""
                    INSERT INTO etl_payment_reconciliation
                    SELECT ?, f.order_id, string_agg(p.payment_id, ','), f.order_amount,
                           sum(CASE WHEN upper(trim(p.payment_status))='SUCCESS' THEN p.amount_paid ELSE 0 END),
                           CASE WHEN count(p.payment_id)=0 AND f.order_status<>'CANCELLED' THEN 'missing payment'
                                WHEN f.order_status='CANCELLED' AND count_if(upper(trim(p.payment_status))='SUCCESS')>0 THEN 'cancelled order has successful payment'
                                WHEN f.order_status IN ('DELIVERED','RETURNED') AND abs(f.order_amount-sum(CASE WHEN upper(trim(p.payment_status))='SUCCESS' THEN p.amount_paid ELSE 0 END))>0.005 THEN 'successful payment total mismatch'
                                ELSE 'payment status/amount requires review' END,
                           current_timestamp
                    FROM all_current_orders f LEFT JOIN current_payments p USING(order_id)
                    GROUP BY f.order_id, f.order_amount, f.order_status
                    HAVING (count(p.payment_id)=0 AND f.order_status<>'CANCELLED')
                       OR (f.order_status='CANCELLED' AND count_if(upper(trim(p.payment_status))='SUCCESS')>0)
                       OR (f.order_status IN ('DELIVERED','RETURNED') AND abs(f.order_amount-sum(CASE WHEN upper(trim(p.payment_status))='SUCCESS' THEN p.amount_paid ELSE 0 END))>0.005)
                       OR count_if(upper(trim(p.payment_status))='REFUNDED')>0
                """, [validated["run_id"]])
                orphan_rows = []
                known_ids = set(frames["all_orders"]["order_id"].astype(str).str.strip())
                # Match the payment-ID winner policy used by clean payment staging.
                payment_winners = {}
                for raw_payment in source_rows(DATA_DIR / SOURCE_FILES["payments"]):
                    payment_winners[str(raw_payment.get("payment_id") or "").strip()] = raw_payment
                for record in payment_winners.values():
                    key = str(record.get("order_id") or "").strip()
                    if key not in known_ids:
                        orphan_rows.append((validated["run_id"], key, record.get("payment_id"),
                                            None, pd.to_numeric(record.get("amount_paid"), errors="coerce")
                                            if str(record.get("payment_status", "")).strip().upper() == "SUCCESS" else 0.0,
                                            "payment has no matching order", utc_now()))
                if orphan_rows:
                    con.executemany("INSERT INTO etl_payment_reconciliation (run_id, order_id, payment_ids, expected_amount, successful_amount, reason, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?)", orphan_rows)
                con.execute("CREATE TABLE IF NOT EXISTS etl_rejected_records (run_id VARCHAR, file VARCHAR, row_key VARCHAR, failure_reason VARCHAR, source_row_number BIGINT, record_json VARCHAR)")
                rejected_rows = []
                for source_file, path in validated.get("quarantine_paths", {}).items():
                    rejected_frame = pd.read_csv(path, dtype=str, keep_default_na=False)
                    key_column = next((column for column in ("order_id", "payment_id", "customer_id", "product_id", "store_id") if column in rejected_frame.columns), "")
                    for record in rejected_frame.to_dict(orient="records"):
                        safe_record = dict(record)
                        if source_file == "customers":
                            safe_record["email"] = mask_email(safe_record.get("email"))
                            safe_record["phone"] = mask_phone(safe_record.get("phone"))
                            for personal_field in ("full_name", "city", "area", "pincode"):
                                safe_record.pop(personal_field, None)
                        elif source_file == "stores":
                            safe_record.pop("manager_name", None)
                        rejected_rows.append({
                            "run_id": validated["run_id"],
                            "file": SOURCE_FILES.get(source_file, source_file),
                            "row_key": record.get(key_column, "") if key_column else "",
                            "failure_reason": record.get("failure_reason", ""),
                            "source_row_number": int(record["_source_row_number"]) if str(record.get("_source_row_number", "")).isdigit() else None,
                            "record_json": json.dumps(safe_record, ensure_ascii=False),
                        })
                rejected_frame = pd.DataFrame(rejected_rows, columns=["run_id", "file", "row_key", "failure_reason", "source_row_number", "record_json"])
                con.execute("DELETE FROM etl_rejected_records WHERE run_id=?", [validated["run_id"]])
                if not rejected_frame.empty:
                    insert_frame(con, "etl_rejected_records", rejected_frame)
                checks = pd.DataFrame(validated["checks"])
                con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=?", [validated["run_id"], DAG_ID])
                con.register("new_dq_results", checks)
                con.execute("""
                    INSERT INTO dq_results (
                        run_id, dag_id, check_name, dimension, severity,
                        rows_checked, rows_failed, status, checked_at
                    )
                    SELECT
                        run_id, dag_id, check_name, dimension, severity,
                        rows_checked, rows_failed, status, checked_at
                    FROM new_dq_results
                """)
                row_count = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
                con.unregister("incoming_payments")
                con.unregister("all_current_orders")
                con.execute("COMMIT")
            finally:
                con.close()
        return {"rows_loaded": int(row_count)}

    @task
    def report() -> dict[str, int]:
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE), read_only=False)
            try:
                con.execute("""
                    CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                    SELECT order_date, zone, store_id, store_name, sum(net_revenue) AS net_revenue,
                           count(*) FILTER (WHERE order_status='DELIVERED') AS delivered_orders
                    FROM etl_fact_orders GROUP BY ALL
                """)
                con.execute("""
                    CREATE OR REPLACE TABLE etl_category_margin AS
                    SELECT category, sum(net_revenue) AS revenue,
                           sum(net_revenue - quantity * cost_price) AS gross_margin
                    FROM etl_fact_orders WHERE order_status='DELIVERED' GROUP BY category
                """)
                print("[Report] Daily revenue:")
                print(con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id").fetchdf().to_string(index=False))
                print("[Report] Category margin:")
                print(con.execute("SELECT * FROM etl_category_margin ORDER BY category").fetchdf().to_string(index=False))
                return {"rows_reported": int(con.execute("SELECT count(*) FROM etl_daily_zone_revenue").fetchone()[0])}
            finally:
                con.close()

    @task(trigger_rule="all_done")
    def audit(**context: Any) -> dict[str, str]:
        ti = context["ti"]
        run_id = context["run_id"]
        start = context["dag_run"].start_date or utc_now()
        validation = ti.xcom_pull(task_ids="validate_and_quarantine") or {}
        load_result = ti.xcom_pull(task_ids="load") or {}
        extracts = [ti.xcom_pull(task_ids=f"extract_{name}") or {} for name in ("orders", "payments", "customers", "products", "stores", "increment")]
        extracted = int(validation.get("extracted", sum(item.get("rows", 0) for item in extracts)))
        quarantined = int(validation.get("quarantined", 0))
        loaded = int(load_result.get("rows_loaded", 0))
        required_ids = ("extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores", "extract_increment", "validate_and_quarantine", "transform", "load", "report")
        failed = not required_tasks_succeeded(context, DAG_ID, required_ids)
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, DAG_ID])
                con.execute("INSERT INTO pipeline_runs (run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, start, utc_now(), extracted, loaded, quarantined, "FAILED" if failed else "SUCCESS"])
            finally:
                con.close()
        return {"status": "FAILED" if failed else "SUCCESS"}

    @task(trigger_rule="all_done", retries=0)
    def verify_run(**context: Any) -> None:
        assert_run_success(context, DAG_ID)

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    increment = extract_increment()
    validation = validate_and_quarantine(orders, payments, customers, products, stores, increment)
    transformed = transform(validation)
    loaded = load(validation, transformed)
    reported = report()
    loaded >> reported
    audit_task = audit()
    for upstream in (orders, payments, customers, products, stores, increment, validation, transformed, loaded, reported):
        upstream >> audit_task
    audit_task >> verify_run()


nammamart_etl_nirmal()
