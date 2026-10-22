"""NammaMart SQL ELT: append-only varchar raw history, current staging and marts."""
from __future__ import annotations

import csv
import re
import sys
from datetime import timedelta
from typing import Any

import duckdb
import pendulum
from airflow.sdk import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_common import required_tasks_succeeded, assert_run_success

from nammamart_common import DATA_DIR, RULES, SQL_DIR, WAREHOUSE, ensure_metadata_tables, utc_now, warehouse_lock

DAG_ID = "nammamart_elt_nirmal"
DEFAULT_ARGS = {
    "owner": "nirmal",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
}
RAW_INPUTS = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
}
EXPECTED_SOURCE_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
    "orders_increment": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
}
METADATA_COLUMNS = ("_loaded_at", "_source_file", "_source_row_number", "_run_id")


def quote_identifier(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise ValueError(f"Unsafe CSV column name: {identifier}")
    return '"' + identifier.replace('"', '""') + '"'


def execute_sql_file(con: duckdb.DuckDBPyConnection, filename: str, run_id: str) -> None:
    sql = (SQL_DIR / filename).read_text(encoding="utf-8")
    orders = RULES["orders"]
    escaped = lambda value: "'" + str(value).replace("'", "''") + "'"
    replacements = {
        "__RUN_ID__": run_id.replace("'", "''"),
        "__VALID_STATUSES__": ", ".join(escaped(value) for value in orders["valid_statuses"]),
        "__VALID_PAYMENT_MODES__": ", ".join(escaped(value) for value in orders["valid_payment_modes"]),
        "__REVENUE_STATUS__": escaped(orders["revenue_status"]),
        "__DELIVERY_CRITICAL__": str(orders["critical_delivery_minutes"]),
        "__NORMAL_DELIVERY_MIN__": str(orders["normal_delivery_minutes_min"]),
        "__NORMAL_DELIVERY_MAX__": str(orders["normal_delivery_minutes_max"]),
        "__MAX_AGE_DAYS__": str(RULES["freshness"]["maximum_order_age_days"]),
        "__PAY_TOLERANCE__": str(RULES["money"]["payment_tolerance"]),
        "__SUCCESS_STATUS__": escaped(RULES["payments"]["successful_status"]),
        "__PAYMENT_STATUSES__": ", ".join(escaped(value) for value in RULES["payments"]["recognized_statuses"]),
        "__LOYALTY_TIERS__": ", ".join(escaped(value) for value in orders["loyalty_tiers"]),
        "__ZONES__": ", ".join(escaped(value) for value in orders["zones"]),
        "__PRODUCT_CATEGORIES__": ", ".join(escaped(value) for value in orders["product_categories"]),
    }
    for marker, value in replacements.items():
        sql = sql.replace(marker, value)
    con.execute("SET TimeZone = 'UTC'")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(sql)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "nirmal", "assignment"],
    description="Append every source row to varchar raw tables, then transform in DuckDB SQL.",
)
def nammamart_elt_nirmal():
    @task
    def load_raw(dataset: str, source_file: str, **context: Any) -> dict[str, Any]:
        path = DATA_DIR / source_file
        if not path.exists():
            if dataset == "orders_increment":
                print("[Raw] Increment source is absent; no rows appended")
                return {"dataset": dataset, "rows": 0, "present": False}
            raise FileNotFoundError(path)
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle), [])
        if not header or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item) for item in header):
            run_id = context["run_id"]
            with warehouse_lock():
                con = duckdb.connect(str(WAREHOUSE))
                try:
                    ensure_metadata_tables(con)
                    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name=?", [run_id, DAG_ID, f"{source_file}_schema"])
                    con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, f"{source_file}_schema", "Validity", "CRITICAL", 0, 1, "FAIL", utc_now()])
                finally:
                    con.close()
            raise ValueError(f"Missing or unsupported source header in {source_file}")
        table = f"raw_{'orders' if dataset == 'orders_increment' else dataset}"
        run_id = context["run_id"]
        if header != EXPECTED_SOURCE_COLUMNS[dataset]:
            with warehouse_lock():
                con = duckdb.connect(str(WAREHOUSE))
                try:
                    ensure_metadata_tables(con)
                    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name=?", [run_id, DAG_ID, f"{source_file}_schema"])
                    con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, f"{source_file}_schema", "Validity", "CRITICAL", len(EXPECTED_SOURCE_COLUMNS[dataset]), 1, "FAIL", utc_now()])
                finally:
                    con.close()
            raise ValueError(f"Schema drift in {source_file}: expected {EXPECTED_SOURCE_COLUMNS[dataset]}, found {header}")
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            source_row_count = sum(1 for _ in csv.DictReader(handle))
        source_columns = ", ".join(quote_identifier(column) for column in header)
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                columns = ", ".join(f"{quote_identifier(column)} VARCHAR" for column in header)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns}, _loaded_at TIMESTAMPTZ, _source_file VARCHAR, _source_row_number BIGINT, _run_id VARCHAR)")
                existing = [row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
                expected = header + list(METADATA_COLUMNS)
                if existing != expected:
                    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name=?", [run_id, DAG_ID, f"{source_file}_schema"])
                    con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, f"{source_file}_schema", "Validity", "CRITICAL", len(expected), 1, "FAIL", utc_now()])
                    raise ValueError(f"Schema drift for {source_file}: expected {expected}, found {existing}")
                template = (SQL_DIR / "raw_append.sql").read_text(encoding="utf-8")
                escaped_path = path.resolve().as_posix().replace("'", "''")
                escaped_file = source_file.replace("'", "''")
                sql = template.replace("{{TABLE_NAME}}", table).replace("{{SOURCE_COLUMNS}}", source_columns)
                sql = sql.replace("{{SOURCE_COLUMN_NAMES}}", ", ".join("'" + column.replace("'", "''") + "'" for column in header))
                sql = sql.replace("{{SOURCE_FILE}}", escaped_file).replace("{{ABSOLUTE_PATH}}", escaped_path)
                sql = sql.replace("{{RUN_ID}}", run_id.replace("'", "''"))
                con.execute("BEGIN TRANSACTION")
                try:
                    con.execute(sql)
                    count = con.execute("SELECT count(*) FROM raw_orders" if table == "raw_orders" else f"SELECT count(*) FROM {table}").fetchone()[0]
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
            finally:
                con.close()
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                expected_schema_size = len(header) + len(METADATA_COLUMNS)
                con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name=?", [run_id, DAG_ID, f"{source_file}_schema"])
                con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, f"{source_file}_schema", "Validity", "CRITICAL", expected_schema_size, 0, "PASS", utc_now()])
                if dataset == "orders":
                    volume_failed = int(source_row_count < 241)
                    con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name='baseline_order_volume'", [run_id, DAG_ID])
                    con.execute("INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, "baseline_order_volume", "Completeness", "WARNING", source_row_count, volume_failed, "WARN" if volume_failed else "PASS", utc_now()])
            finally:
                con.close()
        print(f"[Raw] appended source={source_file} to {table}; current raw history rows={count}")
        return {"dataset": dataset, "rows": source_row_count, "present": True, "source_file": source_file}

    @task
    def stage_dimensions(*raw_results: dict[str, Any], **context: Any) -> dict[str, int]:
        run_id = context["run_id"]
        print(f"[Raw] Layer completed for {len(raw_results)} source inputs")
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name IN ('customer_required_fields','customer_key_unique','customer_format','customer_signup_valid','customer_signup_not_future','product_price_cost','store_zone','payment_fields','product_id_unique','store_id_unique','payment_id_unique')", [run_id, DAG_ID])
                execute_sql_file(con, "stg_dimensions.sql", run_id)
                for table in ("stg_customers", "stg_products", "stg_stores", "stg_payments"):
                    print(f"[Staging] {table}={con.execute(f'SELECT count(*) FROM {table}').fetchone()[0]}")
                return {table: int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]) for table in ("stg_customers", "stg_products", "stg_stores", "stg_payments")}
            finally:
                con.close()

    @task
    def stage_orders(dimensions: dict[str, int], **context: Any) -> dict[str, int]:
        run_id = context["run_id"]
        print(f"[Staging] Dimension row counts: {dimensions}")
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=? AND check_name LIKE 'order_%'", [run_id, DAG_ID])
                execute_sql_file(con, "stg_orders.sql", run_id)
                merge_stats = con.execute("SELECT inserted_orders, matched_orders, changed_status_orders FROM elt_order_merge_audit WHERE run_id=?", [run_id]).fetchone()
                print(f"[MERGE] inserted={merge_stats[0]}, matched={merge_stats[1]}, changed_status={merge_stats[2]}")
                stats = con.execute("SELECT count(*), count(*) FILTER (WHERE dq_status='FAIL'), count(*) FILTER (WHERE dq_status='WARN') FROM stg_orders").fetchone()
                print(f"[Staging] current orders={stats[0]}, critical failures={stats[1]}, warnings={stats[2]}")
                return {"rows": int(stats[0]), "failed": int(stats[1]), "warnings": int(stats[2])}
            finally:
                con.close()

    @task
    def build_marts(orders: dict[str, int], **context: Any) -> dict[str, int]:
        run_id = context["run_id"]
        print(f"[Marts] Staged order summary: {orders}")
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                execute_sql_file(con, "marts.sql", run_id)
                totals = con.execute("SELECT count(*), coalesce(sum(net_revenue),0) FROM mart_daily_store_kpis").fetchone()
                current_orders = con.execute("SELECT count(*) FROM stg_orders WHERE dq_status<>'FAIL'").fetchone()[0]
                rejected_rows = con.execute("SELECT count(*) FROM mart_rejected_records").fetchone()[0]
                print(f"[Marts] daily store rows={totals[0]}, net revenue={totals[1]:.2f}")
                return {"rows_loaded": int(current_orders), "rows_quarantined": int(rejected_rows), "revenue": float(totals[1])}
            finally:
                con.close()

    @task(trigger_rule="all_done")
    def audit(**context: Any) -> dict[str, str]:
        dag_run = context["dag_run"]
        task_ids = tuple(f"load_raw_{name}" for name in RAW_INPUTS) + ("stage_dimensions", "stage_orders", "build_marts")
        failed = not required_tasks_succeeded(context, DAG_ID, task_ids)
        run_id = context["run_id"]
        raw_results = [context["ti"].xcom_pull(task_ids=f"load_raw_{name}") or {} for name in RAW_INPUTS]
        rows_extracted = sum(int(result.get("rows", 0)) for result in raw_results)
        stage_result = context["ti"].xcom_pull(task_ids="stage_orders") or {}
        mart_result = context["ti"].xcom_pull(task_ids="build_marts") or {}
        start = dag_run.start_date or utc_now()
        with warehouse_lock():
            con = duckdb.connect(str(WAREHOUSE))
            try:
                ensure_metadata_tables(con)
                con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [run_id, DAG_ID])
                con.execute("INSERT INTO pipeline_runs (run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [run_id, DAG_ID, start, utc_now(), rows_extracted, int(mart_result.get("rows_loaded", 0)), int(mart_result.get("rows_quarantined", stage_result.get("failed", 0))), "FAILED" if failed else "SUCCESS"])
            finally:
                con.close()
        return {"status": "FAILED" if failed else "SUCCESS"}

    @task(trigger_rule="all_done", retries=0)
    def verify_run(**context: Any) -> None:
        assert_run_success(context, DAG_ID)

    raw_tasks = [
        load_raw.override(task_id=f"load_raw_{name}")(name, filename)
        for name, filename in RAW_INPUTS.items()
    ]
    dimensions = stage_dimensions(*raw_tasks)
    orders = stage_orders(dimensions)
    marts = build_marts(orders)
    audit_task = audit()
    for task_result in (*raw_tasks, dimensions, orders, marts):
        task_result >> audit_task
    audit_task >> verify_run()


nammamart_elt_nirmal()
