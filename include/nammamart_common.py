"""Shared NammaMart rules, profiling utilities, and DuckDB safety helpers."""
from __future__ import annotations

import csv
import json
import os
import re
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Generator

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "include" / "data" / "nammamart"
WAREHOUSE = ROOT / "include" / "warehouse_nirmal.duckdb"
RULES = json.loads((ROOT / "include" / "nammamart_rules.json").read_text(encoding="utf-8"))
SQL_DIR = ROOT / "include" / "sql"
QUARANTINE_DIR = ROOT / "include" / "quarantine" / "nirmal"
TMP_DIR = ROOT / "include" / "tmp" / "nammamart"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_decimal(value: Any) -> Decimal | None:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


def money(value: Any) -> Decimal | None:
    number = as_decimal(value)
    return number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) if number is not None else None


def normalize_mode(value: Any) -> str:
    token = str(value or "").strip().upper()
    return RULES["orders"]["payment_mode_aliases"].get(token, token)


def mask_email(value: Any) -> str | None:
    text = str(value).strip() if value is not None and not pd.isna(value) else ""
    if not text or "@" not in text:
        return None
    local, domain = text.split("@", 1)
    return f"{local[:1]}***@{domain}" if local else f"***@{domain}"


def mask_phone(value: Any) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) < 4:
        return None
    return f"{digits[:2]}{'*' * max(0, len(digits) - 4)}{digits[-2:]}"


def source_rows(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row, _source_row_number=index) for index, row in enumerate(csv.DictReader(handle), start=2)]


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fieldnames or (list(rows[0]) if rows else [])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def acquire_file_lock(path: Path, timeout_seconds: int = 120, stale_after_seconds: int = 3600) -> None:
    """Acquire a cross-process exclusive lock without extra packages."""
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            descriptor = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"pid": os.getpid(), "created_at": utc_now().isoformat()}))
            return
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
                if age > stale_after_seconds:
                    path.unlink(missing_ok=True)
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for DuckDB writer lock: {path}")
            time.sleep(0.25)


@contextmanager
def warehouse_lock() -> Generator[None, None, None]:
    lock = WAREHOUSE.with_suffix(WAREHOUSE.suffix + ".writer.lock")
    acquire_file_lock(lock)
    try:
        yield
    finally:
        lock.unlink(missing_ok=True)


def make_run_directory(run_id: str) -> Path:
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    path = TMP_DIR / safe_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_quarantine(file_name: str, rows: list[dict[str, Any]], run_id: str) -> Path | None:
    if not rows:
        return None
    safe_id = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
    path = QUARANTINE_DIR / f"{Path(file_name).stem}_failed_{safe_id}.csv"
    fields = list(dict.fromkeys(key for row in rows for key in row))
    if "failure_reason" not in fields:
        fields.append("failure_reason")
    write_csv(path, rows, fields)
    return path


def check_record(run_id: str, dag_id: str, check_name: str, dimension: str,
                 severity: str, checked: int, failed: int) -> dict[str, Any]:
    status = "PASS" if failed == 0 else ("WARN" if severity == "WARNING" else "FAIL")
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "check_name": check_name,
        "dimension": dimension,
        "severity": severity,
        "rows_checked": int(checked),
        "rows_failed": int(failed),
        "status": status,
        "checked_at": utc_now(),
    }


def issue(row: dict[str, Any], reason: str, severity: str = "CRITICAL") -> None:
    existing = [part for part in str(row.get("failure_reason", "")).split("; ") if part]
    if reason not in existing:
        existing.append(reason)
    row["failure_reason"] = "; ".join(existing)
    row["_severity"] = max((row.get("_severity", "INFO"), severity), key={"INFO": 0, "WARNING": 1, "CRITICAL": 2}.get)


def validate_unique(rows: list[dict[str, Any]], filename: str, key: str,
                    checks: list[dict[str, Any]], *, deduplicate: bool) -> list[dict[str, Any]]:
    last: dict[str, int] = defaultdict(int)
    for index, row in enumerate(rows):
        last[str(row.get(key) or "").strip()] = index
    failed = 0
    for index, row in enumerate(rows):
        value = str(row.get(key) or "").strip()
        if not value:
            issue(row, f"missing primary key {key}")
            failed += 1
        elif last[value] != index:
            issue(row, f"duplicate {key}; later physical source row wins")
            failed += 1
    checks.append(check_record("", "", f"{filename}_primary_key_unique", "Uniqueness", "CRITICAL", len(rows), failed))
    return [row for index, row in enumerate(rows) if not deduplicate or not str(row.get(key) or "").strip() or last[str(row.get(key) or "").strip()] == index]


def validate_datasets(run_id: str, dag_id: str, include_increment: bool = True) -> dict[str, Any]:
    """Apply the shared six-dimension rules to source rows; return JSON-safe manifests."""
    source = {
        name: source_rows(DATA_DIR / file_name)
        for name, file_name in SOURCE_FILES.items()
        if name != "orders_increment" or (include_increment and (DATA_DIR / file_name).exists())
    }
    checks: list[dict[str, Any]] = []
    by_file: dict[str, list[dict[str, Any]]] = {}
    known_customers = {str(row.get("customer_id", "")).strip() for row in source["customers"]}
    known_products = {str(row.get("product_id", "")).strip() for row in source["products"]}
    known_stores = {str(row.get("store_id", "")).strip() for row in source["stores"]}
    known_order_ids = {str(row.get("order_id", "")).strip() for row in source["orders"]}
    known_order_ids.update(str(row.get("order_id", "")).strip() for row in source.get("orders_increment", []))

    expected_columns = {
        "orders.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
        "payments.csv": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
        "customers.csv": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
        "products.csv": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
        "stores.csv": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
        "orders_2026-10-01_increment.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    }
    for file_name, rows in source.items():
        columns = [column for column in rows[0] if column != "_source_row_number"] if rows else []
        expected = expected_columns[SOURCE_FILES[file_name] if file_name != "orders_increment" else "orders_2026-10-01_increment.csv"]
        failed = int(columns != expected)
        checks.append(check_record(run_id, dag_id, f"{file_name}_schema", "Validity", "CRITICAL", len(expected), failed))
        if failed:
            raise ValueError(f"Schema drift in {file_name}: expected {expected}, found {columns}")

    # Validate dimensions first. Keep last-source winner only; critical values are separately gated.
    for name, key in (("customers", "customer_id"), ("products", "product_id"), ("stores", "store_id"), ("payments", "payment_id")):
        rows = source[name]
        for row in rows:
            row["failure_reason"] = ""
            row["_severity"] = "INFO"
        rows = validate_unique(rows, name, key, checks, deduplicate=True)
        checks[-1].update(run_id=run_id, dag_id=dag_id)
        # Uniqueness check covers physical input; duplicate losers remain in source history/quarantine evidence.
        if name == "customers":
            for row in rows:
                if not str(row.get("city") or "").strip():
                    issue(row, "city is missing", "WARNING")
                if str(row.get("loyalty_tier") or "").strip().upper() not in {"BRONZE", "SILVER", "GOLD", "PLATINUM"}:
                    issue(row, "loyalty_tier is not recognized", "WARNING")
                phone = re.sub(r"\D", "", str(row.get("phone") or ""))
                if phone and len(phone) != 10:
                    issue(row, "phone is not ten digits", "WARNING")
                email = str(row.get("email") or "").strip()
                if email and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
                    issue(row, "email format is invalid", "WARNING")
                if str(row.get("pincode") or "").strip() and not re.fullmatch(r"[1-9][0-9]{5}", str(row["pincode"]).strip()):
                    issue(row, "pincode is not six valid digits", "WARNING")
                signup = pd.to_datetime(row.get("signup_date"), errors="coerce", utc=True)
                if pd.isna(signup):
                    issue(row, "signup_date is missing or invalid")
                elif signup.date() > utc_now().date():
                    issue(row, "signup_date is in the future")
        elif name == "products":
            for row in rows:
                category = str(row.get("category") or "").strip().upper()
                mrp, cost = as_decimal(row.get("mrp")), as_decimal(row.get("cost_price"))
                if category not in {"DAIRY & BAKERY", "FRUITS & VEGETABLES", "HOUSEHOLD", "PERSONAL CARE", "SNACKS & BEVERAGES", "STAPLES"}:
                    issue(row, "category is missing or invalid")
                if mrp is None or mrp <= 0:
                    issue(row, "mrp must be positive")
                if cost is None or (mrp is not None and cost > mrp):
                    issue(row, "cost_price is missing or exceeds mrp")
        elif name == "stores":
            for row in rows:
                if str(row.get("zone") or "").strip().upper() not in {"NORTH", "SOUTH", "EAST", "WEST"}:
                    issue(row, "zone is missing or invalid")
                capacity = as_decimal(row.get("capacity_orders_per_day"))
                if capacity is None or capacity <= 0:
                    issue(row, "capacity_orders_per_day must be positive", "WARNING")
        elif name == "payments":
            for row in rows:
                status = str(row.get("payment_status") or "").strip().upper()
                amount = as_decimal(row.get("amount_paid"))
                if str(row.get("order_id") or "").strip() not in known_order_ids:
                    issue(row, "payment order_id has no matching order", "CRITICAL")
                if status not in set(RULES["payments"]["recognized_statuses"]):
                    issue(row, "payment_status is invalid", "WARNING")
                if amount is None:
                    issue(row, "amount_paid is missing or invalid")
                elif amount < 0:
                    issue(row, "amount_paid is negative")
                if not str(row.get("gateway_ref") or "").strip():
                    issue(row, "gateway_ref is missing", "WARNING")
                if pd.isna(pd.to_datetime(row.get("payment_ts"), errors="coerce", utc=True)):
                    issue(row, "payment_ts is missing or invalid")
        by_file[name] = rows

    # A raw master key is not sufficient when its latest dimension row failed a critical rule.
    known_customers = {str(row.get("customer_id", "")).strip() for row in by_file["customers"] if row.get("_severity") != "CRITICAL"}
    known_products = {str(row.get("product_id", "")).strip() for row in by_file["products"] if row.get("_severity") != "CRITICAL"}
    known_stores = {str(row.get("store_id", "")).strip() for row in by_file["stores"] if row.get("_severity") != "CRITICAL"}

    # Build current order state: baseline then increment; last physical duplicate per file wins.
    order_sources: list[tuple[str, list[dict[str, Any]]]] = [("orders", source["orders"])]
    if "orders_increment" in source:
        order_sources.append(("orders_increment", source["orders_increment"]))
    current: dict[str, dict[str, Any]] = {}
    original_rows: dict[str, dict[str, Any]] = {}
    for name, raw_rows in order_sources:
        rows = [dict(row, failure_reason="", _severity="INFO", _source_file=SOURCE_FILES["orders"] if name == "orders" else "orders_2026-10-01_increment.csv") for row in raw_rows]
        last: dict[str, int] = {}
        for index, row in enumerate(rows):
            last[str(row.get("order_id") or "").strip()] = index
        duplicate_failed = 0
        for index, row in enumerate(rows):
            order_id = str(row.get("order_id") or "").strip()
            if not order_id:
                issue(row, "missing primary key order_id")
                duplicate_failed += 1
            elif last[order_id] != index:
                issue(row, "duplicate order_id; later physical source row wins")
                duplicate_failed += 1
        checks.append(check_record(run_id, dag_id, f"{name}_order_id_unique", "Uniqueness", "CRITICAL", len(rows), duplicate_failed))
        for index, row in enumerate(rows):
            order_id = str(row.get("order_id") or "").strip()
            if order_id and last[order_id] == index:
                current[order_id] = row
                original_rows[order_id] = row

    # Current-state order checks, including dimensions and cross-source reconciliation.
    payments_by_order: dict[str, list[dict[str, Any]]] = {}
    for row in by_file["payments"]:
        if row.get("_severity") == "CRITICAL":
            continue
        payments_by_order.setdefault(str(row.get("order_id") or "").strip(), []).append(row)
    order_failed_ids = {dimension: set() for dimension in ("Completeness", "Validity", "Accuracy", "Consistency", "Timeliness")}
    stale_order_count = 0
    for order_id, row in current.items():
        status = str(row.get("order_status") or "").strip().upper()
        if any(not str(row.get(field) or "").strip() for field in ("order_ts", "customer_id", "store_id", "product_id")):
            issue(row, "required order field is missing")
            order_failed_ids["Completeness"].add(order_id)
        if status not in set(RULES["orders"]["valid_statuses"]):
            issue(row, "order_status is invalid")
            order_failed_ids["Validity"].add(order_id)
        mode = normalize_mode(row.get("payment_mode"))
        row["payment_mode"] = mode
        if mode not in set(RULES["orders"]["valid_payment_modes"]):
            issue(row, "payment_mode is invalid")
            order_failed_ids["Validity"].add(order_id)
        quantity, unit_price, discount, amount = (as_decimal(row.get(column)) for column in ("quantity", "unit_price", "discount_pct", "order_amount"))
        if quantity is None or unit_price is None or discount is None or amount is None:
            issue(row, "order measure is missing or non-numeric")
            order_failed_ids["Completeness"].add(order_id)
        else:
            if quantity <= 0 or not Decimal("0") <= discount <= Decimal("100"):
                issue(row, "quantity must be positive and discount_pct must be 0..100")
                order_failed_ids["Accuracy"].add(order_id)
            expected = money(quantity * unit_price * (Decimal("1") - discount / Decimal("100")))
            if money(amount) != expected:
                issue(row, f"order_amount formula mismatch; expected {expected}")
                order_failed_ids["Accuracy"].add(order_id)
        if status == "DELIVERED":
            minutes = as_decimal(row.get("delivery_minutes"))
            if minutes is None:
                issue(row, "DELIVERED order requires delivery_minutes")
                order_failed_ids["Completeness"].add(order_id)
            elif minutes >= Decimal(str(RULES["orders"]["critical_delivery_minutes"])):
                issue(row, "delivery_minutes is at least 120")
                order_failed_ids["Accuracy"].add(order_id)
            elif minutes < 8 or minutes > 35:
                issue(row, "delivery_minutes is outside normal 8..35 minute range", "WARNING")
        if str(row.get("customer_id") or "").strip() not in known_customers or str(row.get("product_id") or "").strip() not in known_products or str(row.get("store_id") or "").strip() not in known_stores:
            issue(row, "customer_id, product_id or store_id is not found in its master")
            order_failed_ids["Consistency"].add(order_id)
        timestamp = pd.to_datetime(row.get("order_ts"), errors="coerce", utc=True)
        if pd.isna(timestamp):
            issue(row, "order_ts is missing or unparseable")
            order_failed_ids["Validity"].add(order_id)
        elif timestamp.to_pydatetime() > utc_now():
            issue(row, "order_ts is in the future")
            order_failed_ids["Timeliness"].add(order_id)
        elif (utc_now().date() - timestamp.date()).days > int(RULES["freshness"]["maximum_order_age_days"]):
            issue(row, "order feed is historically stale", "WARNING")
            stale_order_count += 1
        successes = [p for p in payments_by_order.get(order_id, []) if str(p.get("payment_status") or "").strip().upper() == "SUCCESS"]
        success_total = sum((as_decimal(p.get("amount_paid")) or Decimal("0") for p in successes), Decimal("0"))
        if (status == "CANCELLED" and successes) or (status in {"DELIVERED", "RETURNED"} and (not successes or amount is None or money(success_total) != money(amount))):
            issue(row, "successful payment total is missing or differs from order_amount")
            order_failed_ids["Consistency"].add(order_id)
    checks.append(check_record(run_id, dag_id, "order_required_fields", "Completeness", "CRITICAL", len(current), len(order_failed_ids["Completeness"])))
    checks.append(check_record(run_id, dag_id, "order_allowed_values", "Validity", "CRITICAL", len(current), len(order_failed_ids["Validity"])))
    checks.append(check_record(run_id, dag_id, "order_formula_and_delivery", "Accuracy", "CRITICAL", len(current), len(order_failed_ids["Accuracy"])))
    checks.append(check_record(run_id, dag_id, "order_master_and_payment_reconciliation", "Consistency", "CRITICAL", len(current), len(order_failed_ids["Consistency"])))
    checks.append(check_record(run_id, dag_id, "order_future_timestamp", "Timeliness", "CRITICAL", len(current), len(order_failed_ids["Timeliness"])))
    checks.append(check_record(run_id, dag_id, "order_feed_freshness", "Timeliness", "WARNING", len(current), stale_order_count))

    baseline_count = len(source.get("orders", []))
    checks.append(check_record(run_id, dag_id, "baseline_order_volume", "Completeness", "WARNING", baseline_count, int(baseline_count < 241)))
    store_prices: dict[str, list[Decimal]] = defaultdict(list)
    all_prices: list[Decimal] = []
    for row in (item for _, items in order_sources for item in items):
        price = as_decimal(row.get("unit_price"))
        if price is not None and price > 0:
            all_prices.append(price)
            store_prices[str(row.get("store_id") or "")].append(price)
    if all_prices:
        global_median = sorted(all_prices)[len(all_prices) // 2]
        drifted_stores = sum(
            (sum(prices, Decimal("0")) / len(prices) > global_median * Decimal("3"))
            or (sum(prices, Decimal("0")) / len(prices) < global_median / Decimal("3"))
            for prices in store_prices.values()
        )
    else:
        drifted_stores = 0
    checks.append(check_record(run_id, dag_id, "unit_price_by_store_distribution", "Accuracy", "WARNING", len(store_prices), drifted_stores))

    # Add missing-dimension checks, then build rejected and clean worksets.
    checks.append(check_record(run_id, dag_id, "customer_optional_location", "Completeness", "WARNING", len(by_file["customers"]), sum("city is missing" in str(r.get("failure_reason")) for r in by_file["customers"])))
    checks.append(check_record(run_id, dag_id, "customer_contact_and_tier_format", "Validity", "WARNING", len(by_file["customers"]), sum(r.get("_severity") == "WARNING" for r in by_file["customers"])))
    checks.append(check_record(run_id, dag_id, "customer_signup_date_valid", "Validity", "CRITICAL", len(by_file["customers"]), sum("signup_date is missing or invalid" in str(r.get("failure_reason", "")) for r in by_file["customers"])))
    checks.append(check_record(run_id, dag_id, "customer_signup_not_future", "Timeliness", "CRITICAL", len(by_file["customers"]), sum("signup_date is in the future" in str(r.get("failure_reason", "")) for r in by_file["customers"])))
    checks.append(check_record(run_id, dag_id, "product_category_and_cost", "Accuracy", "CRITICAL", len(by_file["products"]), sum(r.get("_severity") == "CRITICAL" for r in by_file["products"])))
    checks.append(check_record(run_id, dag_id, "store_zone_and_capacity", "Completeness", "CRITICAL", len(by_file["stores"]), sum(r.get("_severity") == "CRITICAL" for r in by_file["stores"])))
    checks.append(check_record(run_id, dag_id, "payment_required_fields_and_status", "Completeness", "CRITICAL", len(by_file["payments"]), sum(r.get("_severity") == "CRITICAL" for r in by_file["payments"])))
    checks.append(check_record(run_id, dag_id, "payment_order_reference", "Consistency", "CRITICAL", len(by_file["payments"]), sum("no matching order" in str(r.get("failure_reason", "")) for r in by_file["payments"])))

    # Expose validated, last-source-winner order records to downstream tasks.
    by_file["orders"] = [row for row in current.values() if row.get("_source_file") == SOURCE_FILES["orders"]]
    if "orders_increment" in source:
        by_file["orders_increment"] = [row for row in current.values() if row.get("_source_file") == "orders_2026-10-01_increment.csv"]

    rejected: dict[str, list[dict[str, Any]]] = {name: [] for name in ("orders", "orders_increment", "payments", "customers", "products", "stores")}
    for source_name, rows in (("orders", source["orders"]), ("orders_increment", source.get("orders_increment", []))):
        expected_source_file = SOURCE_FILES["orders"] if source_name == "orders" else "orders_2026-10-01_increment.csv"
        for row in rows:
            order_id = str(row.get("order_id") or "").strip()
            current_row = current.get(order_id)
            if (current_row is None
                    or current_row.get("_source_file") != expected_source_file
                    or int(current_row.get("_source_row_number", 0)) != int(row.get("_source_row_number", 0))):
                reason = "superseded by a later source row or increment correction"
            else:
                reason = str(current_row.get("failure_reason", "")) if current_row.get("_severity") == "CRITICAL" else ""
            if reason:
                rejected[source_name].append({**row, "failure_reason": reason})
    for name in ("payments", "customers", "products", "stores"):
        # Keep duplicate losers and rows with critical validation issues in quarantine; warning-only records remain loadable.
        for row in source[name]:
            winner = next((r for r in by_file[name] if str(r.get({"payments":"payment_id","customers":"customer_id","products":"product_id","stores":"store_id"}[name], "")).strip() == str(row.get({"payments":"payment_id","customers":"customer_id","products":"product_id","stores":"store_id"}[name], "")).strip()), None)
            reason = "duplicate key; later physical source row wins" if winner is not None and int(row.get("_source_row_number", 0)) != int(winner.get("_source_row_number", 0)) else str(winner.get("failure_reason", "")) if winner and winner.get("_severity") == "CRITICAL" else ""
            if reason:
                rejected[name].append({**row, "failure_reason": reason})

    workdir = make_run_directory(run_id)
    clean_paths: dict[str, str] = {}
    for name, rows in by_file.items():
        path = workdir / f"{name}_passed.csv"
        # Warning rows pass; critical rows do not. Keep in-file deduplicated winners only.
        write_rows = [row for row in rows if row.get("_severity") != "CRITICAL"]
        write_csv(path, write_rows)
        clean_paths[name] = str(path)
    quarantine_paths = {}
    for name, rows in rejected.items():
        path = write_quarantine(SOURCE_FILES[name], rows, run_id)
        if path:
            quarantine_paths[name] = str(path)
    for record in checks:
        record["run_id"] = run_id
        record["dag_id"] = dag_id
    for name, rows in rejected.items():
        print(f"[DQ] {name}: input={len(source.get(name, []))}, quarantined={len(rows)}")
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "clean_paths": clean_paths,
        "quarantine_paths": quarantine_paths,
        "checks": checks,
        "extracted": sum(len(rows) for rows in source.values()),
        "quarantined": sum(len(rows) for rows in rejected.values()),
    }


def insert_frame(con: Any, table_name: str, frame: pd.DataFrame, *, replace: bool = False) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        raise ValueError(f"Unsafe table name: {table_name}")
    view_name = f"_frame_{uuid.uuid4().hex}"
    con.register(view_name, frame)
    try:
        if replace:
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")
        else:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM {view_name}")
    finally:
        con.unregister(view_name)


def ensure_metadata_tables(con: Any) -> None:
    con.execute("SET TimeZone = 'UTC'")
    con.execute("""
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
            checked_at TIMESTAMPTZ
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )
    """)


def required_tasks_succeeded(context: dict[str, Any], dag_id: str, task_ids: tuple[str, ...]) -> bool:
    """Fail closed unless every required, non-mapped task has actually succeeded.

    Airflow 3 Task SDK queries the execution API; no direct metadata DB access.
    Optional increment absence is handled by its task returning successfully.
    """
    succeeded = context["ti"].get_ti_count(
        dag_id=dag_id, run_ids=[context["run_id"]],
        task_ids=list(task_ids), states=["success"],
    )
    print(f"[Audit] required tasks succeeded={succeeded}/{len(task_ids)}")
    return succeeded == len(task_ids)


def assert_run_success(context: dict[str, Any], dag_id: str) -> None:
    """Final leaf: propagate any pipeline or audit failure into the DAG state."""
    from airflow.exceptions import AirflowException
    result = context["ti"].xcom_pull(task_ids="audit")
    if not isinstance(result, dict) or result.get("status") != "SUCCESS":
        raise AirflowException(f"{dag_id}: a required pipeline task or audit failed; inspect task logs")
