# NammaMart Quick Commerce — ETL & ELT submission

**Author:** Nirmal  
**Status:** Astro CLI 1.46.0, Docker Desktop 4.95.0 and the Astro Runtime Airflow 3.3 container are running. The latest DAG import check returned no errors. ETL and ELT each have a green `astro dev run dags test` execution in the Airflow output. Both report September delivered revenue of **₹937,787.25**; ETL loaded 1,877 validated fact rows (1,739 delivered), while ELT staged 2,550 current source-key rows (673 critical failures, 1,877 without critical failure) and created 414 daily/store aggregate rows. Important: ELT's successful run audit was initially recorded as FAILED because its audit looked up incorrect task IDs; the task IDs have since been made explicit, but the corrected audit has not yet been rerun. Ten UI experiments, all six query result sets, screenshots and idempotency/reconciliation audit verification remain pending. Do not treat the runs as fully accepted until that audit and the pending checks are complete.

## 1. Discovery and profiling

The six untouched assignment CSVs were copied to `include/data/nammamart/`. The reproducible profiler is `include/nammamart_profile.py`; its rule inputs are in `include/nammamart_rules.json`. It generated [PROFILE_FINDINGS.md](assignment/PROFILE_FINDINGS.md) and the PII-safe row issue log [DATA_ISSUES_LOG.csv](assignment/DATA_ISSUES_LOG.csv). Source keys and row numbers are retained in the issues log; personal contact data is not included.

Verified source row counts: orders 2,412; payments 2,228; customers 405; products 152; stores 15; increment 160 (5,372 total). The detailed nulls, duplicate key counts, numeric/date ranges and category values are in the profile report.

| Source | Verified issues | Quality signals |
|---|---:|---|
| orders.csv | 107 issue records | 1 empty timestamp, 18 empty customer IDs, 164 empty delivery durations, 12 duplicate-key groups; negative quantity, discounts above 100%, unknown status/mode, implausible duration, bad amount/timestamp values, and missing master references |
| payments.csv | 484 issue records | 301 empty gateway references, 4 duplicate-key groups, negative amounts, unknown order references and payment reconciliation exceptions |
| customers.csv | 38 issue records | 6 empty city values, 5 duplicate-key groups, invalid contact/pincode/tier cases and future signup date(s) |
| products.csv | 11 issue records | 3 missing categories, 2 duplicate-key groups, invalid price/cost/category cases |
| stores.csv | 2 issue records | 1 missing zone and invalid capacity case(s) |
| orders_2026-10-01_increment.csv | no stand-alone issue records in the profile log | 160 rows; 150 new IDs and 10 baseline IDs with 10 changed statuses, all corrected to RETURNED |

The profile's issue-log totals (which count issue records, not unique bad rows) are 642: 310 CRITICAL and 332 WARNING. The relationship scan found 18 order references to missing customers, 9 missing products, 5 missing stores and 8 payments whose order ID is absent from the baseline order file. Duplicate-group counts are 12 orders, 4 payments, 5 customers, 2 products and 0 stores.

### Three questions for the client

1. Should October return corrections restate September revenue, or should Finance retain a September close snapshot as well as the restated current view?
2. Is reconciliation based on the exact sum of SUCCESS captures after cent rounding? How should partial captures, multiple captures, chargebacks and REFUNDED events be mapped to orders?
3. Are 8–35 minutes a warning-only operating band and 120 minutes the critical rejection ceiling? Which Ops/Finance owner approves changes to these thresholds?

### Shared rule decisions

The Python and SQL paths read `include/nammamart_rules.json`. Amounts use decimal arithmetic and two-decimal ROUND_HALF_UP rounding. Discounts are inclusive 0–100%; quantity must be positive. Modes are trimmed/uppercased with only explicit aliases (`upi`, `cod`, etc.) mapped; unknown values are not guessed. A duplicate within a source file loses to the last physical source row; the losing source row is kept in quarantine/evidence. For cross-file order state, the increment always wins over baseline, including on later reruns. Source row numbers and file names are retained to show which version won.

Only SUCCESS payments count as collections; their sum is compared exactly to the rounded order amount (zero tolerance after rounding). FAILED/REFUNDED events remain auditable but do not count as successful collections. A canceled order with a SUCCESS payment is an exception. Partial/multiple captures and refunds need client sign-off. Reported revenue is after-discount order amount for current DELIVERED state only. A later RETURNED correction removes the order from the current/restated revenue view; raw ELT history preserves its earlier state.

The source is a historical September snapshot, but the assignment also asks that latest data be at most one day old. Historical freshness is therefore logged as a WARNING for this baseline; future dates are CRITICAL. The threshold should be applied as a hard production SLA only to daily feeds. **Schedule ambiguity:** 9:00 AM is the specified trigger time, but cannot also guarantee completion *before* 9:00. To meet “before 9 AM,” the client must approve an earlier start (for example, 8:00 AM) and a completion-time SLA.

## 2. Pipelines

- ETL DAG: [dags/nammamart_etl_nirmal.py](dags/nammamart_etl_nirmal.py). It has six parallel source extracts (the five base exports plus the available increment), Python validation, per-source `failure_reason` quarantine, standardization/masking, star-schema load, payment reconciliation, daily/category reports, and an `all_done` run audit. Finance facts are rebuilt as an idempotent current-state snapshot, so updates replace by `order_id` and rejected rows do not reach the fact.
- ELT DAG: [dags/nammamart_elt_nirmal.py](dags/nammamart_elt_nirmal.py). Every input CSV is appended without cleaning into VARCHAR raw tables with load/file/row/run metadata. SQL in `include/sql/` creates typed, deduplicated, masked staging and reporting marts. Increment rows take precedence over baseline by order ID; raw history remains append-only and intentionally grows on reruns.
- Shared Python checks and a cross-process warehouse writer lock are in [include/nammamart_common.py](include/nammamart_common.py). The SQL report questions are in [include/sql/business_questions.sql](include/sql/business_questions.sql). The trainer DAG `dags/meridian_pipeline.py` was not modified.
- The personal-data-bearing source CSVs in `include/data/nammamart/` are the assignment-provided source-data exception. Generated warehouses, WALs, quarantine, logs, intermediates, credentials and local settings are ignored by `.gitignore`; no generated personal-data output should be added to Git.

## 3. Schedule and Airflow behavior

Both DAGs use `0 9 * * 1-6` and timezone-aware `Asia/Kolkata` start dates, `catchup=False`, two retries, a two-minute retry delay and a 25-minute execution timeout. Monday–Saturday is selected; Sunday is omitted.

The checked-in Dockerfile pins Astro Runtime `3.3-2`. Astronomer's release notes say this image contains Apache Airflow **3.3.0** (with the Astro patch) and Python 3.12–3.14. The assigned DAG's 9:00 AM IST time corresponds to **03:30 UTC** year-round. The expected UI display is 03:30 UTC when the UI is set to UTC; a UI screenshot/next-run check remains pending. The cron-triggered run occurs at its scheduled instant; tasks take additional time, so it does not meet a “ready before 9” SLA.

Airflow 3.3 distinguishes trigger timetables from data-interval timetables; this project uses a cron string and its configured runtime default. With `catchup=False`, missed scheduled intervals are not replayed as a backlog. If the laptop/Docker is off at 9 AM, no local scheduler runs then. On restart, the next eligible schedule runs; the missed report must be manually triggered if it is still required. `catchup=False` avoids a burst of stale historical runs, which is appropriate for a daily current-state report.

## 4. Data quality, observability and governance

Both DAGs write `dq_results` rows by run/check/dimension/severity, including completeness, uniqueness, validity, accuracy, consistency and timeliness. Schema, order volume, price distribution, freshness and payment/foreign-key checks are included. CRITICAL rows are rejected from reporting; warning-only rows may load and are recorded. The ELT raw tables remain untouched and are intentionally not finance/reporting tables. A run-level `pipeline_runs` record is attempted by a final `all_done` task and uses observed task states to record failure.

DuckDB has one writer at a time per database file. The helpers serialize writers through a cross-process lock shared by the two DAGs; reads/reports also acquire that lock around DuckDB sessions to avoid local writer conflicts. A lock left by a killed process may require operational cleanup if its task retries time out.

### Ownership and access

Orders: Order Management/Ops owns order fields and statuses. Payments: Finance/Gateway Operations. Customers: CRM/Privacy. Products and costs: Merchandising/Finance. Stores/zones/capacity: Store Operations. The warehouse team owns derived table contracts. Ops and Finance jointly approve delivery/revenue rules; Privacy reviews personal-data use. Raw customer data should be restricted to a small, audited ingestion/privacy group; staging exposes only masked contact fields; finance reads curated facts/marts; analysts receive masked marts; quarantine is restricted to data-quality operators and source owners.

### Data dictionary (reporting surface)

| Table/column | Type (logical) | Meaning / source | Allowed values / PII |
|---|---|---|---|
| `etl_fact_orders.order_id` | string | OMS order key | Unique current state; linkable identifier, restrict access |
| `etl_fact_orders.order_ts`, `order_date` | timestamp, date | OMS event time/date | Parseable; not PII by itself |
| `etl_fact_orders.customer_id`, `product_id`, `store_id` | string | OMS foreign keys | Must match valid dimensions; customer ID is linkable/pseudonymous |
| `etl_fact_orders.quantity`, `unit_price`, `discount_pct`, `order_amount` | numeric | OMS measures | Quantity >0; discount 0–100; money rounded to 2 decimals; no PII |
| `etl_fact_orders.order_status`, `payment_mode` | string | OMS current state/mode | DELIVERED/CANCELLED/RETURNED; UPI/CARD/CASH ON DELIVERY/WALLET |
| `etl_fact_orders.delivery_minutes`, `within_15_min` | integer, boolean | OMS delivery duration / derived SLA | Delivered duration required; >=120 critical; no PII |
| `etl_fact_orders.net_revenue`, `gross_margin` | decimal | Derived from discounted amount and product cost | Revenue only on delivered orders; no PII |
| `etl_dim_customer.customer_id`, `loyalty_tier`, `email`, `phone` | string | CRM, deduplicated; email/phone masked | Approved tier; masked contact fields are still personal data |
| `etl_dim_product.product_id`, `category`, `cost_price`, `mrp` | string, decimal | Product catalogue | Approved category; positive MRP and cost <= MRP; no PII |
| `etl_dim_store.store_id`, `zone`, `store_name` | string | Store master | North/South/East/West; manager name and precise store contact data excluded |
| ELT `stg_customers` | typed fields + `dq_status`, `failure_reason` | Masked/deduplicated CRM state | Full name/address omitted; email/phone masked |
| ELT `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`, `mart_rejected_records` | typed aggregate/detail tables | SQL reporting layer | `mart_top_customers` uses customer ID/tier only; no raw contacts |

Personal data in the source includes customer full name, email, phone, city, area and pincode, plus store manager names. Customer IDs are treated as linkable identifiers. The reporting facts retain only the operational customer ID; dimensions mask email/phone and omit names/address/manager names. Masking preserves limited matching/quality utility while lowering exposure; it is not anonymization.

**Indian data-protection-law answer — legal verification required before submission:** the likely governing framework is India's Digital Personal Data Protection Act, 2023, for digital personal data. At a high level, document a clear purpose/notice and lawful basis, limit access/use, protect the data, handle rights/grievances and breach duties, and erase when no longer needed subject to lawful retention. I have not verified the current commencement notifications, rules, or amendments against the latest Gazette/MeitY/India Code text; confirm with authoritative current sources or counsel before representing this as a current compliance opinion.

**Retention proposal (not client-approved):** keep raw source history for a short, approved audit/replay window (initial proposal: 90 days), quarantine for 90 days after resolution, and masked analytical history according to Finance's statutory record schedule. Apply legal holds where needed; then securely delete/archive under an approved policy and log disposal. Finance/Privacy must set final periods before production.

Secrets are not needed by these DAGs. The setup guide describes Airflow Variables for credentials/webhooks; future integrations should use Airflow Variables or a secrets backend at runtime, never literals in Python or Git. The checked-in setup guide is not evidence that secrets have been configured here.

### Concrete lineage example

A lineage trace begins at `orders.csv.order_id/order_amount/order_status`, joins the matching customer/product/store source rows, uses `products.cost_price` for gross margin, then aggregates to the store/date row in `etl_daily_zone_revenue` or `mart_daily_store_kpis`. ETL preserves source row numbers in quarantine evidence, while ELT preserves the exact raw row and load metadata, making replay/history easier. **A specific surviving order-to-mart numeric lineage example is still pending.**

Audit question: choose a rejected key from [DATA_ISSUES_LOG.csv](assignment/DATA_ISSUES_LOG.csv), inspect its source-file row (the original raw CSV is preserved), read `failure_reason` in ETL quarantine or ELT staging, and query that key against the finance fact/mart to prove exclusion. The DAGs have run, but this specific rejected-key proof remains pending.

Change-history answer: after loading the increment, current ETL fact and ELT staging/marts should show the 10 corrected orders as RETURNED and exclude them from current September net revenue. ELT raw history retains both baseline and correction versions; ETL quarantine/validation evidence retains superseded versions. A finance close snapshot would need a separate immutable month-close table if Finance wants the original September close as well as a restated view.

## 5. Six business queries and results

The executable ETL and ELT queries are in [business_questions.sql](include/sql/business_questions.sql). They cover: (1) daily revenue by zone/store; (2) category revenue/margin; (3) delivery average and <=15-minute share; (4) revenue by payment mode and unreconciled amount; (5) top ten masked-ID customers/tier; (6) rejected rows by file/reason.

**Verified comparison:** September delivered orders are 1,739 and net revenue is ₹937,787.25 in both ETL fact and ELT daily/store mart, with identical current-state scope. The ETL category report is in the execution log: Dairy & Bakery ₹149,487.55 revenue / ₹39,561.72 margin; Fruits & Vegetables ₹76,713.75 / ₹18,480.45; Household ₹175,270.40 / ₹44,399.97; Personal Care ₹176,181.35 / ₹39,704.95; Snacks & Beverages ₹147,571.30 / ₹34,234.36; Staples ₹212,562.90 / ₹54,280.95. Other outputs needed to fully answer each question from both pipelines (daily-zone rows, SLA, payment modes/reconciliation, top customers, and rejection reasons) still need to be queried and captured. Do not describe all six question answers as verified yet.

## 6. Ten observability experiments

The ordinary ETL and ELT happy paths have each completed once, but the ten controlled acceptance experiments below have not been performed. Expected behaviors below are design expectations, not observed outcomes. Run each experiment on an isolated copy, restore source files afterwards, and record actual task IDs/log lines/table counts plus screenshots in `submission/evidence/` before marking complete.

| # | Experiment | Expected | Observed / evidence location | Proposed improvement |
|---:|---|---|---|---|
| 1 | Happy path | Parallel extracts; DQ, quarantine, facts/marts and run audit complete | **Pending** — no Airflow execution/UI | Capture Graph, task durations, DQ and audit rows |
| 2 | Rerun | ETL fact/marts unchanged; ELT raw grows by rows actually appended, current marts unchanged | **Pending** | Capture before/after raw counts and reporting totals |
| 3 | Increment | 150 new order IDs; 10 existing IDs updated to RETURNED; no duplicate current IDs | Source profile verified 150/10; **warehouse application pending** | Query insert/update/current-status counts |
| 4 | Missing products file | Extract retry then failed/upstream-failed states; audit row FAILED | **Pending** | Add explicit failure callback/alert after UI behavior is verified |
| 5 | Five bad orders | Null customer, negative quantity, unknown store/status and future timestamp each receives a specific critical reason; no finance load | **Pending** | Confirm all five quarantine rows and per-check DQ counts |
| 6 | Rename order_amount | Source-contract check fails loudly and records a schema failure; no silent mart load | **Pending** | Capture exception and `dq_results` row |
| 7 | Only 50 orders | Volume warning triggers under the configured <10%-of-baseline cutoff | **Pending** | Confirm the baseline/increment scope and warning threshold with Ops |
| 8 | Multiply one store's unit_price by 100 | Per-store price-distribution warning; any revenue/margin change measured from isolated copy | **Pending** | Establish store/category-aware bounds from history |
| 9 | Orders 30 days old | Freshness WARNING; future-date rule remains separate/critical | **Pending** | Confirm production freshness SLA and source delivery schedule |
| 10 | Recovery after failure | Clear failed task and downstream tasks; completed upstream work stays complete subject to dependencies | **Pending** | Capture task states and run audit before/after recovery |

Five observability pillars: freshness (latest timestamp warning), volume (baseline order count threshold), schema (expected headers), distribution (store price mean vs global median), lineage (ELT source file/row/run metadata and raw history; ETL quarantine source row). **Designed strongest:** lineage/auditability, especially ELT raw. **Weakest pending measurement:** distribution, because its initial threshold is deliberately simple and needs business-calibrated baselines.

## 7. Recommendation to Sameer (provisional; execution evidence pending)

**To: Sameer, CTO — Recommendation: adopt ELT raw history with a certified, quality-gated finance mart.** The profile found defects across all five operational files: duplicate keys, missing relationships, implausible measures/timestamps, and payment exceptions. That makes a single spreadsheet-style “clean and forget” extract unsuitable. ELT's append-only VARCHAR raw layer preserves the source as received and load/run provenance; SQL staging lets Analytics and Compliance replay rules as they change. Finance should consume only the validated marts, never raw or failed staging rows. This combines ELT's audit history with the finance gate that motivated ETL.

ETL remains a useful independent validation path and comparison control. It makes the clean-vs-rejected split explicit and is easier to reason about for the compact current dataset. No runtime benchmark has been collected, so a 1000× performance or cost conclusion would be speculation. At 1000×, local DuckDB file locking and whole-snapshot staging would need benchmarking and likely replacement with partitioned/object-store raw plus a shared analytical warehouse and a production orchestrator. The open-source local design has low tool licensing cost but still incurs engineering, storage, operations and recovery costs. No performance result is claimed.

Before production, run both pipelines end-to-end, prove ETL/ELT September totals on identical state/policy, and let Finance approve mismatch/refund/close semantics. **Top three risks:** (1) the simple distribution/freshness thresholds may flag legitimate operations or miss subtle drift; (2) current-state return corrections restate historical revenue unless month-close snapshots are explicitly retained; (3) a shared local DuckDB file and local laptop scheduler are not a highly available production serving/orchestration platform.

## 8. Run instructions and evidence still needed

Prerequisites from the repository guide: Docker Desktop running and Astro CLI installed. On PowerShell, from the repository root:

```powershell
astro dev start
astro dev run dags list-import-errors
astro dev run dags test nammamart_etl_nirmal
astro dev run dags test nammamart_elt_nirmal
```

Open `http://localhost:8080`, inspect both DAGs, trigger each, then query `include/warehouse_nirmal.duckdb` using SQLTools/DuckDB. If dependencies changed, run `astro dev restart`. The host Python 3.11.9 does not have the project packages; pipeline execution uses the Astro Runtime container (observed in the first run as Airflow 3.3 and Python 3.14). Docker CLI and Astro CLI may need their installation directories added to a newly opened PowerShell session's PATH. Astro reports that its proxy daemon is unsupported on Windows; the Airflow UI was nevertheless reported at `http://localhost:8080`.

### First ETL execution attempt (2026-10-18)

`astro dev run dags list-import-errors` returned **No data found** after both DAGs were updated to add `/usr/local/airflow/include` to `sys.path`. The ETL DAG test then completed all six extracts, validation/quarantine and transformation. Its `load` task failed at the first `dq_results` insertion with DuckDB `ConversionException: Could not convert string 'orders_schema' to INT64 when casting from source column check_name`. I changed the insertion to name all destination columns explicitly. **That fix has not yet been re-executed**, so the ETL is not yet verified successful. The initial test also remains a failed/unfinished run in Airflow's history; do not treat its partial task outputs as a successful pipeline result.

**Screenshot checklist — all pending, no evidence files are linked until created:**

- [ ] ETL DAG Graph view with a successful run.
- [ ] ELT DAG Graph view with a successful run.
- [ ] Both DAG schedule / next run showing 9:00 AM IST (03:30 UTC in UTC UI).
- [ ] A quarantine CSV with `failure_reason` (redact or use synthetic data if it contains PII).
- [ ] `dq_results`, `pipeline_runs` and payment-reconciliation query results.
- [ ] Six business-query result sets from each pipeline.
- [ ] Before/after rerun and increment counts; ELT raw history counts.
- [ ] Experiments 1–10 with expected → observed → evidence → improvement.

## 9. Rubric completion checklist

| Rubric item | Status |
|---|---|
| Source profiling, six files, PII-safe issues log and client questions | **Completed / profile verified** |
| ETL implementation: parallel TaskFlow, validation, quarantine, masking, star schema, idempotent upsert, reports/reconciliation | **Implemented / runtime verification pending** |
| ELT raw/staging/marts, SQL rules, append history, increment precedence | **Implemented / runtime verification pending** |
| IST schedule, catchup, retries and timeout | **Implemented / schedule UI verification pending** |
| Six DQ dimensions, schema/freshness/volume/distribution checks and scorecard | **Implemented / runtime results pending** |
| Governance, data dictionary, lineage and legal answer | **Documented; current Indian-law/Gazette verification pending** |
| `pipeline_runs`, ten experiments and five pillars | **Implemented/documented; execution evidence pending** |
| Recommendation grounded in data profile | **Provisional recommendation written; warehouse/performance evidence pending** |
| DAG imports | **Verified: `astro dev run dags list-import-errors` returned no errors** |
| ETL green run, ELT green run, actual business-query outputs and screenshots | **Pending (ETL load fix not rerun; ELT not yet tested)** |
| Submission branch/URL | **Local branch is prepared. The assignment accepts submission in the same GitHub repository; pushing still requires confirmed write access to `origin`. No push has been made.** |
