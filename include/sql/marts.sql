-- SQL-only reporting marts. Critical-failure rows and invalid dimensions are excluded.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    CAST(o.order_ts AS DATE) AS order_date,
    s.zone,
    s.store_id,
    s.store_name,
    count(*) FILTER (WHERE o.order_status='DELIVERED') AS delivered_orders,
    round(sum(CASE WHEN o.order_status='DELIVERED' THEN o.order_amount ELSE 0 END), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON s.store_id=o.store_id AND s.dq_status<>'FAIL'
JOIN stg_products p ON p.product_id=o.product_id AND p.dq_status<>'FAIL'
JOIN stg_customers c ON c.customer_id=o.customer_id AND c.dq_status<>'FAIL'
WHERE o.dq_status<>'FAIL'
GROUP BY ALL;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    round(sum(o.order_amount), 2) AS revenue,
    round(sum(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON p.product_id=o.product_id AND p.dq_status<>'FAIL'
JOIN stg_stores s ON s.store_id=o.store_id AND s.dq_status<>'FAIL'
JOIN stg_customers c ON c.customer_id=o.customer_id AND c.dq_status<>'FAIL'
WHERE o.dq_status<>'FAIL' AND o.order_status='DELIVERED'
GROUP BY p.category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    round(avg(o.delivery_minutes) FILTER (WHERE o.order_status='DELIVERED'), 2) AS average_delivery_minutes,
    count(*) FILTER (WHERE o.order_status='DELIVERED') AS delivered_orders,
    round(100.0 * count(*) FILTER (WHERE o.order_status='DELIVERED' AND o.delivery_minutes<=15)
          / nullif(count(*) FILTER (WHERE o.order_status='DELIVERED'), 0), 2) AS within_15_minutes_pct
FROM stg_orders o
JOIN stg_stores s ON s.store_id=o.store_id AND s.dq_status<>'FAIL'
JOIN stg_products p ON p.product_id=o.product_id AND p.dq_status<>'FAIL'
JOIN stg_customers c ON c.customer_id=o.customer_id AND c.dq_status<>'FAIL'
WHERE o.dq_status<>'FAIL'
GROUP BY s.store_id, s.store_name, s.zone;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH payment_totals AS (
    SELECT order_id,
           string_agg(payment_id, ',' ORDER BY payment_id) AS payment_ids,
           sum(amount_paid) FILTER (WHERE payment_status='SUCCESS') AS successful_amount,
           count(*) FILTER (WHERE payment_status='SUCCESS') AS successful_count,
           sum(amount_paid) FILTER (WHERE payment_status='REFUNDED') AS refunded_amount,
           sum(amount_paid) FILTER (WHERE payment_status='FAILED') AS failed_amount
    FROM stg_payments WHERE dq_status<>'FAIL' GROUP BY order_id
)
SELECT
    o.order_id,
    p.payment_ids,
    o.order_status,
    o.order_amount AS expected_amount,
    coalesce(p.successful_amount, 0) AS successful_amount,
    round(coalesce(p.successful_amount, 0) - o.order_amount, 2) AS difference,
    p.refunded_amount,
    p.failed_amount,
    CASE
        WHEN p.successful_count IS NULL AND o.order_status='CANCELLED' THEN 'cancelled order has no SUCCESS payment (expected)'
        WHEN p.successful_count IS NULL THEN 'missing SUCCESS payment'
        WHEN o.order_status='CANCELLED' THEN 'cancelled order has SUCCESS payment'
        WHEN abs(coalesce(p.successful_amount,0)-o.order_amount)>__PAY_TOLERANCE__ THEN 'SUCCESS payment amount mismatch'
        WHEN p.refunded_amount IS NOT NULL THEN 'payment was refunded; review return/refund linkage'
        ELSE 'reconciled'
    END AS reason
FROM stg_orders o LEFT JOIN payment_totals p USING(order_id)
WHERE (o.order_status<>'CANCELLED' AND p.successful_count IS NULL)
   OR (o.order_status='CANCELLED' AND coalesce(p.successful_count,0)>0)
    OR (o.order_status IN ('DELIVERED','RETURNED') AND abs(coalesce(p.successful_amount,0)-o.order_amount)>__PAY_TOLERANCE__)
   OR p.refunded_amount IS NOT NULL
UNION ALL
SELECT p.order_id, p.payment_id, NULL::VARCHAR, NULL::DECIMAL(12,2),
       CASE WHEN p.payment_status='SUCCESS' THEN coalesce(p.amount_paid,0) ELSE 0 END,
       CASE WHEN p.payment_status='SUCCESS' THEN coalesce(p.amount_paid,0) ELSE 0 END,
       CASE WHEN p.payment_status='REFUNDED' THEN p.amount_paid ELSE NULL END,
       CASE WHEN p.payment_status='FAILED' THEN p.amount_paid ELSE NULL END,
       'payment has no matching order'
FROM stg_payments p
WHERE NOT EXISTS (SELECT 1 FROM stg_orders o WHERE o.order_id=p.order_id);

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.loyalty_tier,
    round(sum(o.order_amount),2) AS lifetime_spend,
    count(*) AS delivered_order_count
FROM stg_orders o
JOIN stg_customers c ON c.customer_id=o.customer_id AND c.dq_status<>'FAIL'
JOIN stg_stores s ON s.store_id=o.store_id AND s.dq_status<>'FAIL'
JOIN stg_products p ON p.product_id=o.product_id AND p.dq_status<>'FAIL'
WHERE o.dq_status<>'FAIL' AND o.order_status='DELIVERED'
GROUP BY c.customer_id, c.loyalty_tier
ORDER BY lifetime_spend DESC
LIMIT 10;

CREATE OR REPLACE TABLE mart_rejected_records AS
WITH rejected AS (
SELECT _source_file AS file, order_id AS row_key, _source_row_number AS source_row_number, failure_reason AS reason
FROM stg_orders WHERE dq_status='FAIL'
UNION ALL
SELECT _source_file, customer_id, _source_row_number, failure_reason FROM stg_customers WHERE dq_status='FAIL'
UNION ALL
SELECT _source_file, product_id, _source_row_number, failure_reason FROM stg_products WHERE dq_status='FAIL'
UNION ALL
SELECT _source_file, store_id, _source_row_number, failure_reason FROM stg_stores WHERE dq_status='FAIL'
UNION ALL
SELECT _source_file, payment_id, _source_row_number, failure_reason FROM stg_payments WHERE dq_status='FAIL'
UNION ALL
SELECT _source_file, order_id, _source_row_number, 'duplicate order_id within this source file; later source row wins'
FROM (
    SELECT *, row_number() OVER (PARTITION BY _source_file, order_id ORDER BY _source_row_number DESC) AS _duplicate_rank
    FROM raw_orders WHERE _run_id='__RUN_ID__'
) d WHERE _duplicate_rank>1
UNION ALL
SELECT _source_file, customer_id, _source_row_number, 'duplicate customer_id within this source file; later source row wins'
FROM (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _source_row_number DESC) AS _duplicate_rank
    FROM raw_customers WHERE _run_id='__RUN_ID__'
) d WHERE _duplicate_rank>1
UNION ALL
SELECT _source_file, product_id, _source_row_number, 'duplicate product_id within this source file; later source row wins'
FROM (
    SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY _source_row_number DESC) AS _duplicate_rank
    FROM raw_products WHERE _run_id='__RUN_ID__'
) d WHERE _duplicate_rank>1
UNION ALL
SELECT _source_file, store_id, _source_row_number, 'duplicate store_id within this source file; later source row wins'
FROM (
    SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _source_row_number DESC) AS _duplicate_rank
    FROM raw_stores WHERE _run_id='__RUN_ID__'
) d WHERE _duplicate_rank>1
UNION ALL
SELECT _source_file, payment_id, _source_row_number, 'duplicate payment_id within this source file; later source row wins'
FROM (
    SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY _source_row_number DESC) AS _duplicate_rank
    FROM raw_payments WHERE _run_id='__RUN_ID__'
) d WHERE _duplicate_rank>1
)
SELECT * FROM rejected;
