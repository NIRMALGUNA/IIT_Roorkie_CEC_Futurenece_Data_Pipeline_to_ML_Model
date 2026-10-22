-- Current-order state from append-only raw history; increment file has explicit precedence.
CREATE OR REPLACE TEMP TABLE incoming_order_state AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY nullif(trim(order_id), '')
        ORDER BY CASE WHEN _source_file='orders_2026-10-01_increment.csv' THEN 2 ELSE 1 END DESC,
                 _loaded_at DESC, _source_row_number DESC
    ) AS _rn
    FROM raw_orders
), casted AS (
    SELECT
        trim(order_id) AS order_id,
        try_cast(order_ts AS TIMESTAMPTZ) AS order_ts,
        trim(customer_id) AS customer_id,
        trim(store_id) AS store_id,
        trim(product_id) AS product_id,
        try_cast(quantity AS INTEGER) AS quantity,
        try_cast(unit_price AS DECIMAL(12,2)) AS unit_price,
        try_cast(discount_pct AS DECIMAL(7,2)) AS discount_pct,
        try_cast(order_amount AS DECIMAL(12,2)) AS order_amount,
        CASE upper(trim(payment_mode)) WHEN 'COD' THEN 'CASH ON DELIVERY' ELSE upper(trim(payment_mode)) END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        upper(trim(channel)) AS channel,
        try_cast(delivery_minutes AS INTEGER) AS delivery_minutes,
        _source_file, _source_row_number, _loaded_at, _run_id
    FROM ranked WHERE _rn=1
), computed AS (
    SELECT *, round(quantity * unit_price * (1 - discount_pct / 100), 2) AS expected_amount,
        (SELECT sum(p.amount_paid) FROM stg_payments p WHERE p.order_id=casted.order_id AND p.payment_status=__SUCCESS_STATUS__ AND p.dq_status<>'FAIL') AS successful_payment_amount,
        (SELECT count(*) FROM stg_payments p WHERE p.order_id=casted.order_id AND p.payment_status=__SUCCESS_STATUS__ AND p.dq_status<>'FAIL') AS successful_payment_count
    FROM casted
), checked AS (
    SELECT *, concat_ws('; ',
        CASE WHEN nullif(order_id,'') IS NULL THEN 'missing order_id' END,
        CASE WHEN order_ts IS NULL THEN 'order_ts missing or invalid' END,
        CASE WHEN customer_id IS NULL OR customer_id='' THEN 'customer_id missing' END,
        CASE WHEN store_id IS NULL OR store_id='' THEN 'store_id missing' END,
        CASE WHEN product_id IS NULL OR product_id='' THEN 'product_id missing' END,
        CASE WHEN order_status NOT IN (__VALID_STATUSES__) THEN 'order_status invalid' END,
        CASE WHEN payment_mode NOT IN (__VALID_PAYMENT_MODES__) THEN 'payment_mode invalid' END,
        CASE WHEN quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL THEN 'required numeric field missing or invalid' END,
        CASE WHEN quantity IS NOT NULL AND quantity<=0 THEN 'quantity must be positive' END,
        CASE WHEN discount_pct IS NOT NULL AND (discount_pct<0 OR discount_pct>100) THEN 'discount_pct outside 0..100' END,
        CASE WHEN expected_amount IS NOT NULL AND abs(order_amount-expected_amount)>__PAY_TOLERANCE__ THEN 'order_amount formula mismatch' END,
        CASE WHEN order_status='DELIVERED' AND delivery_minutes IS NULL THEN 'delivered order missing delivery_minutes' END,
        CASE WHEN order_status=__REVENUE_STATUS__ AND delivery_minutes>=__DELIVERY_CRITICAL__ THEN 'delivery_minutes at least 120' END,
        CASE WHEN order_ts>current_timestamp THEN 'future order_ts' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id=checked_base.customer_id AND c.dq_status<>'FAIL') THEN 'customer_id missing/invalid in customer dimension' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id=checked_base.product_id AND p.dq_status<>'FAIL') THEN 'product_id missing/invalid in product dimension' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id=checked_base.store_id AND s.dq_status<>'FAIL') THEN 'store_id missing/invalid in store dimension' END,
        CASE WHEN order_status='CANCELLED' AND successful_payment_count>0 THEN 'cancelled order has SUCCESS payment' END,
        CASE WHEN order_status IN ('DELIVERED','RETURNED') AND (successful_payment_count=0 OR abs(coalesce(successful_payment_amount,0)-order_amount)>__PAY_TOLERANCE__) THEN 'SUCCESS payment missing or amount mismatch' END
    ) AS failure_reason
    FROM computed checked_base
)
SELECT *,
    CASE WHEN failure_reason<>'' THEN 'FAIL'
         WHEN order_status=__REVENUE_STATUS__ AND delivery_minutes NOT BETWEEN __NORMAL_DELIVERY_MIN__ AND __NORMAL_DELIVERY_MAX__ THEN 'WARN'
         ELSE 'PASS' END AS dq_status
FROM checked;

CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM incoming_order_state WHERE false;
CREATE TABLE IF NOT EXISTS elt_order_merge_audit (
    run_id VARCHAR, inserted_orders BIGINT, matched_orders BIGINT,
    changed_status_orders BIGINT, recorded_at TIMESTAMPTZ
);
-- Preserve the first application metrics for this run, even if a task is retried.
INSERT INTO elt_order_merge_audit
    (run_id, inserted_orders, matched_orders, changed_status_orders, recorded_at)
SELECT '__RUN_ID__',
       count(*) FILTER (WHERE target._exists IS NULL),
       count(*) FILTER (WHERE target._exists IS NOT NULL),
       count(*) FILTER (WHERE target._exists IS NOT NULL
                        AND target.order_status IS DISTINCT FROM incoming.order_status),
       current_timestamp
FROM incoming_order_state incoming
LEFT JOIN (SELECT *, true AS _exists FROM stg_orders) target
  ON target.order_id IS NOT DISTINCT FROM incoming.order_id
HAVING NOT EXISTS (SELECT 1 FROM elt_order_merge_audit WHERE run_id='__RUN_ID__');

-- Includes invalid current versions with dq_status=FAIL: never revive a rejected old version.
MERGE INTO stg_orders AS target
USING incoming_order_state AS incoming
ON target.order_id IS NOT DISTINCT FROM incoming.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;


INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_required_fields', 'Completeness', 'CRITICAL', count(*), count(*) FILTER (WHERE customer_id IS NULL OR customer_id='' OR store_id IS NULL OR store_id='' OR product_id IS NULL OR product_id='' OR order_ts IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL OR (order_status='DELIVERED' AND delivery_minutes IS NULL)), CASE WHEN count(*) FILTER (WHERE customer_id IS NULL OR customer_id='' OR store_id IS NULL OR store_id='' OR product_id IS NULL OR product_id='' OR order_ts IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL OR (order_status='DELIVERED' AND delivery_minutes IS NULL))=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_id_unique_in_source', 'Uniqueness', 'CRITICAL', (SELECT count(*) FROM raw_orders WHERE _run_id='__RUN_ID__'),
       (SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM raw_orders WHERE _run_id='__RUN_ID__' GROUP BY _source_file, order_id) d WHERE n>1),
       CASE WHEN (SELECT count(*) FROM (SELECT _source_file, order_id FROM raw_orders WHERE _run_id='__RUN_ID__' GROUP BY _source_file, order_id HAVING count(*)>1) d)=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_allowed_values', 'Validity', 'CRITICAL', count(*), count(*) FILTER (WHERE order_status NOT IN (__VALID_STATUSES__) OR payment_mode NOT IN (__VALID_PAYMENT_MODES__) OR order_ts IS NULL), CASE WHEN count(*) FILTER (WHERE order_status NOT IN (__VALID_STATUSES__) OR payment_mode NOT IN (__VALID_PAYMENT_MODES__) OR order_ts IS NULL)=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_amount_and_delivery', 'Accuracy', 'CRITICAL', count(*), count(*) FILTER (WHERE quantity<=0 OR discount_pct NOT BETWEEN 0 AND 100 OR abs(order_amount-expected_amount)>__PAY_TOLERANCE__ OR (order_status=__REVENUE_STATUS__ AND delivery_minutes>=__DELIVERY_CRITICAL__)), CASE WHEN count(*) FILTER (WHERE quantity<=0 OR discount_pct NOT BETWEEN 0 AND 100 OR abs(order_amount-expected_amount)>__PAY_TOLERANCE__ OR (order_status=__REVENUE_STATUS__ AND delivery_minutes>=__DELIVERY_CRITICAL__))=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_foreign_keys_and_payment', 'Consistency', 'CRITICAL', count(*), count(*) FILTER (WHERE failure_reason LIKE '%dimension%' OR failure_reason LIKE '%payment%' OR failure_reason LIKE '%cancelled order%'), CASE WHEN count(*) FILTER (WHERE failure_reason LIKE '%dimension%' OR failure_reason LIKE '%payment%' OR failure_reason LIKE '%cancelled order%')=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_future_timestamp', 'Timeliness', 'CRITICAL', count(*), count(*) FILTER (WHERE order_ts > current_timestamp), CASE WHEN count(*) FILTER (WHERE order_ts > current_timestamp)=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_feed_freshness', 'Timeliness', 'WARNING', count(*), CASE WHEN max(order_ts) IS NULL OR max(order_ts) < current_timestamp - INTERVAL '__MAX_AGE_DAYS__' DAY THEN count(*) ELSE 0 END, CASE WHEN CASE WHEN max(order_ts) IS NULL OR max(order_ts) < current_timestamp - INTERVAL '__MAX_AGE_DAYS__' DAY THEN count(*) ELSE 0 END=0 THEN 'PASS' ELSE 'WARN' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_delivery_normal_range', 'Accuracy', 'WARNING', count(*), count(*) FILTER (WHERE order_status=__REVENUE_STATUS__ AND delivery_minutes NOT BETWEEN __NORMAL_DELIVERY_MIN__ AND __NORMAL_DELIVERY_MAX__), CASE WHEN count(*) FILTER (WHERE order_status=__REVENUE_STATUS__ AND delivery_minutes NOT BETWEEN __NORMAL_DELIVERY_MIN__ AND __NORMAL_DELIVERY_MAX__)=0 THEN 'PASS' ELSE 'WARN' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'order_volume_baseline', 'Completeness', 'WARNING', count(*), CASE WHEN count(*)<241 THEN count(*) ELSE 0 END, CASE WHEN count(*)<241 THEN 'WARN' ELSE 'PASS' END, current_timestamp FROM stg_orders;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
WITH global_price AS (
    SELECT median(try_cast(unit_price AS DOUBLE)) AS median_price
    FROM raw_orders WHERE _run_id='__RUN_ID__' AND try_cast(unit_price AS DOUBLE)>0
), by_store AS (
    SELECT store_id, avg(try_cast(unit_price AS DOUBLE)) AS average_price
    FROM raw_orders WHERE _run_id='__RUN_ID__' AND try_cast(unit_price AS DOUBLE)>0
    GROUP BY store_id
), drift AS (
    SELECT count(*) AS checked,
           count(*) FILTER (WHERE average_price>median_price*3 OR average_price<median_price/3) AS failed
    FROM by_store CROSS JOIN global_price
)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'unit_price_by_store_distribution', 'Accuracy', 'WARNING', checked, failed,
       CASE WHEN failed=0 THEN 'PASS' ELSE 'WARN' END, current_timestamp FROM drift;
