-- SQL-only typed and masked dimension/payment staging.
-- DuckDB table sources are appended raw by the ELT DAG; keep current state keyed on IDs.

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY nullif(trim(customer_id), '')
        ORDER BY _loaded_at DESC, _source_file ASC, _source_row_number DESC
    ) AS _rn
    FROM raw_customers
), typed AS (
    SELECT
        trim(customer_id) AS customer_id,
        CASE WHEN strpos(trim(email), '@') > 1
             THEN substr(trim(email), 1, 1) || '***@' || split_part(trim(email), '@', 2)
             ELSE NULL END AS email_masked,
        CASE WHEN length(regexp_replace(coalesce(phone, ''), '[^0-9]', '', 'g')) >= 4
             THEN substr(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2) ||
                  repeat('*', greatest(length(regexp_replace(phone, '[^0-9]', '', 'g')) - 4, 0)) ||
                  right(regexp_replace(phone, '[^0-9]', '', 'g'), 2)
             ELSE NULL END AS phone_masked,
        upper(trim(loyalty_tier)) AS loyalty_tier,
        upper(trim(is_active)) AS is_active,
        try_cast(signup_date AS DATE) AS signup_date,
        _source_file, _source_row_number, _loaded_at, _run_id,
        concat_ws('; ',
            CASE WHEN nullif(trim(customer_id), '') IS NULL THEN 'missing customer_id' END,
            CASE WHEN try_cast(signup_date AS DATE) IS NULL THEN 'signup_date missing or invalid' END,
            CASE WHEN try_cast(signup_date AS DATE) > current_date THEN 'future signup_date' END
        ) AS failure_reason,
        CASE WHEN nullif(trim(customer_id), '') IS NULL OR try_cast(signup_date AS DATE) IS NULL OR try_cast(signup_date AS DATE) > current_date THEN 'FAIL'
               WHEN nullif(trim(city), '') IS NULL
                   OR (email IS NOT NULL AND email <> '' AND NOT regexp_matches(trim(email), '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'))
                   OR (phone IS NOT NULL AND phone <> '' AND length(regexp_replace(phone, '[^0-9]', '', 'g'))<>10)
                   OR (pincode IS NOT NULL AND pincode <> '' AND NOT regexp_matches(trim(pincode), '^[1-9][0-9]{5}$'))
                   OR upper(trim(loyalty_tier)) NOT IN (__LOYALTY_TIERS__) THEN 'WARN'
             ELSE 'PASS' END AS dq_status
    FROM ranked WHERE _rn = 1
)
SELECT * FROM typed;

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY nullif(trim(product_id), '')
        ORDER BY _loaded_at DESC, _source_row_number DESC
    ) AS _rn
    FROM raw_products
)
SELECT
    trim(product_id) AS product_id,
    trim(product_name) AS product_name,
    CASE upper(trim(category)) WHEN 'DAIRY & BAKERY' THEN 'Dairy & Bakery' WHEN 'FRUITS & VEGETABLES' THEN 'Fruits & Vegetables' WHEN 'HOUSEHOLD' THEN 'Household' WHEN 'PERSONAL CARE' THEN 'Personal Care' WHEN 'SNACKS & BEVERAGES' THEN 'Snacks & Beverages' WHEN 'STAPLES' THEN 'Staples' ELSE trim(category) END AS category,
    trim(sub_category) AS sub_category,
    trim(brand) AS brand,
    try_cast(mrp AS DECIMAL(12,2)) AS mrp,
    try_cast(cost_price AS DECIMAL(12,2)) AS cost_price,
    trim(unit) AS unit,
    try_cast(is_perishable AS BOOLEAN) AS is_perishable,
    try_cast(launch_date AS DATE) AS launch_date,
    _source_file, _source_row_number, _loaded_at, _run_id,
    concat_ws('; ',
        CASE WHEN nullif(trim(product_id), '') IS NULL THEN 'missing product_id' END,
        CASE WHEN nullif(trim(category), '') IS NULL OR upper(trim(category)) NOT IN (__PRODUCT_CATEGORIES__) THEN 'missing or invalid category' END,
        CASE WHEN try_cast(mrp AS DECIMAL(12,2)) IS NULL OR try_cast(mrp AS DECIMAL(12,2)) <= 0 THEN 'mrp must be positive' END,
        CASE WHEN try_cast(cost_price AS DECIMAL(12,2)) IS NULL OR try_cast(cost_price AS DECIMAL(12,2)) > try_cast(mrp AS DECIMAL(12,2)) THEN 'cost_price missing or exceeds mrp' END
    ) AS failure_reason,
    CASE WHEN nullif(trim(product_id), '') IS NULL OR nullif(trim(category), '') IS NULL OR upper(trim(category)) NOT IN (__PRODUCT_CATEGORIES__)
              OR try_cast(mrp AS DECIMAL(12,2)) IS NULL OR try_cast(mrp AS DECIMAL(12,2)) <= 0
              OR try_cast(cost_price AS DECIMAL(12,2)) IS NULL OR try_cast(cost_price AS DECIMAL(12,2)) > try_cast(mrp AS DECIMAL(12,2)) THEN 'FAIL'
         ELSE 'PASS' END AS dq_status
FROM ranked WHERE _rn = 1;

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY nullif(trim(store_id), '')
        ORDER BY _loaded_at DESC, _source_row_number DESC
    ) AS _rn
    FROM raw_stores
)
SELECT
    trim(store_id) AS store_id,
    trim(store_name) AS store_name,
    CASE upper(trim(zone)) WHEN 'NORTH' THEN 'North' WHEN 'SOUTH' THEN 'South' WHEN 'EAST' THEN 'East' WHEN 'WEST' THEN 'West' ELSE trim(zone) END AS zone,
    try_cast(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    try_cast(open_date AS DATE) AS open_date,
    _source_file, _source_row_number, _loaded_at, _run_id,
    concat_ws('; ',
        CASE WHEN nullif(trim(store_id), '') IS NULL THEN 'missing store_id' END,
        CASE WHEN upper(trim(zone)) NOT IN (__ZONES__) THEN 'zone missing or invalid' END,
        CASE WHEN try_cast(capacity_orders_per_day AS INTEGER) IS NULL OR try_cast(capacity_orders_per_day AS INTEGER) <= 0 THEN 'capacity must be positive' END
    ) AS failure_reason,
    CASE WHEN nullif(trim(store_id), '') IS NULL OR upper(trim(zone)) NOT IN (__ZONES__) THEN 'FAIL'
         WHEN try_cast(capacity_orders_per_day AS INTEGER) IS NULL OR try_cast(capacity_orders_per_day AS INTEGER) <= 0 THEN 'WARN'
         ELSE 'PASS' END AS dq_status
FROM ranked WHERE _rn = 1;

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY nullif(trim(payment_id), '')
        ORDER BY _loaded_at DESC, _source_row_number DESC
    ) AS _rn
    FROM raw_payments
)
SELECT
    trim(payment_id) AS payment_id,
    trim(order_id) AS order_id,
    try_cast(payment_ts AS TIMESTAMPTZ) AS payment_ts,
    CASE upper(trim(payment_mode)) WHEN 'COD' THEN 'CASH ON DELIVERY' ELSE upper(trim(payment_mode)) END AS payment_mode,
    try_cast(amount_paid AS DECIMAL(12,2)) AS amount_paid,
    upper(trim(payment_status)) AS payment_status,
    nullif(trim(gateway_ref), '') AS gateway_ref,
    _source_file, _source_row_number, _loaded_at, _run_id,
    concat_ws('; ',
        CASE WHEN nullif(trim(payment_id), '') IS NULL THEN 'missing payment_id' END,
        CASE WHEN try_cast(payment_ts AS TIMESTAMPTZ) IS NULL THEN 'payment_ts missing or invalid' END,
        CASE WHEN try_cast(amount_paid AS DECIMAL(12,2)) IS NULL THEN 'amount_paid missing or invalid' END,
        CASE WHEN try_cast(amount_paid AS DECIMAL(12,2)) < 0 THEN 'amount_paid is negative' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM raw_orders o WHERE trim(o.order_id)=trim(ranked.order_id)) THEN 'order_id has no matching order' END
    ) AS failure_reason,
    CASE WHEN nullif(trim(payment_id), '') IS NULL OR try_cast(payment_ts AS TIMESTAMPTZ) IS NULL
              OR try_cast(amount_paid AS DECIMAL(12,2)) IS NULL OR try_cast(amount_paid AS DECIMAL(12,2)) < 0
              OR NOT EXISTS (SELECT 1 FROM raw_orders o WHERE trim(o.order_id)=trim(ranked.order_id)) THEN 'FAIL'
         WHEN nullif(trim(gateway_ref), '') IS NULL OR upper(trim(payment_status)) NOT IN (__PAYMENT_STATUSES__) THEN 'WARN'
         ELSE 'PASS' END AS dq_status
FROM ranked WHERE _rn = 1;

-- Six-dimension scorecard rows for every dimension table and dimension represented here.
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'customer_required_fields', 'Completeness', 'CRITICAL', count(*), count(*) FILTER (WHERE failure_reason LIKE '%missing customer_id%' OR failure_reason LIKE '%signup_date%'), CASE WHEN count(*) FILTER (WHERE failure_reason LIKE '%missing customer_id%' OR failure_reason LIKE '%signup_date%')=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_customers;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'customer_key_unique', 'Uniqueness', 'CRITICAL', (SELECT count(*) FROM raw_customers WHERE _run_id='__RUN_ID__'),
       coalesce((SELECT sum(n-1) FROM (SELECT count(*) n FROM raw_customers WHERE _run_id='__RUN_ID__' GROUP BY customer_id HAVING count(*)>1) d),0),
       CASE WHEN EXISTS (SELECT 1 FROM raw_customers WHERE _run_id='__RUN_ID__' GROUP BY customer_id HAVING count(*)>1) THEN 'FAIL' ELSE 'PASS' END, current_timestamp;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'customer_format', 'Validity', 'WARNING', count(*), count(*) FILTER (WHERE dq_status='WARN'), CASE WHEN count(*) FILTER (WHERE dq_status='WARN')=0 THEN 'PASS' ELSE 'WARN' END, current_timestamp FROM stg_customers;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'customer_signup_valid', 'Validity', 'CRITICAL', count(*), count(*) FILTER (WHERE signup_date IS NULL), CASE WHEN count(*) FILTER (WHERE signup_date IS NULL)=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_customers;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'customer_signup_not_future', 'Timeliness', 'CRITICAL', count(*), count(*) FILTER (WHERE signup_date>current_date), CASE WHEN count(*) FILTER (WHERE signup_date>current_date)=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_customers;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'product_price_cost', 'Accuracy', 'CRITICAL', count(*), count(*) FILTER (WHERE dq_status='FAIL'), CASE WHEN count(*) FILTER (WHERE dq_status='FAIL')=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_products;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'store_zone', 'Completeness', 'CRITICAL', count(*), count(*) FILTER (WHERE dq_status='FAIL'), CASE WHEN count(*) FILTER (WHERE dq_status='FAIL')=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_stores;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'payment_fields', 'Consistency', 'CRITICAL', count(*), count(*) FILTER (WHERE dq_status='FAIL'), CASE WHEN count(*) FILTER (WHERE dq_status='FAIL')=0 THEN 'PASS' ELSE 'FAIL' END, current_timestamp FROM stg_payments;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'product_id_unique', 'Uniqueness', 'CRITICAL', (SELECT count(*) FROM raw_products WHERE _run_id='__RUN_ID__'),
       coalesce((SELECT sum(n-1) FROM (SELECT count(*) n FROM raw_products WHERE _run_id='__RUN_ID__' GROUP BY product_id HAVING count(*)>1) d),0),
       CASE WHEN EXISTS (SELECT 1 FROM raw_products WHERE _run_id='__RUN_ID__' GROUP BY product_id HAVING count(*)>1) THEN 'FAIL' ELSE 'PASS' END, current_timestamp;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'store_id_unique', 'Uniqueness', 'CRITICAL', (SELECT count(*) FROM raw_stores WHERE _run_id='__RUN_ID__'),
       coalesce((SELECT sum(n-1) FROM (SELECT count(*) n FROM raw_stores WHERE _run_id='__RUN_ID__' GROUP BY store_id HAVING count(*)>1) d),0),
       CASE WHEN EXISTS (SELECT 1 FROM raw_stores WHERE _run_id='__RUN_ID__' GROUP BY store_id HAVING count(*)>1) THEN 'FAIL' ELSE 'PASS' END, current_timestamp;
INSERT INTO dq_results (run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at)
SELECT '__RUN_ID__', 'nammamart_elt_nirmal', 'payment_id_unique', 'Uniqueness', 'CRITICAL', (SELECT count(*) FROM raw_payments WHERE _run_id='__RUN_ID__'),
       coalesce((SELECT sum(n-1) FROM (SELECT count(*) n FROM raw_payments WHERE _run_id='__RUN_ID__' GROUP BY payment_id HAVING count(*)>1) d),0),
       CASE WHEN EXISTS (SELECT 1 FROM raw_payments WHERE _run_id='__RUN_ID__' GROUP BY payment_id HAVING count(*)>1) THEN 'FAIL' ELSE 'PASS' END, current_timestamp;
