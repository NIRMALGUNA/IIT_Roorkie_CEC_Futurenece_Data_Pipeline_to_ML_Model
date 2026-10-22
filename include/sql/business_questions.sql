-- The six assignment questions. Run the ETL or ELT section after its DAG has succeeded.

-- ETL Q1: daily net revenue by zone/store; delivered only, after discount.
SELECT order_date, zone, store_id, store_name, sum(net_revenue) AS net_revenue
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY ALL ORDER BY order_date, zone, store_id;

-- ETL Q2: revenue and gross margin by product category.
SELECT category, round(sum(net_revenue),2) AS revenue,
       round(sum(net_revenue - quantity*cost_price),2) AS gross_margin
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY category ORDER BY category;

-- ETL Q3: store delivery mean and SLA <= 15 minutes.
SELECT store_id, store_name, avg(delivery_minutes) AS average_delivery_minutes,
       100.0*count(*) FILTER (WHERE delivery_minutes<=15)/nullif(count(*),0) AS within_15_minutes_pct
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY store_id, store_name ORDER BY store_id;

-- ETL Q4: delivered revenue by mode; the second result is a single total mismatch amount.
SELECT payment_mode, sum(net_revenue) AS delivered_revenue
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY payment_mode ORDER BY payment_mode;
SELECT coalesce(sum(abs(coalesce(expected_amount,0)-coalesce(successful_amount,0))),0) AS total_unreconciled_amount
FROM etl_payment_reconciliation
WHERE run_id=(SELECT run_id FROM etl_payment_reconciliation ORDER BY checked_at DESC LIMIT 1);

-- ETL Q5: top 10 customers; only customer_id/tier, no contact or name.
SELECT customer_id, loyalty_tier, round(sum(net_revenue),2) AS spend
FROM etl_fact_orders WHERE order_status='DELIVERED'
GROUP BY customer_id, loyalty_tier ORDER BY spend DESC LIMIT 10;

-- ETL Q6: today's rejected rows by source file and failure reason.
SELECT file, failure_reason, count(*) AS rejected_rows
FROM etl_rejected_records
WHERE run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_nirmal' ORDER BY end_time DESC LIMIT 1)
GROUP BY file, failure_reason ORDER BY file, rejected_rows DESC;

-- ELT Q1.
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;

-- ELT Q2.
SELECT category, revenue, gross_margin FROM mart_category_margin ORDER BY category;

-- ELT Q3.
SELECT store_id, store_name, average_delivery_minutes, within_15_minutes_pct
FROM mart_delivery_sla ORDER BY store_id;

-- ELT Q4: delivered revenue by mode; the second result is a single total mismatch amount.
SELECT payment_mode, round(sum(order_amount),2) AS delivered_revenue
FROM stg_orders WHERE dq_status<>'FAIL' AND order_status='DELIVERED'
GROUP BY payment_mode ORDER BY payment_mode;
SELECT coalesce(sum(abs(difference)),0) AS total_unreconciled_amount FROM mart_payment_reconciliation;

-- ELT Q5.
SELECT customer_id, loyalty_tier, lifetime_spend FROM mart_top_customers ORDER BY lifetime_spend DESC;

-- ELT Q6.
SELECT file, reason, count(*) AS rejected_rows
FROM mart_rejected_records GROUP BY file, reason ORDER BY file, rejected_rows DESC;
