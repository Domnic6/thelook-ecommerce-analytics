-- Section 1 __BASE FACT TABLES__

--1. FACT TABLE : ALL ORDER ITEMS
--Purpose : Cleaned transaction - level base table used for downstream analysis

CREATE OR REPLACE TABLE analytics.fact_order_items_all AS
SELECT
 oi.user_id,
 oi.product_id,
 oi.order_id,
 DATE(oi.created_at) AS order_date,
 oi.sale_price AS revenue,
 oi.status
FROM
`bigquery-public-data.thelook_ecommerce.order_items` oi;

--2. FACT TABLE : COMPLETED/ SUCCESSFUL ORDER ITEMS
--Purpose : Revenue - related analysis using valid purchase statuses
--NOTE: This table includes commercially valid revenue statuses
--(Processing, Shipped, Complete), not only fully completed orders

CREATE OR REPLACE TABLE analytics.fact_order_items_completed AS
SELECT *
FROM 
analytics.fact_order_items_all
WHERE status IN ('Processing', 'Shipped', 'Complete');

--3.PRODUCT DIMENSION 
CREATE OR REPLACE TABLE analytics.dim_product AS
SELECT
  id AS product_id,
  category AS product_category,
  brand,
  name AS product_name,
  retail_price
FROM `bigquery-public-data.thelook_ecommerce.products`;


--SECTION 2 : PAGE 1 
--4.CATEGORY PERFORMANCE TABLE
--Purpose : Category - level revenue, orders, users, AOV, revenue share, cancellation rate

CREATE OR REPLACE TABLE analytics.final_category_performance AS

WITH order_level AS(
  SELECT
   order_id,
   user_id,
   product_id,
   status,
   SUM(revenue) AS order_revenue
  FROM analytics.fact_order_items_all
  GROUP BY order_id, user_id, product_id, status 
),
category_base AS(
  SELECT
   p.product_category,
   COUNT(DISTINCT o.order_id)AS total_orders,
   COUNT(DISTINCT IF(o.status = 'Cancelled',o.order_id,NULL))AS cancelled_orders,
   SUM(
    IF(o.status IN ('Processing','Shipped', 'Complete'),o.order_revenue,0)
   )AS total_revenue,
   COUNT(DISTINCT IF(
    o.status IN ('Processing', 'Shipped', 'Complete'),o.user_id,NULL))AS purchasing_users
  FROM order_level o
  LEFT JOIN analytics.dim_product p
   ON o.product_id = p.product_id
  GROUP BY p.product_category   
),

company_totals AS(
  SELECT
   SUM(total_revenue) AS company_revenue,
   SUM(total_orders) AS company_orders
  FROM category_base 
)
SELECT
 cb.product_category,
 cb.total_orders,
 cb.cancelled_orders,
 cb.total_revenue,
 cb.purchasing_users,

 SAFE_DIVIDE(cb.total_revenue, cb.total_orders) AS AOV,
 SAFE_DIVIDE(cb.total_revenue, cb.purchasing_users)AS revenue_per_user,
 SAFE_DIVIDE(cb.cancelled_orders, cb.total_orders)AS cancellation_rate,
 SAFE_DIVIDE(cb.total_revenue, ct.company_revenue) AS revenue_share,
 SAFE_DIVIDE(cb.total_orders, ct.company_orders) AS order_share

FROM category_base cb
CROSS JOIN company_totals ct;

--SECTION 3 : PAGE 2
--5. FUNNEL METRICS TABLE
--Purpose : Monthly view -> cart -> purchase funnel metrics and conversion  rates

CREATE OR REPLACE TABLE analytics.agg_funnel_metrics AS
WITH product_views AS(
  SELECT
  DATE_TRUNC(DATE(created_at), MONTH) AS month,
  COUNT(DISTINCT session_id) AS product_view_sessions
  FROM
  `bigquery-public-data.thelook_ecommerce.events`
  WHERE event_type = 'product'
  GROUP BY month
),

add_to_cart AS(
  SELECT
  DATE_TRUNC(DATE(created_at),MONTH)AS month,
  COUNT(DISTINCT session_id)AS add_to_cart_sessions
  FROM
  `bigquery-public-data.thelook_ecommerce.events`
  WHERE event_type = 'cart'
  GROUP BY month
),

purchases AS(
  SELECT
  DATE_TRUNC(order_date, MONTH)AS month,
  COUNT(DISTINCT user_id) AS purchasing_users
  FROM analytics.fact_order_items_completed
  GROUP BY month
)

SELECT
pv.month,
pv.product_view_sessions,
ac.add_to_cart_sessions,
pu.purchasing_users,

SAFE_DIVIDE(ac.add_to_cart_sessions,
pv.product_view_sessions) AS view_to_cart_rate,

SAFE_DIVIDE(pu.purchasing_users,
ac.add_to_cart_sessions) AS cart_to_purchase_rate,

SAFE_DIVIDE(pu.purchasing_users,
pv.product_view_sessions)AS view_to_purchase_rate

FROM product_views pv
LEFT JOIN add_to_cart ac
ON pv.month = ac.month
LEFT JOIN purchases pu
ON pv.month = pu.month
ORDER BY pv.month;

--Section 4 : Page 3
--MONTHLY GROWTH KPI TABLE
--Purpose : Monthly revenue, orders, users, revenue per user, cancellation metrics, MoM growth

--6.Aggregate monthly KPI's

CREATE OR REPLACE TABLE analytics.agg_monthly_kpis AS
WITH monthly_orders AS(
  SELECT
   DATE_TRUNC(order_date, MONTH) AS month,
   COUNT(DISTINCT order_id) AS total_orders_all,
   COUNT(DISTINCT IF(status = 'Cancelled', order_id, NULL)) AS cancelled_orders
  FROM analytics.fact_order_items_all
  GROUP BY month
),

monthly_revenue AS(
  SELECT
   DATE_TRUNC(order_date, MONTH) AS month,
   SUM(revenue) AS total_revenue,
   COUNT(DISTINCT order_id) AS completed_orders,
   COUNT(DISTINCT user_id) AS purchasing_users
  FROM analytics.fact_order_items_completed
  GROUP BY month 
)

SELECT
 r.month,
 r.total_revenue,
 r.completed_orders AS total_orders,
 r.purchasing_users,
 SAFE_DIVIDE(r.total_revenue, r.completed_orders) AS avg_order_value,
 SAFE_DIVIDE(r.total_revenue, r.purchasing_users) AS revenue_per_user,
 o.total_orders_all,
 o.cancelled_orders,
 SAFE_DIVIDE(o.cancelled_orders, o.total_orders_all) AS cancellation_rate
FROM monthly_revenue r
LEFT JOIN monthly_orders o
 ON r.month = o.month
ORDER  BY r.month;

--7. AGGREGATE MONTHLY KPIS ENRICHED
--Purpose : Monthly revenue and order growth 
CREATE OR REPLACE TABLE analytics.agg_monthly_kpis_enriched AS
SELECT
  month,
  total_revenue,
  total_orders,
  purchasing_users,
  revenue_per_user,
  cancellation_rate,
  cancelled_revenue,

  -- MoM % change
  SAFE_DIVIDE(
    total_revenue - LAG(total_revenue) OVER (ORDER BY month),
    LAG(total_revenue) OVER (ORDER BY month)
  )AS revenue_mom_pct,

  SAFE_DIVIDE(
    total_orders - LAG(total_orders) OVER (ORDER BY month),
    LAG(total_orders) OVER (ORDER BY month)
  )AS orders_mom_pct

FROM analytics.agg_monthly_kpis
ORDER BY month;

--8.YEARLY GROWTH KPI TABLE
--Purpose : Yearly revenue, orders, YoY growth

CREATE OR REPLACE TABLE analytics.agg_yearly_kpis AS 
SELECT
 EXTRACT(YEAR FROM month) AS year,
 SUM(total_revenue) AS total_revenue,
 SUM(total_orders) AS total_orders
FROM analytics.agg_monthly_kpis
GROUP BY year
ORDER BY year;

--9. YEARLY GROWTH KPI TABLE (ENRICHED)
--Purpose : Adds YoY revenue and order growth to yearly KPI base table
CREATE OR REPLACE TABLE analytics.agg_yearly_kpis_enriched AS 

SELECT
 year,
 total_revenue,
 total_orders,

 SAFE_DIVIDE(
  total_revenue-LAG(total_revenue) OVER (ORDER BY year),
  LAG(total_revenue) OVER (ORDER BY year)
 ) AS revenue_yoy_pct,

 SAFE_DIVIDE(
  total_orders-LAG(total_orders) OVER (ORDER BY year),
  LAG(total_orders) OVER (ORDER BY year)
 )AS orders_yoy_pct
FROM analytics.agg_yearly_kpis
ORDER BY year;









