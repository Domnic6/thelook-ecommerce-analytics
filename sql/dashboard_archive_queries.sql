-- Create analytics schema
CREATE SCHEMA IF NOT EXISTS `project-495dcf05-56f5-4903-af6.analytics` 
OPTIONS (location = 'US');


-- Dimension: User
CREATE OR REPLACE TABLE analytics.dim_user AS
SELECT 
  id AS user_id,
  -- Country standardization
  CASE 
    WHEN country IN ('España', 'Spain') THEN 'Spain'
    WHEN country IN ('Brasil', 'Brazil') THEN 'Brazil'
    ELSE country 
  END AS country,

  COALESCE(gender, 'Unknown') AS gender,

  CASE 
    WHEN age BETWEEN 16 AND 24 THEN '16-24'
    WHEN age BETWEEN 25 AND 34 THEN '25-34'
    WHEN age BETWEEN 35 AND 44 THEN '35-44'
    WHEN age BETWEEN 45 AND 54 THEN '45-54'
    WHEN age >= 55 THEN '55+'
    ELSE 'Unknown'
  END AS age_group

FROM bigquery-public-data.thelook_ecommerce.users;


-- Fact: Events (initial build)
CREATE OR REPLACE TABLE analytics.fact_events AS
SELECT 
  user_id, 
  DATE(created_at) AS event_date, 
  event_type
FROM bigquery-public-data.thelook_ecommerce.events
WHERE event_type IN ('product', 'purchase')
  AND user_id IS NOT NULL;

-- Country segment metrics
CREATE OR REPLACE TABLE analytics.country_segment_metrics AS
SELECT 
  u.country,
  u.gender,
  u.age_group,
  p.product_category,

  -- Browsing
  COUNTIF(e.event_type = 'product') AS product_views,

  -- Purchasing
  COUNT(oi.product_id) AS purchases,
  SUM(oi.revenue) AS total_revenue

FROM analytics.dim_user u
LEFT JOIN analytics.fact_events e 
  ON u.user_id = e.user_id
LEFT JOIN analytics.fact_order_items oi 
  ON u.user_id = oi.user_id
LEFT JOIN analytics.dim_product p 
  ON oi.product_id = p.product_id
GROUP BY 1, 2, 3, 4;


-- Sanity check: views vs purchases
SELECT 
  SUM(product_views) AS product_views, 
  SUM(purchases) AS purchases 
FROM analytics.country_segment_metrics;


-- Cancellation metrics
-- Cancellation rate = cancelled order items / total order items
-- Tells:
-- 1. Operational quality
-- 2. Supply chain / fulfillment issues
-- 3. Country-level risks
CREATE OR REPLACE TABLE analytics.country_cancellation_metrics AS
SELECT 
  u.country,
  u.age_group,
  u.gender,
  COUNT(*) AS total_order_items,
  COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate

FROM analytics.dim_user u
LEFT JOIN bigquery-public-data.thelook_ecommerce.order_items oi
  ON u.user_id = oi.user_id
GROUP BY 1, 2, 3;


-- Country-level browsing metrics (dim_user join)
CREATE OR REPLACE TABLE analytics.country_browsing_metrics AS
SELECT 
  u.country, 
  COUNT(*) AS product_views
FROM analytics.dim_user u
JOIN analytics.fact_events e 
  ON u.user_id = e.user_id
WHERE e.event_type = 'product'
GROUP BY 1;


-- Sanity check: browsing events
SELECT 
  COUNT(*) AS total_users, 
  COUNT(user_id) AS non_null_user_events 
FROM bigquery-public-data.thelook_ecommerce.events
WHERE event_type = 'product';


-- Rebuild browsing metrics using raw events
-- Browsing data is often anonymous and incomplete
-- Orders are identified, browsing often lacks user IDs
CREATE OR REPLACE TABLE analytics.country_browsing_metrics AS
SELECT 
  u.country, 
  COUNT(*) AS product_views
FROM bigquery-public-data.thelook_ecommerce.events e
JOIN bigquery-public-data.thelook_ecommerce.users u
  ON e.user_id = u.id
WHERE e.event_type = 'product'
GROUP BY 1;


-- Cart users by country
CREATE OR REPLACE TABLE analytics.country_cart_metrics AS
SELECT 
  u.country, 
  COUNT(DISTINCT e.user_id) AS cart_users
FROM bigquery-public-data.thelook_ecommerce.events e
JOIN analytics.dim_user u 
  ON e.user_id = u.user_id
WHERE e.event_type = 'cart'
GROUP BY 1;


-- Purchasing users by country
CREATE OR REPLACE TABLE analytics.country_purchase_users AS
SELECT 
  u.country, 
  COUNT(DISTINCT oi.user_id) AS purchasing_users
FROM analytics.fact_order_items oi
JOIN analytics.dim_user u 
  ON oi.user_id = u.user_id
GROUP BY 1;


-- Cart to purchase funnel
CREATE OR REPLACE TABLE analytics.country_cart_purchase_funnel AS
SELECT 
  country,
  cart_users,
  purchasing_users,
  cart_to_purchase_conversion,
  ROUND(cart_to_purchase_conversion * 100, 2) AS cart_to_purchase_conversion_pct
FROM (
  SELECT 
    c.country,
    c.cart_users,
    COALESCE(p.purchasing_users, 0) AS purchasing_users,
    SAFE_DIVIDE(
      COALESCE(p.purchasing_users, 0),
      c.cart_users
    ) AS cart_to_purchase_conversion
  FROM analytics.country_cart_metrics c
  LEFT JOIN analytics.country_purchase_users p
    ON c.country = p.country
)
ORDER BY cart_to_purchase_conversion ASC;


-- Rebuilding fact_events (adding cart)
CREATE OR REPLACE TABLE analytics.fact_events AS
SELECT 
  user_id, 
  DATE(created_at) AS event_date, 
  event_type
FROM bigquery-public-data.thelook_ecommerce.events
WHERE event_type IN ('product', 'cart', 'purchase')
  AND user_id IS NOT NULL;


-- Funnel sanity check
SELECT 
  COUNT(DISTINCT CASE WHEN event_type = 'product' THEN user_id END) AS product_users,
  COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_users,
  COUNT(DISTINCT user_id) AS total_users
FROM analytics.fact_events;


-- Revenue sanity check 1:Country revenue
CREATE OR REPLACE TABLE analytics.country_revenue AS
SELECT 
  u.country,
  ROUND(SUM(oi.sale_price), 2) AS total_revenue,
  COUNT(DISTINCT oi.user_id) AS purchasing_users,
  COUNT(*) AS total_order_items
FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN analytics.dim_user u 
  ON oi.user_id = u.user_id
WHERE oi.status != 'Cancelled'
GROUP BY 1
ORDER BY total_revenue DESC;


--Revenue sanity check 2: Revenue share
CREATE OR REPLACE TABLE analytics.revenue_share AS
SELECT 
  country,
  total_revenue,
  ROUND(
    total_revenue / SUM(total_revenue) OVER() * 100,
    2
  ) AS revenue_share_pct
FROM analytics.country_revenue;


--Revenue sanity check 3: Revenue per user
CREATE OR REPLACE TABLE analytics.revenue_per_user AS
SELECT 
  u.country,
  ROUND(SUM(oi.sale_price), 2) AS total_revenue,
  COUNT(DISTINCT oi.user_id) AS purchasing_users,
  ROUND(
    SAFE_DIVIDE(SUM(oi.sale_price), COUNT(DISTINCT oi.user_id)),
    2
  ) AS revenue_per_user
FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN analytics.dim_user u 
  ON oi.user_id = u.user_id
WHERE oi.status != 'Cancelled'
GROUP BY 1
ORDER BY revenue_per_user DESC;


SELECT 
  schema_name,
  location
FROM `project-495dcf05-56f5-4903-af6.region-us.INFORMATION_SCHEMA.SCHEMATA`;

--Overall category conversion(Global benchmark)

CREATE OR REPLACE TABLE analytics.category_conversion_global AS
SELECT
  p.product_category,
  COUNTIF(e.event_type = 'product') AS product_views,
  COUNT(oi.product_id) AS purchases,
  SAFE_DIVIDE(
    COUNT(oi.product_id),
    COUNTIF(e.event_type = 'product')
  ) AS category_conversion_rate
FROM analytics.fact_events e
LEFT JOIN analytics.fact_order_items oi
  ON e.user_id = oi.user_id
LEFT JOIN analytics.dim_product p
  ON oi.product_id = p.product_id
GROUP BY 1;

--Demographic preference table

CREATE OR REPLACE TABLE analytics.demographic_category_preferences AS
SELECT
  age_group,
  gender,
  product_category,
  SUM(purchases) AS total_purchases,
  SUM(total_revenue) AS total_revenue
FROM analytics.country_segment_metrics
GROUP BY 1,2,3;

--Country performance summary table

CREATE OR REPLACE TABLE analytics.country_performance_summary AS
SELECT
  r.country,
  r.total_revenue,
  r.purchasing_users,
  f.cart_to_purchase_conversion_pct,
  c.cancellation_rate
FROM analytics.country_revenue r
LEFT JOIN analytics.country_cart_purchase_funnel f
  ON r.country = f.country
LEFT JOIN (
  SELECT country, AVG(cancellation_rate) AS cancellation_rate
  FROM analytics.country_cancellation_metrics
  GROUP BY country
) c
  ON r.country = c.country;

--Category purchase intensity

CREATE OR REPLACE TABLE analytics.category_purchase_intensity AS
SELECT
  p.product_category,
  COUNT(DISTINCT oi.user_id) AS purchasing_users,
  COUNT(*) AS total_items_sold,
  ROUND(SUM(oi.revenue), 2) AS total_revenue,
  ROUND(
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT oi.user_id)),
    2
  ) AS items_per_user
FROM analytics.fact_order_items oi
JOIN analytics.dim_product p
  ON oi.product_id = p.product_id
GROUP BY 1
ORDER BY total_revenue DESC;

--Sanity check: Country cancellation metrics

SELECT
 u.country,
 u.age_group,
 u.gender,
 COUNT(*) AS total_order_items,
 COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
 SAFE_DIVIDE(
   COUNTIF(oi.status = 'Cancelled'),
   COUNT(*)
 ) AS cancellation_rate
FROM analytics.dim_user u
LEFT JOIN bigquery-public-data.thelook_ecommerce.order_items oi
  ON u.user_id = oi.user_id
GROUP BY 1,2,3;

--Sanity check:Variance in cancellation rate by product category

SELECT
  p.category AS product_category,
  COUNT(*) AS total_items,
  COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate
FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN bigquery-public-data.thelook_ecommerce.products p
  ON oi.product_id = p.id
GROUP BY product_category
ORDER BY cancellation_rate DESC;

--A) Identify high_cancellation products inside categories

SELECT
 p.category,
 p.name,
 COUNT(*) AS total_items,
 COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
 SAFE_DIVIDE(
  COUNTIF(oi.status = 'Cancelled'),
  COUNT(*)
 )AS cancellation_rate
FROM `bigquery-public-data.thelook_ecommerce.order_items`oi
JOIN `bigquery-public-data.thelook_ecommerce.products`p
 ON oi.product_id = p.id
GROUP BY p.category, p.name
--HAVING total_items > 50 --avoid tiny sample noise
ORDER BY cancellation_rate DESC;  

--B) Price vs cancellation behavior

SELECT
  p.category,
  AVG(p.retail_price) AS avg_price,
  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate
FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN bigquery-public-data.thelook_ecommerce.products p
  ON oi.product_id = p.id
GROUP BY p.category
ORDER BY avg_price DESC;

--Identify "Hero Products"

SELECT
  p.category,
  p.name,
  SUM(oi.sale_price) AS revenue,
  COUNT(DISTINCT oi.user_id) AS unique_buyers
FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN bigquery-public-data.thelook_ecommerce.products p
  ON oi.product_id = p.id
WHERE oi.status = 'Complete'
GROUP BY p.category, p.name
ORDER BY revenue DESC;

--Producy performance summary

CREATE OR REPLACE TABLE analytics.product_performance_summary AS

SELECT
  p.category AS product_category,
  p.name AS product_name,
  p.retail_price,

  -- Volume metrics
  COUNT(*) AS total_order_items,
  COUNTIF(oi.status = 'Complete') AS completed_items,
  COUNTIF(oi.status = 'Cancelled') AS cancelled_items,

  -- Revenue
  SUM(CASE WHEN oi.status = 'Complete' THEN oi.sale_price ELSE 0 END) AS total_revenue,

  -- Customer metrics
  COUNT(DISTINCT CASE WHEN oi.status = 'Complete' THEN oi.user_id END) AS unique_buyers,

  -- Derived metrics
  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate,

  SAFE_DIVIDE(
    SUM(CASE WHEN oi.status = 'Complete' THEN oi.sale_price ELSE 0 END),
    COUNT(DISTINCT CASE WHEN oi.status = 'Complete' THEN oi.user_id END)
  ) AS revenue_per_buyer,

  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Complete'),
    COUNT(DISTINCT CASE WHEN oi.status = 'Complete' THEN oi.user_id END)
  ) AS items_per_buyer

FROM bigquery-public-data.thelook_ecommerce.order_items oi
JOIN bigquery-public-data.thelook_ecommerce.products p
  ON oi.product_id = p.id

GROUP BY
  product_category,
  product_name,
  retail_price;

--Category level cancellation risk

CREATE OR REPLACE TABLE analytics.category_cancellation_risk AS
SELECT
  p.product_category,
  COUNT(*) AS total_order_items,
  COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
  COUNTIF(oi.status != 'Cancelled') AS completed_items,

  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate

FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN analytics.dim_product p
  ON oi.product_id = p.product_id

GROUP BY 1
ORDER BY cancellation_rate DESC;

--1)Brand performance summary

CREATE OR REPLACE TABLE analytics.brand_performance_summary AS

SELECT
  p.brand,

  -- Volume metrics
  COUNT(*) AS total_order_items,
  COUNTIF(oi.status = 'Complete') AS completed_items,
  COUNTIF(oi.status = 'Cancelled') AS cancelled_items,
  COUNTIF(oi.status = 'Returned') AS returned_items,

  -- Revenue (only completed sales)
  SUM(CASE WHEN oi.status = 'Complete' THEN oi.sale_price ELSE 0 END) AS total_revenue,

  -- Customer metrics
  COUNT(DISTINCT CASE WHEN oi.status = 'Complete' THEN oi.user_id END) AS unique_buyers,

  -- Time coverage
  MIN(CASE WHEN oi.status = 'Complete' THEN oi.created_at END) AS first_sale_date,
  MAX(CASE WHEN oi.status = 'Complete' THEN oi.created_at END) AS last_sale_date,

  -- Derived metrics
  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Cancelled'),
    COUNT(*)
  ) AS cancellation_rate,

  SAFE_DIVIDE(
    COUNTIF(oi.status = 'Returned'),
    COUNTIF(oi.status = 'Complete') + COUNTIF(oi.status = 'Returned')
  ) AS return_rate,

  SAFE_DIVIDE(
    SUM(CASE WHEN oi.status = 'Complete' THEN oi.sale_price ELSE 0 END),
    COUNT(DISTINCT CASE WHEN oi.status = 'Complete' THEN oi.user_id END)
  ) AS revenue_per_buyer

FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN analytics.dim_product p
  ON oi.product_id = p.product_id

GROUP BY p.brand
ORDER BY total_revenue DESC;


--Monthly Revenue(Global).

SELECT
 DATE_TRUNC(DATE(created_at),MONTH)AS month,
 SUM(sale_price) AS total_revenue,
 COUNT(DISTINCT order_id) AS total_orders,
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE status != 'Cancelled' 
GROUP BY month
ORDER BY month;

--Monthly purchasing users

SELECT
 DATE_TRUNC(DATE(created_at), MONTH)AS month,
 COUNT(DISTINCT user_id) AS purchasing_users
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE status != 'Cancelled'
 AND user_id IS NOT NULL
GROUP BY month
ORDER BY month;   

--Monthly cancellation rate

SELECT
 DATE_TRUNC(DATE(created_at), MONTH)AS month,
 COUNT(CASE WHEN status = 'Cancelled' THEN 1 END)/COUNT(*) AS cancellation_rate
FROM  `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY month
ORDER BY month;

 --Revenue lost to cancellation

SELECT
 DATE_TRUNC(DATE(created_at),month) AS month,
 SUM(CASE WHEN status = 'Cancelled' THEN sale_price ELSE 0 END) AS cancelled_revenue
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY month
ORDER BY month;

--Revenue per order(Monthly)

SELECT
 DATE_TRUNC(DATE(created_at), MONTH) AS month,
 ROUND(SUM(sale_price), 2) AS total_revenue,
 COUNT(DISTINCT order_id) AS total_orders,
 ROUND(
  SAFE_DIVIDE(SUM(sale_price),
  COUNT(DISTINCT order_id)),2
 ) AS revenue_per_order
FROM `bigquery-public-data.thelook_ecommerce.order_items` 
WHERE status != 'Cancelled'
GROUP BY 1
ORDER BY month;

--Revenue per user(Monthly)

SELECT
 DATE_TRUNC(DATE(oi.created_at),month) AS month,
 ROUND(SUM(oi.sale_price),2) AS total_revenue,
 COUNT(DISTINCT oi.user_id) AS purchasing_users,
 ROUND(
  SAFE_DIVIDE(SUM(oi.sale_price),
  COUNT(DISTINCT oi.user_id)),
  2
 )AS revenue_per_user
FROM `bigquery-public-data.thelook_ecommerce.order_items`oi
WHERE oi.status != 'Cancelled'
GROUP BY 1
ORDER BY month;

--Aggregate country KPI's

CREATE OR REPLACE TABLE analytics.agg_country_kpis AS

WITH base AS(
  SELECT
   CASE
    WHEN u.country IN('España', 'Spain') THEN 'Spain'
    WHEN u.country IN ('Deutschland','Germany') THEN 'Germany'
    ELSE u.country END AS country,
   
   oi.order_id,
   oi.user_id,
   oi.sale_price,
   oi.status
  FROM
 `bigquery-public-data.thelook_ecommerce.order_items`oi 
  LEFT JOIN 
 `bigquery-public-data.thelook_ecommerce.users`u
   ON oi.user_id = u.id
)

SELECT
 country,

 --Core metrics
 ROUND(SUM(CASE WHEN status != 'Cancelled' THEN sale_price ELSE 0 END),2
 )AS total_revenue,
 
 COUNT(DISTINCT CASE WHEN status != 'Cancelled'THEN order_id END
 )AS total_orders,

 COUNT(DISTINCT CASE WHEN status != 'Cancelled' AND user_id IS NOT NULL THEN user_id END
 ) AS purchasing_users,

 --Efficency metrics
 ROUND(
  SAFE_DIVIDE(
    SUM(CASE WHEN status != 'Cancelled'THEN sale_price ELSE 0 END),
    COUNT(DISTINCT CASE WHEN status != 'Cancelled' THEN order_id END)
   ),2
  )AS revenue_per_order,

 ROUND(
   SAFE_DIVIDE(
     SUM(CASE WHEN status != 'Cancelled' THEN sale_price ELSE 0 END),
     COUNT(DISTINCT CASE WHEN status != 'Cancelled' AND user_id IS NOT NULL THEN user_id END)
    ),2
  ) AS revenue_per_user,


 --Operational metricS
 ROUND(
  SAFE_DIVIDE(
    COUNT(CASE WHEN status = 'Cancelled' THEN 1 END),
    COUNT(*)
  ),4
 ) AS cancellation_rate,

 ROUND(
  SUM(CASE WHEN status = 'Cancelled' THEN sale_price ELSE 0 END),2
 )AS cancelled_revenue
 
FROM base 
GROUP BY country
ORDER BY total_revenue DESC; 

 
--Aggregate category KPI's

CREATE OR REPLACE TABLE analytics.aggregate_category_kpis AS 

WITH base AS(
  SELECT
   p.category,
   oi.order_id,
   oi.user_id,
   oi.sale_price,
   oi.status
  FROM
 `bigquery-public-data.thelook_ecommerce.order_items`oi
  LEFT JOIN 
 `bigquery-public-data.thelook_ecommerce.products` p
   ON oi.product_id = p.id  
)

SELECT
 category,

 --Core metrics
 ROUND(SUM(CASE WHEN status != 'Cancelled' THEN sale_price ELSE 0 END),2) AS total_revenue,
 COUNT(DISTINCT CASE WHEN status != 'Cancelled' THEN order_id END)AS total_orders,
 COUNT(DISTINCT CASE WHEN status != 'Cancelled' AND user_id IS NOT NULL THEN user_id END)AS purchasing_users,

 --Efficiency metrics
 ROUND(
  SAFE_DIVIDE(
    SUM(CASE WHEN status != 'Cancelled' THEN sale_price ELSE 0 END),
    COUNT(DISTINCT CASE WHEN status != 'Cancelled' THEN order_id END)
    ),2
  )AS revenue_per_order,

 ROUND(
  SAFE_DIVIDE(
    SUM(CASE WHEN status!= 'Cancelled' AND user_id IS NOT NULL THEN sale_price ELSE 0 END),
    COUNT(DISTINCT CASE WHEN status != 'Cancelled' AND user_id IS NOT NULL THEN user_id END)
    ),2
  )AS revenue_per_user,
  
  --Operational metrics
  ROUND(
    SAFE_DIVIDE(
      COUNT(CASE WHEN status = 'Cancelled' THEN 1 END),
      COUNT(*)
      ),4
  )AS cancellation_rate,

  ROUND(
    SUM(CASE WHEN status = 'Cancelled' THEN sale_price ELSE 0 END),2
  )AS cancelled_revenue
FROM base
GROUP BY category
ORDER BY total_revenue DESC;

--**Final canonical events table

CREATE OR REPLACE TABLE analytics.fact_events_all AS

SELECT
 id AS event_id,
 user_id,
 session_id,
 event_type,
 created_at,
 DATE(created_at) AS event_date
FROM `bigquery-public-data.thelook_ecommerce.events`
WHERE event_type IN ('product', 'cart', 'purchase'); 


--Sanity check:1
SELECT
 count(*) AS total_events,
 COUNTIF(user_id IS NULL) AS null_user_events
 FROM analytics.fact_events_all;
--Sanity check:2
SELECT
 COUNT(DISTINCT session_id) AS sessions,
 COUNT(DISTINCT user_id) AS users
FROM analytics.fact_events_all; 

--Aggregate daily FUNNEL

CREATE OR REPLACE TABLE analytics.agg_daily_funnel AS
SELECT
 event_date,

 --Browsing(session-based)
 COUNT(DISTINCT IF(event_type = 'product', session_id, NULL)) AS product_view_sessions,
 COUNT(DISTINCT IF(event_type = 'cart', session_id, NULL)) AS add_to_cart_sessions,

 --Purchase(user-based)
 COUNT(DISTINCT IF(event_type = 'purchase',user_id, NULL)) AS purchasing_users,

 --Conversion rates
 SAFE_DIVIDE(
  COUNT(DISTINCT IF(event_type = 'cart', session_id, NULL)),
  COUNT(DISTINCT IF(event_type = 'product', session_id, NULL))
 )AS view_to_cart_rate,

 SAFE_DIVIDE(
  COUNT(DISTINCT IF (event_type = 'purchase', user_id, NULL)),
  COUNT(DISTINCT IF (event_type = 'cart',session_id, NULL))
 )AS cart_to_purchase_rate,

 SAFE_DIVIDE(
  COUNT(DISTINCT IF(event_type = 'purchase', user_id, NULL)),
  COUNT(DISTINCT IF(event_type = 'product', session_id, NULL))
 )AS view_to_purchase_rate
FROM analytics.fact_events_all
GROUP BY event_date
ORDER BY event_date;

--Aggregate monthly FUNNEL

CREATE OR REPLACE TABLE analytics.agg_monthly_funnel AS

SELECT
 DATE_TRUNC(event_date, month) AS month,

 SUM(product_view_sessions) AS product_view_sessions,
 SUM(add_to_cart_sessions) AS add_to_cart_sessions,
 SUM(purchasing_users) AS purchasing_users,

 SAFE_DIVIDE(
  SUM(add_to_cart_sessions),
  SUM(product_view_sessions)
 )AS view_to_cart_rate,

 SAFE_DIVIDE(
  SUM(purchasing_users),
  SUM(add_to_cart_sessions)
 )AS cart_to_purchase_rate,

 SAFE_DIVIDE(
  SUM(purchasing_users),
  SUM(product_view_sessions)
 )AS view_to_purchase_rate

FROM analytics.agg_daily_funnel
GROUP BY month
ORDER BY month;

SELECT table_name
FROM analytics.INFORMATION_SCHEMA.TABLES
ORDER BY table_name;


--Users based  on countries
CREATE OR REPLACE TABLE analytics.agg_country_user_base AS 
SELECT
 country,
 COUNT(DISTINCT user_id) AS total_users
FROM analytics.dim_user 
GROUP BY country;

--Final country performance

CREATE OR REPLACE TABLE analytics.final_country_performance AS 
WITH user_base AS(
  SELECT
   country,
   COUNT(DISTINCT user_id) AS total_users
  FROM analytics.dim_user
  GROUP BY country 
)

SELECT
 c.country,
 u.total_users,
 c.purchasing_users,
 c.total_revenue,
 SAFE_DIVIDE(c.purchasing_users, u.total_users) AS purchase_rate,
 SAFE_DIVIDE(c.total_revenue, c.purchasing_users) AS revenue_per_user,
 c.cart_to_purchase_conversion_pct,
 c.cancellation_rate
FROM analytics.country_performance_summary c
LEFT JOIN user_base u
ON c.country = u.country;

--Aggregate country user base
CREATE OR REPLACE TABLE analytics.agg_country_user_base AS
SELECT
  CASE 
    WHEN country = 'Deutschland' THEN 'Germany'
    ELSE country
  END AS country,
  COUNT(DISTINCT user_id) AS total_users
FROM analytics.dim_user
GROUP BY 1;

CREATE OR REPLACE TABLE analytics.final_country_performance AS
WITH cleaned_users AS (
  SELECT
    CASE 
      WHEN country = 'Deutschland' THEN 'Germany'
      ELSE country
    END AS country,
    user_id
  FROM analytics.dim_user
),

user_base AS (
  SELECT
    country,
    COUNT(DISTINCT user_id) AS total_users
  FROM cleaned_users
  GROUP BY country
),

cleaned_perf AS (
  SELECT
    CASE 
      WHEN country = 'Deutschland' THEN 'Germany'
      ELSE country
    END AS country,
    purchasing_users,
    total_revenue,
    cart_to_purchase_conversion_pct,
    cancellation_rate
  FROM analytics.country_performance_summary
)

SELECT
    p.country,
    u.total_users,
    p.purchasing_users,
    p.total_revenue,
    SAFE_DIVIDE(p.purchasing_users, u.total_users) AS purchase_rate,
    SAFE_DIVIDE(p.total_revenue, p.purchasing_users) AS revenue_per_user,
    p.cart_to_purchase_conversion_pct,
    p.cancellation_rate
FROM cleaned_perf p
LEFT JOIN user_base u
ON p.country = u.country;

--Revenue variance by country

SELECT
    du.country,
    COUNT(DISTINCT fo.user_id) AS users,
    SUM(foi.revenue) AS total_revenue,
    AVG(foi.revenue) AS avg_order_value
FROM analytics.fact_orders fo
JOIN analytics.fact_order_items foi
    ON fo.order_id = foi.order_id
JOIN analytics.dim_user du
    ON fo.user_id = du.user_id
GROUP BY du.country
ORDER BY total_revenue DESC;

--Revenue variance by Gender x Age

SELECT
    du.gender,
    du.age_group,
    COUNT(DISTINCT fo.user_id) AS users,
    SUM(foi.revenue) AS total_revenue,
    AVG(foi.revenue) AS avg_order_value
FROM analytics.fact_orders fo
JOIN analytics.fact_order_items foi
    ON fo.order_id = foi.order_id
JOIN analytics.dim_user du
    ON fo.user_id = du.user_id
GROUP BY du.gender, du.age_group
ORDER BY total_revenue DESC;

--Revenue distribition across users(Skew)

WITH user_revenue AS (
    SELECT
        fo.user_id,
        SUM(foi.revenue) AS total_revenue
    FROM analytics.fact_orders fo
    JOIN analytics.fact_order_items foi
        ON fo.order_id = foi.order_id
    GROUP BY fo.user_id
)

SELECT
    MIN(total_revenue) AS min_revenue,
    MAX(total_revenue) AS max_revenue,
    AVG(total_revenue) AS avg_revenue
FROM user_revenue;


--

WITH user_revenue AS (
    SELECT
        fo.user_id,
        du.country,
        SUM(foi.revenue) AS total_revenue
    FROM analytics.fact_orders fo
    JOIN analytics.fact_order_items foi
        ON fo.order_id = foi.order_id
    JOIN analytics.dim_user du
        ON fo.user_id = du.user_id
    GROUP BY fo.user_id, du.country
)

SELECT
    country,
    COUNT(*) AS users,
    MIN(total_revenue) AS min_revenue,
    AVG(total_revenue) AS avg_revenue,
    MAX(total_revenue) AS max_revenue
FROM user_revenue
GROUP BY country
ORDER BY avg_revenue DESC;


--Revenue by category

SELECT
    dp.product_category,
    COUNT(DISTINCT fo.user_id) AS users,
    SUM(foi.revenue) AS total_revenue,
    AVG(foi.revenue) AS avg_order_value
FROM analytics.fact_orders fo
JOIN analytics.fact_order_items foi
    ON fo.order_id = foi.order_id
JOIN analytics.dim_product dp
    ON foi.product_id = dp.product_id
GROUP BY dp.product_category
ORDER BY total_revenue DESC;

--skew within each product

WITH category_user_revenue AS (
    SELECT
        dp.product_category,
        fo.user_id,
        SUM(foi.revenue) AS total_revenue
    FROM analytics.fact_orders fo
    JOIN analytics.fact_order_items foi
        ON fo.order_id = foi.order_id
    JOIN analytics.dim_product dp
        ON foi.product_id = dp.product_id
    GROUP BY dp.product_category, fo.user_id
)

SELECT
    product_category,
    COUNT(*) AS users,
    AVG(total_revenue) AS avg_revenue,
    MAX(total_revenue) AS max_revenue
FROM category_user_revenue
GROUP BY product_category
ORDER BY avg_revenue DESC;

--Sanity check
SELECT 
EXTRACT(MONTH FROM month) AS month_number,
ROUND(AVG(revenue_mom_pct)*100, 2) AS avg_revenue_mom_pct,
ROUND(AVG(orders_mom_pct)*100, 2) AS avg_orders_mom_pct
FROM
analytics.agg_monthly_kpis_enriched
GROUP BY month_number
ORDER BY month_number;

SELECT
EXTRACT(YEAR FROM month) AS year,
EXTRACT(MONTH FROM month) AS month_number,
ROUND(AVG(revenue_mom_pct)*100, 2) AS rev_mom,
ROUND(AVG(orders_mom_pct)*100, 2)AS order_mom
FROM
analytics.agg_monthly_kpis_enriched
GROUP BY year, month_number
ORDER BY year, month_number;

SELECT
FORMAT_DATE('%b',month) AS month,
ROUND(AVG(revenue_mom_pct)*100, 2) AS avg_revenue_mom_pct,
ROUND(AVG(orders_mom_pct)*100, 2 ) AS avg_orders_mom_pct
FROM
analytics.agg_monthly_kpis_enriched
GROUP BY month
ORDER BY MIN(month);

SELECT
EXTRACT(MONTH FROM month) AS month,
ROUND(AVG(revenue_mom_pct),2) AS avg_rev_mom,
ROUND(AVG(orders_mom_pct),2) AS avg_orders_mom
FROM analytics.agg_monthly_kpis_enriched
WHERE EXTRACT(YEAR FROM month) BETWEEN 2020 AND 2025
GROUP BY month
ORDER BY month;


WITH base AS (
  SELECT
    EXTRACT(YEAR FROM month) AS yr,
    EXTRACT(MONTH FROM month) AS mon,
    revenue_mom_pct AS rev_mom,
    orders_mom_pct  AS ord_mom
  FROM analytics.agg_monthly_kpis_enriched
  WHERE revenue_mom_pct IS NOT NULL
    AND orders_mom_pct IS NOT NULL
    -- exclude incomplete 2026 months if you want stable seasonality
    AND EXTRACT(YEAR FROM month) BETWEEN 2020 AND 2025
)
SELECT
  mon,
  COUNT(*) AS year_month_points,
  SUM(CASE WHEN rev_mom > 0 THEN 1 ELSE 0 END) AS rev_pos_count,
  SUM(CASE WHEN rev_mom < 0 THEN 1 ELSE 0 END) AS rev_neg_count,
  AVG(rev_mom) AS avg_rev_mom,
  SUM(CASE WHEN ord_mom > 0 THEN 1 ELSE 0 END) AS ord_pos_count,
  SUM(CASE WHEN ord_mom < 0 THEN 1 ELSE 0 END) AS ord_neg_count,
  AVG(ord_mom) AS avg_ord_mom
FROM base
GROUP BY mon
ORDER BY mon;

SELECT
  EXTRACT(YEAR FROM month) AS yr,
  EXTRACT(MONTH FROM month) AS mon,
  revenue_mom_pct, orders_mom_pct
FROM analytics.agg_monthly_kpis_enriched
WHERE EXTRACT(YEAR FROM month) = 2026
ORDER BY month;

--LAG sanity check

WITH x AS (
  SELECT
    month,
    total_revenue,
    total_orders,
    LAG(total_revenue) OVER (ORDER BY month) AS prev_rev,
    LAG(total_orders)  OVER (ORDER BY month) AS prev_ord
  FROM analytics.agg_monthly_kpis_enriched
)
SELECT
  month,
  SAFE_DIVIDE(total_revenue - prev_rev, prev_rev) AS rev_mom_calc,
  SAFE_DIVIDE(total_orders  - prev_ord, prev_ord) AS ord_mom_calc
FROM x
WHERE prev_rev IS NOT NULL
ORDER BY month;



WITH base AS (
  SELECT
    country,
    EXTRACT(YEAR FROM month) AS yr,
    EXTRACT(MONTH FROM month) AS mon,
    revenue_mom_pct AS rev_mom,
    orders_mom_pct  AS ord_mom
  FROM analytics.agg_monthly_kpis_enriched
  WHERE revenue_mom_pct IS NOT NULL
    AND EXTRACT(YEAR FROM month) BETWEEN 2020 AND 2025
)
SELECT
  country,
  mon,
  AVG(rev_mom) AS avg_rev_mom,
  AVG(ord_mom) AS avg_ord_mom,
  COUNT(*) AS points
FROM base
WHERE country IN ('United States','China','Brazil')
GROUP BY country, mon
ORDER BY country, mon;



SELECT
EXTRACT(MONTH FROM oi.created_at) AS month,
p.category,
SUM(oi.sale_price) AS revenue
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.products` p
ON oi.product_id = p.id
WHERE oi.status IN ('Complete','Shipped','Processing')
GROUP BY month, category
ORDER BY month, revenue DESC;






