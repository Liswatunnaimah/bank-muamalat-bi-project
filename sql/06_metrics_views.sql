-- =====================================================================
-- File     : 06_metrics_views.sql
-- Purpose  : Curate reusable, business-ready views for dashboards and
--            add lightweight guardrail tests to ensure metric parity.
-- Dataset  : muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi
-- Notes    : Views sit on top of the star schema (fact_sales + dims).
--            All metrics use a single base join to avoid formula drift.
-- =====================================================================


-- ---------------------------------------------------------------------
-- BASE VIEW — one canonical star join for all downstream metrics.
-- Why: centralize join semantics, calendar fields, and descriptors so
--      every view uses the same grain, keys, and column definitions.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales` AS
SELECT
  f.date_key,                                             -- numeric calendar key (YYYYMMDD) for stable joins
  d.date                         AS order_date,           -- natural DATE for filters/axes; timezone-agnostic
  d.year, d.month, d.quarter,                              -- canonical calendar breakdown expected by BI tools
  FORMAT_DATE('%Y-%m', d.date)   AS order_ym,             -- normalized monthly bucket (YYYY-MM) for trending
  d.day_of_week_num, d.day_of_week_name,                  -- DoW fields to analyze seasonality & ops cadence
  f.customer_sk,                                          -- conformed surrogate key (customer)
  dc.customer_email, dc.city,                              -- minimal descriptors; keep the PII surface small
  f.product_sk,                                           -- conformed surrogate key (product)
  dp.product_name, dp.category_name,                       -- one-hop labels from the star for usability
  f.order_qty,                                            -- fully additive volume measure across dimensions
  f.total_sales,                                          -- fully additive revenue measure across dimensions
  f.product_price                                         -- transactional attribute for price/mix analysis
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales`   f   -- atomic line-grain measures
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date`     d   ON d.date_key    = f.date_key     -- trusted calendar
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_customer` dc  ON dc.customer_sk = f.customer_sk  -- resolves city/email
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_product`  dp  ON dp.product_sk  = f.product_sk;  -- resolves product/category



-- =====================================================================
-- A) CORE KPI VIEWS (Daily, Monthly, AOV/ASP)
-- =====================================================================

-- Daily headline metrics — feeds daily trend cards/alerts.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_daily` AS
SELECT
  order_date,                                                                                 -- daily x-axis
  SUM(total_sales)                              AS sales,                                     -- daily revenue
  SUM(order_qty)                                AS qty,                                       -- daily units
  COUNT(DISTINCT customer_sk)                   AS active_customers,                          -- unique buyers per day
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp,                                       -- average selling price (sales/qty)
  SAFE_DIVIDE(
    SUM(total_sales),                                                                          -- numerator = daily sales
    COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING)))          -- proxy orders = distinct customer-day
  ) AS aov_proxy                                                                               -- AOV proxy when no explicit order_id
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY order_date;

-- Monthly rollup — primary source for executive KPI cards.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly` AS
SELECT
  order_ym,                                                                                    -- month bucket (YYYY-MM)
  MIN(order_date)                              AS month_start,                                 -- coverage sanity check
  MAX(order_date)                              AS month_end,                                   -- coverage sanity check
  SUM(total_sales)                              AS sales,                                      -- revenue per month
  SUM(order_qty)                                AS qty,                                        -- units per month
  COUNT(DISTINCT customer_sk)                   AS active_customers,                           -- unique buyers per month
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp                                         -- monthly ASP (sales/qty)
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY order_ym
ORDER BY order_ym;

-- Monthly AOV proxy — decoupled for clarity and reuse.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_aov_monthly` AS
WITH base AS (
  SELECT
    order_ym,                                                                                  -- month bucket
    customer_sk,                                                                               -- buyer
    date_key,                                                                                  -- day
    SUM(total_sales) AS sales_day_cust                                                         -- consolidate multi-line same customer-day
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym, customer_sk, date_key
)
SELECT
  order_ym,                                                                                   -- month bucket
  SUM(sales_day_cust) AS sales,                                                               -- numerator aligned to denominator
  COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING))) AS orders_proxy,  -- proxy orders
  SAFE_DIVIDE(SUM(sales_day_cust),
              COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING)))) AS aov_proxy  -- AOV proxy
FROM base
GROUP BY order_ym
ORDER BY order_ym;

-- Combined KPI view — single stop for overview cards (sales, qty, customers, ASP, AOV).
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_kpi_overview_monthly` AS
SELECT
  sm.order_ym,                                                                                -- month bucket
  sm.sales,
  sm.qty,
  sm.active_customers,
  sm.asp,
  am.aov_proxy
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly` sm                      -- core monthly metrics
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_aov_monthly` am                   -- AOV proxy (same month)
  ON am.order_ym = sm.order_ym;



-- =====================================================================
-- B) TIME & SEASONALITY
-- =====================================================================

-- Day-of-week lens — informs promo timing and staffing strategy.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_by_dow` AS
SELECT
  day_of_week_num,                                                                             -- numeric sort (1..7)
  day_of_week_name,                                                                            -- chart label
  SUM(total_sales)                              AS sales,                                      -- revenue by DoW
  SUM(order_qty)                                AS qty,                                        -- units by DoW
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp,                                        -- ASP by DoW
  COUNT(DISTINCT customer_sk)                   AS active_customers                            -- breadth of buyers by DoW
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY day_of_week_num, day_of_week_name
ORDER BY day_of_week_num;



-- =====================================================================
-- C) CATEGORY & PRODUCT PERFORMANCE
-- =====================================================================

-- Category health snapshot (revenue, volume, price efficiency).
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_category_performance` AS
SELECT
  category_name,                                                                                -- portfolio slice
  SUM(total_sales)                              AS sales,                                      -- total revenue
  SUM(order_qty)                                AS qty,                                        -- total units
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp                                         -- price-per-unit perspective
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY category_name
ORDER BY sales DESC;

-- Product leaderboard — dense_rank keeps Top-N stable for reporting.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_top_products` AS
SELECT
  product_name,                                                                               -- display field
  category_name,                                                                              -- slicer
  SUM(total_sales)                              AS sales,                                     -- total revenue
  SUM(order_qty)                                AS qty,                                       -- total units
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp,                                       -- price-per-unit
  DENSE_RANK() OVER (ORDER BY SUM(total_sales) DESC) AS sales_rank                            -- stable ranking for Top-N tables
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY product_name, category_name
ORDER BY sales DESC;

-- Monthly category mix share — supports stacked area / 100% bars and portfolio shift analysis.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_mix_share_monthly` AS
WITH m AS (
  SELECT
    order_ym,                                                                                 -- month bucket
    category_name,                                                                            -- category slice
    SUM(total_sales) AS sales,                                                                -- category revenue
    SUM(order_qty)   AS qty                                                                   -- category units
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym, category_name
),
t AS (
  SELECT
    order_ym,                                                                                 -- month bucket
    SUM(sales) AS sales_total,                                                                -- monthly total revenue
    SUM(qty)   AS qty_total                                                                   -- monthly total units
  FROM m
  GROUP BY order_ym
)
SELECT
  m.order_ym,                                                                                 -- month bucket
  m.category_name,                                                                            -- category slice
  m.sales,                                                                                    -- absolute revenue
  m.qty,                                                                                      -- absolute units
  SAFE_DIVIDE(m.sales, t.sales_total) AS sales_share,                                         -- revenue composition per month
  SAFE_DIVIDE(m.qty,   t.qty_total)   AS qty_share                                            -- volume composition per month
FROM m
JOIN t USING (order_ym)
ORDER BY m.order_ym, m.category_name;



-- =====================================================================
-- D) CITY / GEO PERFORMANCE
-- =====================================================================

-- City-level performance — for geo bars/maps and regional segmentation.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_city_performance` AS
SELECT
  city,                                                                                       -- city label
  SUM(total_sales)                              AS sales,                                     -- revenue by city
  SUM(order_qty)                                AS qty,                                       -- units by city
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp,                                       -- price-per-unit by city
  COUNT(DISTINCT customer_sk)                   AS unique_customers                           -- unique buyer count
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY city
ORDER BY sales DESC;

-- City × Category × Month — detect regional leaders and seasonality.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_city_category_monthly` AS
SELECT
  order_ym,                                                                                    -- month bucket
  city,                                                                                        -- geo slice
  category_name,                                                                               -- category slice
  SUM(total_sales) AS sales,                                                                   -- revenue
  SUM(order_qty)   AS qty                                                                      -- units
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY order_ym, city, category_name
ORDER BY order_ym, city, category_name;



-- =====================================================================
-- E) PRICE & CUSTOMER BEHAVIOR
-- =====================================================================

-- Price distribution — uses buckets precomputed in master_sales_ext (compute once).
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_price_bucket_distribution` AS
SELECT
  price_bucket,                                                                                -- Under 20, 20–49.99, 50–99.99, 100+
  COUNT(*)                           AS line_count,                                            -- number of lines in bucket
  SUM(total_sales)                   AS sales,                                                 -- revenue contributed
  SUM(order_qty)                     AS qty,                                                   -- units contributed
  SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(order_qty),0)) AS asp                               -- price-per-unit at bucket level
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`
GROUP BY price_bucket
ORDER BY sales DESC;

-- Customer activity profile — shows long-tail vs whales.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_customer_activity` AS
SELECT
  customer_sk,                                                                                -- buyer key
  MIN(order_date)                   AS first_purchase,                                        -- lifecycle anchor
  MAX(order_date)                   AS last_purchase,                                         -- recency anchor
  COUNT(*)                          AS line_count,                                            -- line-grain engagement
  COUNT(DISTINCT order_ym)          AS active_months,                                         -- breadth of activity
  SUM(order_qty)                    AS qty,                                                   -- total units
  SUM(total_sales)                  AS sales,                                                 -- total spend
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp                                        -- customer-level ASP
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY customer_sk;



-- =====================================================================
-- F) AOV / ASP BREAKDOWNS (Category, City)
-- =====================================================================

-- AOV proxy per month × category — depth of demand by portfolio.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_aov_category_monthly` AS
WITH base AS (
  SELECT
    order_ym,                                                                                  -- month bucket
    category_name,                                                                             -- category slice
    customer_sk,                                                                               -- buyer
    date_key,                                                                                  -- day
    SUM(total_sales) AS sales_day_cust_cat                                                     -- consolidate multi-line same day
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym, category_name, customer_sk, date_key
)
SELECT
  order_ym,                                                                                    -- month bucket
  category_name,                                                                               -- category slice
  SUM(sales_day_cust_cat) AS sales,                                                            -- aligned numerator
  COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING))) AS orders_proxy, -- proxy orders
  SAFE_DIVIDE(SUM(sales_day_cust_cat),
              COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING)))) AS aov_proxy -- AOV proxy
FROM base
GROUP BY order_ym, category_name
ORDER BY order_ym, category_name;

-- ASP per month × category — separate price/mix lens.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_asp_category_monthly` AS
SELECT
  order_ym,                                                                                    -- month bucket
  category_name,                                                                               -- category slice
  SUM(total_sales)                              AS sales,                                      -- revenue
  SUM(order_qty)                                AS qty,                                        -- units
  SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp                                         -- monthly category ASP
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY order_ym, category_name
ORDER BY order_ym, category_name;

-- AOV proxy per month × city — regional purchasing power/behavior.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_aov_city_monthly` AS
WITH base AS (
  SELECT
    order_ym,                                                                                  -- month bucket
    city,                                                                                      -- geo slice
    customer_sk,                                                                               -- buyer
    date_key,                                                                                  -- day
    SUM(total_sales) AS sales_day_cust_city                                                    -- consolidate multi-line same day
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym, city, customer_sk, date_key
)
SELECT
  order_ym,                                                                                    -- month bucket
  city,                                                                                        -- geo slice
  SUM(sales_day_cust_city) AS sales,                                                           -- aligned numerator
  COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING))) AS orders_proxy, -- proxy orders
  SAFE_DIVIDE(SUM(sales_day_cust_city),
              COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING)))) AS aov_proxy -- AOV proxy
FROM base
GROUP BY order_ym, city
ORDER BY order_ym, city;

-- Minimal daily rollup — handy for quick trend drills without extra cols.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_daily_min` AS
SELECT
  order_date,                                                                                 -- day
  SUM(total_sales) AS sales,                                                                   -- revenue
  SUM(order_qty)   AS qty                                                                      -- units
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
GROUP BY order_date
ORDER BY order_date;



-- =====================================================================
-- G) HELPER VIEWS — YoY, MoM, Index, Rolling Avg
-- =====================================================================

-- Monthly YoY comparator & growth — quick context for trend panels.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly_yoy` AS
WITH m AS (
  SELECT
    PARSE_DATE('%Y-%m', order_ym) AS d,                                                       -- DATE for window functions
    sales, qty, active_customers, asp                                                         -- core monthly metrics
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
)
SELECT
  FORMAT_DATE('%Y-%m', d) AS order_ym,                                                        -- back to string bucket
  sales, qty, active_customers, asp,                                                          -- current period
  LAG(sales,12) OVER (ORDER BY d) AS sales_last_year,                                         -- comparator
  SAFE_DIVIDE(sales - LAG(sales,12) OVER (ORDER BY d),
              LAG(sales,12) OVER (ORDER BY d)) AS sales_yoy_pct,                              -- YoY growth
  LAG(qty,12)   OVER (ORDER BY d) AS qty_last_year,
  SAFE_DIVIDE(qty - LAG(qty,12) OVER (ORDER BY d),
              LAG(qty,12) OVER (ORDER BY d))   AS qty_yoy_pct
FROM m;

-- Monthly MoM growth — complementary to YoY for recent dynamics.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly_mom` AS
WITH m AS (
  SELECT PARSE_DATE('%Y-%m', order_ym) AS d, sales, qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
)
SELECT
  FORMAT_DATE('%Y-%m', d) AS order_ym,                                                        -- month bucket
  sales,
  qty,
  LAG(sales) OVER (ORDER BY d) AS sales_prev_month,                                           -- comparator
  SAFE_DIVIDE(sales - LAG(sales) OVER (ORDER BY d),
              LAG(sales) OVER (ORDER BY d)) AS sales_mom_pct,                                 -- MoM growth
  LAG(qty)   OVER (ORDER BY d) AS qty_prev_month,
  SAFE_DIVIDE(qty - LAG(qty) OVER (ORDER BY d),
              LAG(qty) OVER (ORDER BY d))   AS qty_mom_pct
FROM m
ORDER BY order_ym;

-- Monthly Index (base = first month) — normalize trajectories (base = 1.0).
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly_index` AS
WITH m AS (
  SELECT PARSE_DATE('%Y-%m', order_ym) AS d, sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
),
b AS (
  SELECT sales AS base_sales FROM m ORDER BY d LIMIT 1                                        -- baseline = first available month
)
SELECT
  FORMAT_DATE('%Y-%m', d) AS order_ym,                                                        -- month bucket
  sales,
  SAFE_DIVIDE(sales, (SELECT base_sales FROM b)) AS sales_index                               -- normalized index
FROM m
ORDER BY order_ym;

-- Rolling 3-month moving average — smooths volatility for exec readouts.
CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly_rolling3` AS
WITH m AS (
  SELECT PARSE_DATE('%Y-%m', order_ym) AS d, sales, qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
)
SELECT
  FORMAT_DATE('%Y-%m', d) AS order_ym,                                                        -- month bucket
  sales,
  AVG(sales) OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sales_ma3,         -- 3-month MA including current
  qty,
  AVG(qty)   OVER (ORDER BY d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_ma3
FROM m
ORDER BY order_ym;


-- =====================================================================
-- H) GUARDRAIL TESTS — Parity & Coverage (diagnostic queries only)
--      Expected: mismatches = 0, missing months = [].
-- =====================================================================

-- H1) Sales/Qty parity: v_sales_monthly vs recompute from v_base_sales.
WITH fact AS (
  SELECT
    FORMAT_DATE('%Y-%m', order_date) AS order_ym,                                 -- recomputed month bucket (YYYY-MM)
    SUM(total_sales) AS sales,                                                    -- recomputed monthly revenue
    SUM(order_qty)   AS qty                                                       -- recomputed monthly units
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`              -- consistent base join for parity
  GROUP BY order_ym
)
SELECT
  COUNTIF(ABS(f.sales - v.sales) > 0.001) AS sales_mismatch_months,               -- should be 0; epsilon for float math
  COUNTIF(ABS(f.qty   - v.qty)   > 0.001) AS qty_mismatch_months                  -- should be 0
FROM fact f
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly` v USING (order_ym);

-- H2) AOV parity: v_aov_monthly vs exact proxy recomputation (customer-day).
WITH base AS (
  SELECT
    FORMAT_DATE('%Y-%m', order_date) AS order_ym,                                 -- month bucket
    customer_sk, date_key,                                                        -- buyer-day proxy for "order"
    SUM(total_sales) AS sales_day_cust                                            -- consolidate multi-line same customer-day
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym, customer_sk, date_key
),
recalc AS (
  SELECT
    order_ym,
    SUM(sales_day_cust) AS sales,                                                 -- numerator aligned to denominator
    COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING)))
      AS orders_proxy,                                                            -- proxy "orders" = distinct customer-day
    SAFE_DIVIDE(SUM(sales_day_cust),
                COUNT(DISTINCT CONCAT(CAST(customer_sk AS STRING),'|',CAST(date_key AS STRING))))
      AS aov_proxy                                                                -- recomputed AOV proxy
  FROM base
  GROUP BY order_ym
)
SELECT
  COUNTIF(ABS(r.aov_proxy - v.aov_proxy) > 0.001) AS aov_mismatch_months          -- should be 0
FROM recalc r
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_aov_monthly` v USING (order_ym);

-- H3) Month coverage: enforce the 2020–2021 window used by the dataset.
--     Scalar subqueries avoid “aggregate without FROM” errors.
WITH months AS (
  SELECT FORMAT_DATE('%Y-%m', d) AS ym
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2021-12-31', INTERVAL 1 MONTH)) AS d   -- expected 24 months
),
missing AS (
  SELECT ym FROM months
  EXCEPT DISTINCT
  SELECT order_ym FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`           -- actual coverage
)
SELECT
  (SELECT COUNT(*) FROM months)                                        AS expected_months,        -- should be 24
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`)
                                                                      AS actual_months,          -- months present
  (SELECT ARRAY_AGG(ym ORDER BY ym) FROM missing)                      AS missing_months,         -- [] if no gaps
  ((SELECT COUNT(*) FROM months) =
   (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`))
                                                                      AS is_month_count_match,   -- quick pass/fail boolean
  ((SELECT COUNT(*) FROM missing) = 0)                                 AS is_no_gap               -- quick pass/fail boolean
;

-- =====================================================================
-- H4) ROW-COUNT & SUM PARITY — ensure dimensional views sum back to base.
--      Goal: prevent metric drift (dimension totals ≠ base join totals).
--      Expectation: every diff = 0 (within small floating-point tolerance).
-- =====================================================================

-- H4a) Global parity: totals in dimensional views must match v_base_sales totals.
WITH
base AS (
  SELECT
    SUM(total_sales) AS sales,                                    -- baseline revenue at the atomic line grain
    SUM(order_qty)   AS qty                                       -- baseline units at the atomic line grain
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
),
cat AS (
  SELECT SUM(sales) AS sales, SUM(qty) AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_category_performance`
),
city AS (
  SELECT SUM(sales) AS sales, SUM(qty) AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_city_performance`
),
prod AS (
  SELECT SUM(sales) AS sales, SUM(qty) AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_top_products`
)
SELECT
  ROUND((SELECT sales FROM base) - (SELECT sales FROM cat), 3)  AS diff_sales_category,   -- 0 if category composition is complete
  ROUND((SELECT qty   FROM base) - (SELECT qty   FROM cat), 3)  AS diff_qty_category,     -- 0 if no dupes/missing in category view
  ROUND((SELECT sales FROM base) - (SELECT sales FROM city), 3) AS diff_sales_city,        -- 0 if city coverage is exact
  ROUND((SELECT qty   FROM base) - (SELECT qty   FROM city), 3) AS diff_qty_city,          -- 0 if no “orphan” rows by city
  ROUND((SELECT sales FROM base) - (SELECT sales FROM prod), 3) AS diff_sales_product,     -- 0 if product aggregation is correct
  ROUND((SELECT qty   FROM base) - (SELECT qty   FROM prod), 3) AS diff_qty_product        -- 0 if no double-count on product
;  -- Practice: reference this row in the README as the “global parity check”.

-- H4b) Monthly parity: each month’s composition must equal the monthly headline.
WITH
b AS (  -- Recompute from base for cross-control against v_sales_monthly
  SELECT
    FORMAT_DATE('%Y-%m', order_date) AS order_ym,                   -- consistent monthly bucket across views
    SUM(total_sales) AS sales,                                       -- baseline revenue
    SUM(order_qty)   AS qty                                          -- baseline units
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym
),
v AS (
  SELECT order_ym, sales, qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
),
mix AS (  -- Category composition by month must sum to the headline by month
  SELECT order_ym, SUM(sales) AS sales, SUM(qty) AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_mix_share_monthly`
  GROUP BY order_ym
),
j_base_vs_v AS (
  SELECT b.order_ym, b.sales AS b_sales, v.sales AS v_sales, b.qty AS b_qty, v.qty AS v_qty
  FROM b JOIN v USING (order_ym)                                        -- months should align 1:1
),
j_mix_vs_v AS (
  SELECT mix.order_ym, mix.sales AS m_sales, v.sales AS v_sales, mix.qty AS m_qty, v.qty AS v_qty
  FROM mix JOIN v USING (order_ym)                                      -- composition should sum to headline
)
SELECT
  -- Count months with mismatches (epsilon 0.001 for float tolerance)
  (SELECT COUNTIF(ABS(b_sales - v_sales) > 0.001) FROM j_base_vs_v) AS mismatch_months_base_vs_v_sales,   -- ideally 0
  (SELECT COUNTIF(ABS(b_qty   - v_qty)   > 0.001) FROM j_base_vs_v) AS mismatch_months_base_vs_v_qty,     -- ideally 0
  (SELECT COUNTIF(ABS(m_sales - v_sales) > 0.001) FROM j_mix_vs_v)  AS mismatch_months_mix_vs_v_sales,    -- ideally 0
  (SELECT COUNTIF(ABS(m_qty   - v_qty)   > 0.001) FROM j_mix_vs_v)  AS mismatch_months_mix_vs_v_qty,      -- ideally 0

  -- List offending months to speed up root cause analysis
  (SELECT ARRAY_AGG(order_ym ORDER BY order_ym)
     FROM j_base_vs_v WHERE ABS(b_sales - v_sales) > 0.001) AS offending_months_base_vs_v_sales,          -- [] when healthy
  (SELECT ARRAY_AGG(order_ym ORDER BY order_ym)
     FROM j_base_vs_v WHERE ABS(b_qty   - v_qty)   > 0.001) AS offending_months_base_vs_v_qty,
  (SELECT ARRAY_AGG(order_ym ORDER BY order_ym)
     FROM j_mix_vs_v  WHERE ABS(m_sales - v_sales) > 0.001) AS offending_months_mix_vs_v_sales,
  (SELECT ARRAY_AGG(order_ym ORDER BY order_ym)
     FROM j_mix_vs_v  WHERE ABS(m_qty   - v_qty)   > 0.001) AS offending_months_mix_vs_v_qty
;  -- Practice: if any mismatches appear, review view sources (joins/filters) or calendar coverage in dim_date/fact.



-- =====================================================================
-- H5) DATA HEALTH SENTINELS — early detection of implausible values.
--      Goal: surface anomalies (negatives, non-positive, nulls) and
--            provide quick sample rows for fast debugging.
-- =====================================================================

-- H5a) Health summary at the atomic line grain.
WITH base AS (
  SELECT
    COUNT(*)                                         AS rows_total,                          -- total line-item rows
    COUNTIF(order_qty <= 0)                          AS rows_qty_nonpositive,                -- qty <= 0 is implausible for sales
    COUNTIF(total_sales < 0)                         AS rows_sales_negative,                 -- negative revenue (refund?) → review
    COUNTIF(product_price <= 0)                      AS rows_price_nonpositive,              -- non-positive unit price is invalid here
    COUNTIF(order_date IS NULL)                      AS rows_date_null                       -- date required for time-series
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
)
SELECT
  rows_total,                                                                                 -- dataset scale
  rows_qty_nonpositive,                                                                       -- should be 0 for this teaching dataset
  rows_sales_negative,                                                                        -- should be 0 (unless refund logic exists)
  rows_price_nonpositive,                                                                     -- should be 0 (price bucketed upstream)
  rows_date_null,                                                                             -- should be 0 (dim_date enforced)
  (rows_qty_nonpositive = 0 AND rows_sales_negative = 0 AND rows_price_nonpositive = 0 AND rows_date_null = 0)
    AS is_basic_health_ok                                                                     -- TRUE when all guards pass
FROM base
;  -- Practice: capture this result in the README to demonstrate data quality discipline.

-- H5b) Problematic row samples (max 5 per category) for quick investigation.
--      Tip: if any appear, trace back to stg_* and master_sales.
(
  SELECT
    'qty_nonpositive' AS issue,                                                               -- issue label
    order_date, customer_sk, product_sk, order_qty, product_price, total_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  WHERE order_qty <= 0
  ORDER BY order_date DESC
  LIMIT 5
)
UNION ALL
(
  SELECT
    'sales_negative' AS issue,
    order_date, customer_sk, product_sk, order_qty, product_price, total_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  WHERE total_sales < 0
  ORDER BY order_date DESC
  LIMIT 5
)
UNION ALL
(
  SELECT
    'price_nonpositive' AS issue,
    order_date, customer_sk, product_sk, order_qty, product_price, total_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  WHERE product_price <= 0
  ORDER BY order_date DESC
  LIMIT 5
)
;  -- Practice: during demos, show this table to make QA visible; “no data” means no offending rows were found.

-- H5c) Null-rate sanity for derived KPIs in v_sales_monthly.
SELECT
  COUNTIF(asp IS NULL)                    AS null_asp_months,                                  -- null when qty=0; expect 0
  COUNTIF(asp IS NOT NULL AND asp < 0)    AS neg_asp_months,                                   -- negative price/unit is implausible
  COUNT(*)                                 AS months_total,                                     -- denominator context
  (COUNTIF(asp IS NULL) = 0 AND COUNTIF(asp IS NOT NULL AND asp < 0) = 0) AS is_kpi_semantics_ok
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_sales_monthly`
;  -- Practice: if any null/negative appears, locate the months with zero qty or definition drift.
