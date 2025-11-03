-- =====================================================================
-- File     : 07_advanced_analysis.sql
-- Purpose  : Advanced behavioral analytics for dashboards & decisioning:
--            RFM segmentation, monthly cohort retention, Pareto 80/20,
--            simple CLV (with explicit margin assumption), plus guardrails.
-- Dataset  : muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi
-- Notes    : All views depend on v_base_sales (single canonical join).
--            Keep calculations reproducible and formula-stable across BI.
-- =====================================================================


-- =====================================================================
-- A) RFM SEGMENTATION
--      Goal: quantify customer value & recency to prioritize lifecycle ops.
--      Frequency uses "distinct customer-day" as an order proxy (no order_id).
-- =====================================================================

CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_rfm` AS
WITH ref AS (
  SELECT MAX(order_date) AS ref_date                     -- freeze reference date for deterministic recency
  FROM   `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
),
cust AS (
  SELECT
    s.customer_sk                                        -- customer surrogate key (conformed across mart)
  , DATE_DIFF((SELECT ref_date FROM ref), MAX(s.order_date), DAY) AS recency_days  -- days since last purchase → freshness
  , COUNT(DISTINCT s.date_key)                           AS frequency_orders       -- distinct customer-day ≈ order count proxy
  , SUM(s.total_sales)                                   AS monetary_value         -- total spend across the observed window
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales` s
  GROUP BY s.customer_sk
),
scored AS (
  SELECT
    customer_sk                                          -- id
  , recency_days                                         -- raw recency (lower is better)
  , frequency_orders                                     -- raw frequency (higher is better)
  , monetary_value                                       -- raw monetary (higher is better)
  , (6 - NTILE(5) OVER (ORDER BY recency_days ASC)) AS r_score  -- invert so most recent = 5 (exec-friendly sort)
  , NTILE(5) OVER (ORDER BY frequency_orders ASC)   AS f_score  -- quintile frequency: 5 = most frequent buyers
  , NTILE(5) OVER (ORDER BY monetary_value  ASC)   AS m_score   -- quintile monetary:  5 = highest spenders
  FROM cust
)
SELECT
  customer_sk                                            -- id
, recency_days                                           -- feature for lifecycle timing (re-activation windows)
, frequency_orders                                       -- feature for engagement depth
, monetary_value                                         -- feature for value concentration analysis
, r_score, f_score, m_score                              -- normalized 1..5 scores for simple rule-based segments
, CASE                                                   -- pragmatic segmentation aligned to CRM playbooks
    WHEN r_score >=4 AND f_score >=4 AND m_score >=4 THEN 'Champions'           -- very recent, frequent, high value
    WHEN r_score >=3 AND f_score >=4                     THEN 'Loyal'            -- frequent & recent enough
    WHEN r_score >=4 AND f_score <=2 AND m_score <=2     THEN 'New Customers'    -- new/recent but low depth/value (nurture)
    WHEN r_score <=2 AND f_score >=3                     THEN 'At Risk'          -- cooling off despite decent history
    WHEN r_score =1  AND f_score <=2                     THEN 'Lost'             -- stale + low engagement/value
    ELSE 'Regulars'                                                            -- middle cluster; maintain with light touches
  END AS rfm_segment
FROM scored
; -- Why: this view feeds audience selection (retention, win-back, upsell) and can be trended monthly in BI.


-- =====================================================================
-- B) MONTHLY COHORT RETENTION (Tall format for BI pivot)
--      Goal: measure repeat activity after first purchase month by month.
--      Cohort = first purchase month; activity = any purchase in a month.
-- =====================================================================

CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_monthly_cohort` AS
WITH firsts AS (
  SELECT
    customer_sk                                                              -- id
  , FORMAT_DATE('%Y-%m', MIN(order_date)) AS cohort_ym                       -- first purchase month (YYYY-MM)
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY customer_sk
),
act AS (
  SELECT
    s.customer_sk                                                            -- id
  , FORMAT_DATE('%Y-%m', s.order_date) AS order_ym                           -- activity month (YYYY-MM)
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales` s
  GROUP BY s.customer_sk, order_ym
),
joined AS (
  SELECT
    f.cohort_ym                                                               -- cohort label
  , a.order_ym                                                                -- activity month
  , DATE_DIFF(PARSE_DATE('%Y-%m', a.order_ym), PARSE_DATE('%Y-%m', f.cohort_ym), MONTH) AS months_since_first
                                                                              -- elapsed months since first purchase
  , a.customer_sk                                                             -- id
  FROM firsts f
  JOIN act a USING (customer_sk)                                              -- safe: both sides at customer-grain
),
base AS (
  SELECT
    cohort_ym                                                                  -- cohort label
  , months_since_first                                                         -- 0..N (0 must be 100% by definition)
  , COUNT(DISTINCT customer_sk) AS active_customers                            -- active buyers in month k for cohort cohort_ym
  FROM joined
  WHERE months_since_first >= 0                                                -- guard: ignore potential data glitches
  GROUP BY cohort_ym, months_since_first
),
size AS (
  SELECT cohort_ym, active_customers AS cohort_size                            -- size at month 0 = baseline denominator
  FROM base
  WHERE months_since_first = 0
)
SELECT
  b.cohort_ym                                                                  -- cohort label
, b.months_since_first                                                         -- k = 0..N
, b.active_customers                                                           -- actives in month k
, s.cohort_size                                                                -- baseline size
, SAFE_DIVIDE(b.active_customers, s.cohort_size) AS retention_rate             -- retention% = actives / cohort_size
FROM base b
JOIN size s USING (cohort_ym)
; -- Why: tall layout keeps the view schema stable; pivoting can be done in BI for heatmaps without regenerating SQL.


-- =====================================================================
-- C) SALES TREND (Monthly headline + YoY + Index + Rolling)
--      Goal: one-stop trend panel for exec dashboards.
-- =====================================================================

CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_sales_trend` AS
WITH m AS (
  SELECT
    FORMAT_DATE('%Y-%m', order_date) AS order_ym          -- month bucket (YYYY-MM) aligned to other views
  , SUM(total_sales)                         AS sales      -- monthly revenue
  , SUM(order_qty)                           AS qty        -- monthly volume
  , COUNT(DISTINCT customer_sk)              AS active_customers
  , SAFE_DIVIDE(SUM(total_sales), SUM(order_qty)) AS asp   -- monthly ASP
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY order_ym
),
t AS (
  SELECT
    PARSE_DATE('%Y-%m', order_ym) AS d                    -- DATE for window calcs
  , order_ym, sales, qty, active_customers, asp
  , LAG(sales,12) OVER (ORDER BY PARSE_DATE('%Y-%m', order_ym)) AS sales_last_year  -- YoY comparator
  , LAG(sales)     OVER (ORDER BY PARSE_DATE('%Y-%m', order_ym)) AS sales_prev_month -- MoM comparator
FROM m
),
base AS (
  SELECT order_ym, sales
  FROM t
  ORDER BY d
  LIMIT 1                                                -- baseline = first month
)
SELECT
  t.order_ym                                             -- month bucket
, t.sales, t.qty, t.active_customers, t.asp              -- headline KPIs
, SAFE_DIVIDE(t.sales - t.sales_last_year, t.sales_last_year) AS sales_yoy_pct  -- YoY growth (NULL for first 12 months)
, SAFE_DIVIDE(t.sales - t.sales_prev_month, t.sales_prev_month) AS sales_mom_pct -- MoM growth
, SAFE_DIVIDE(t.sales, (SELECT sales FROM base)) AS sales_index                  -- index vs first month (1.0 = baseline)
, AVG(t.sales) OVER (ORDER BY t.d ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sales_ma3  -- 3M moving avg
FROM t
ORDER BY t.order_ym
; -- Why: concentrates trend context (YoY/MoM/Index/MA) in one view so BI doesn’t recompute windows.


-- =====================================================================
-- D) PARETO 80/20 — PRODUCT CONCENTRATION
--      Goal: identify SKUs that cumulatively contribute ~80% of revenue.
-- =====================================================================

CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_pareto_products` AS
WITH p AS (
  SELECT
    product_sk                                         -- stable key for joins/filters
  , ANY_VALUE(product_name)           AS product_name   -- display label (one-hop from star)
  , ANY_VALUE(category_name)          AS category_name  -- category for faceting
  , SUM(total_sales)                  AS sales          -- total revenue by product
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY product_sk
),
ranked AS (
  SELECT
    p.*
  , DENSE_RANK() OVER (ORDER BY sales DESC) AS sales_rank            -- reproducible ordering for top-N lists
  , SUM(sales) OVER ()                         AS sales_all          -- grand total for share computation
  , SUM(sales) OVER (ORDER BY sales DESC
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sales_cum -- cumulative revenue
  FROM p
)
SELECT
  product_sk, product_name, category_name, sales, sales_rank         -- product leaderboard
, SAFE_DIVIDE(sales,     sales_all)    AS sales_share                 -- product share of total revenue
, SAFE_DIVIDE(sales_cum, sales_all)    AS cumulative_share            -- cumulative Pareto curve
, (SAFE_DIVIDE(sales_cum, sales_all) <= 0.80) AS is_top_80_percent    -- boolean mask for 80% cut
FROM ranked
ORDER BY sales_rank
; -- Why: isolates the “vital few” SKUs; useful for assortment, promo, and inventory focus.


-- =====================================================================
-- E) SIMPLE CLV (REVENUE & MARGIN VIEW)
--      Goal: pragmatic lifetime value proxy from observed window.
--      Assumption: margin_rate = 30% (explicit & tweakable in BI).
-- =====================================================================

CREATE OR REPLACE VIEW `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_clv_simple` AS
WITH span AS (
  SELECT
    customer_sk                                                    -- id
  , MIN(order_date) AS first_purchase                              -- lifecycle start
  , MAX(order_date) AS last_purchase                               -- lifecycle recency
  , COUNT(DISTINCT date_key) AS orders_proxy                       -- distinct customer-day ≈ order count
  , SUM(total_sales)          AS revenue_total                      -- observed lifetime revenue
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
  GROUP BY customer_sk
),
calc AS (
  SELECT
    *
  , SAFE_DIVIDE(revenue_total, NULLIF(orders_proxy,0)) AS aov_proxy -- average order value proxy
  , DATE_DIFF(last_purchase, first_purchase, MONTH) AS tenure_months -- observed tenure (can be 0 for new)
  FROM span
)
SELECT
  customer_sk, first_purchase, last_purchase, tenure_months         -- lifecycle fields for segmentation
, orders_proxy, aov_proxy, revenue_total                            -- behavioral & value metrics
, 0.30 AS margin_rate                                               -- explicit assumption for transparency
, revenue_total * 0.30 AS clv_margin_estimate                       -- simple margin-based CLV proxy
FROM calc
; -- Why: keeps CLV assumption visible; BI can parameterize margin (e.g., by category) without SQL changes.


-- =====================================================================
-- F) GUARDRAILS — SANITY & PARITY CHECKS FOR ADVANCED ANALYTICS
--      Expected: all pass booleans = TRUE, mismatch counters = 0.
--      These are diagnostic queries (not views).
-- =====================================================================

-- F1) RFM coverage: every distinct customer appears exactly once in view_rfm.
WITH
base AS (
  SELECT COUNT(DISTINCT customer_sk) AS n_cust_base
  FROM   `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
),
rfm AS (
  SELECT
    COUNT(*) AS n_rows_rfm
  , COUNTIF(r_score IS NULL OR f_score IS NULL OR m_score IS NULL) AS n_null_scores
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_rfm`
)
SELECT
  (SELECT n_cust_base FROM base) = (SELECT n_rows_rfm FROM rfm) AS is_rfm_rowcount_match   -- TRUE if 1:1 coverage
, (SELECT n_null_scores FROM rfm) = 0                          AS is_rfm_scores_not_null   -- TRUE if scoring complete
;

-- F2) Cohort semantics: month 0 retention must be 100% per cohort; no negative k.
WITH
c AS (
  SELECT cohort_ym, months_since_first, retention_rate
  FROM   `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_monthly_cohort`
)
SELECT
  COUNTIF(months_since_first < 0)                    AS neg_k_rows                         -- should be 0
, COUNTIF(months_since_first = 0 AND ABS(retention_rate - 1.0) > 0.0001) AS bad_m0_rows     -- should be 0 (100% at k=0)
, (COUNTIF(months_since_first < 0) = 0
   AND COUNTIF(months_since_first = 0 AND ABS(retention_rate - 1.0) > 0.0001) = 0) AS is_cohort_semantics_ok
FROM c
;

-- F3) Pareto curve sanity: cumulative share must be monotonic and end ~1.0.
WITH p AS (
  SELECT
    product_sk, cumulative_share
  , LAG(cumulative_share) OVER (ORDER BY cumulative_share) AS prev_cum
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_pareto_products`
)
SELECT
  COUNTIF(prev_cum IS NOT NULL AND cumulative_share < prev_cum) AS non_monotone_steps  -- should be 0
, ABS(1.0 - MAX(cumulative_share))                        AS tail_gap_from_one          -- ~0 with float epsilon
, (COUNTIF(prev_cum IS NOT NULL AND cumulative_share < prev_cum) = 0
   AND ABS(1.0 - MAX(cumulative_share)) < 0.001)          AS is_pareto_curve_ok         -- TRUE if curve well-formed
FROM p
;

-- F4) CLV parity: sum of revenue_total across customers equals base revenue.
WITH
base AS (
  SELECT SUM(total_sales) AS sales_base
  FROM   `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_base_sales`
),
clv AS (
  SELECT SUM(revenue_total) AS sales_clv
  FROM   `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.view_clv_simple`
)
SELECT
  ROUND((SELECT sales_base FROM base) - (SELECT sales_clv FROM clv), 3) AS diff_sales_total  -- expect 0.000
, ABS((SELECT sales_base FROM base) - (SELECT sales_clv FROM clv)) < 0.001 AS is_clv_parity_ok -- TRUE if equal within epsilon
;
