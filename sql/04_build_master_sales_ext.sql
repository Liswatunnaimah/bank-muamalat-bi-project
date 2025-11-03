-- =====================================================================
-- File     : 04_build_master_sales_ext.sql
-- Project  : Business Intelligence Pipeline
-- Purpose  : Provide an enriched, analysis-friendly sales table so that
--            recurring business logic (time breakdowns, price buckets,
--            customer lifecycle, and performance tiers) is consistent
--            and does not need to be re-authored in BI tools.
--
-- Design notes (why this matters):
-- • Centralizing derived attributes avoids metric drift across dashboards.
-- • Window functions are favored where row-level context is required
--   (e.g., first purchase anchor, repeat flag, customer LTV).
-- • LEFT JOINs are used for tier lookups to preserve all fact rows.
-- • The script is idempotent: re-running it safely refreshes the table.
-- =====================================================================


/* ========================= A) VALIDATION ==============================
   Intent: confirm the base table is healthy before enrichment, so any
   downstream diagnostics reflect business logic rather than data defects.
   Implication: if guards fail here, enrichment would only mask upstream issues.
====================================================================== */
SELECT
  COUNT(*) AS rows_master,                                  -- volume sanity check
  COUNTIF(order_date IS NULL) AS null_order_date,           -- date is the aggregation anchor
  COUNTIF(order_qty  IS NULL OR order_qty  <= 0) AS bad_qty,
  COUNTIF(product_price IS NULL OR product_price <= 0) AS bad_price
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`;


-- ====================== B) MATERIALIZATION ============================
-- Intent: build a reusable, analysis-grade table from the minimal master.
-- Implications:
-- • Tiers are computed from observed totals; if stakeholders need
--   year-specific ranking, switch the CTE toggle to the per-year variant.
-- • Order sequencing uses deterministic tie-breakers to keep rankings stable.
-- =====================================================================
CREATE OR REPLACE TABLE `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext` AS
WITH
-- ---------- Toggle for tiering granularity --------------------------------
-- Set this to TRUE to compute tiers per order_year; FALSE for all-time tiers.
params AS (SELECT FALSE AS tiers_per_year),

-- ---------- City performance (all-time) -----------------------------------
city_perf_all AS (
  SELECT cust_city, SUM(total_sales) AS city_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
  GROUP BY cust_city
),
city_tier_all AS (
  SELECT
    cust_city,
    city_sales,
    NTILE(3) OVER (ORDER BY city_sales DESC) AS city_ntile  -- 1=Top, 3=Emerging
  FROM city_perf_all
),

-- ---------- City performance (per-year) -----------------------------------
city_perf_year AS (
  SELECT EXTRACT(YEAR FROM order_date) AS order_year, cust_city, SUM(total_sales) AS city_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
  GROUP BY order_year, cust_city
),
city_tier_year AS (
  SELECT
    order_year,
    cust_city,
    city_sales,
    NTILE(3) OVER (PARTITION BY order_year ORDER BY city_sales DESC) AS city_ntile
  FROM city_perf_year
),

-- ---------- Category performance (all-time) -------------------------------
cat_perf_all AS (
  SELECT category_name, SUM(total_sales) AS category_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
  GROUP BY category_name
),
cat_tier_all AS (
  SELECT
    category_name,
    category_sales,
    NTILE(3) OVER (ORDER BY category_sales DESC) AS category_ntile
  FROM cat_perf_all
),

-- ---------- Category performance (per-year) --------------------------------
cat_perf_year AS (
  SELECT EXTRACT(YEAR FROM order_date) AS order_year, category_name, SUM(total_sales) AS category_sales
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
  GROUP BY order_year, category_name
),
cat_tier_year AS (
  SELECT
    order_year,
    category_name,
    category_sales,
    NTILE(3) OVER (PARTITION BY order_year ORDER BY category_sales DESC) AS category_ntile
  FROM cat_perf_year
),

-- ---------- Final selection with enrichment --------------------------------
base AS (
  SELECT
    ms.cust_email                                        AS cust_email,          -- customer key for CLV/RFM
    ms.cust_city                                         AS cust_city,           -- stable geo label
    ms.order_date                                        AS order_date,          -- daily grain retained
    ms.order_qty                                         AS order_qty,           -- line quantity
    ms.product_name                                      AS product_name,        -- for top-N analysis
    ms.product_price                                     AS product_price,       -- NUMERIC for money arithmetic
    ms.category_name                                     AS category_name,       -- category slice
    ms.total_sales                                       AS total_sales,         -- line revenue

    -- Time breakdowns: unified semantics for BI filters and sorting
    EXTRACT(YEAR  FROM ms.order_date)                    AS order_year,
    EXTRACT(MONTH FROM ms.order_date)                    AS order_month,
    FORMAT_DATE('%Y-%m', ms.order_date)                  AS order_ym,
    FORMAT_DATE('%a',   ms.order_date)                   AS dow_name,            -- weekday label (Mon..Sun)
    EXTRACT(DAYOFWEEK FROM ms.order_date)                AS dow_num,             -- 1=Sun..7=Sat (BQ convention)
    EXTRACT(WEEK FROM ms.order_date)                     AS week_of_year,

    -- Price buckets: explicit non-overlapping ranges
    CASE
      WHEN ms.product_price < 20   THEN 'Under 20'
      WHEN ms.product_price >= 20  AND ms.product_price < 50  THEN '20–49.99'
      WHEN ms.product_price >= 50  AND ms.product_price < 100 THEN '50–99.99'
      ELSE '100+'
    END                                                  AS price_bucket,

    -- Lifecycle: first purchase anchor and deterministic ordering
    MIN(ms.order_date) OVER (PARTITION BY ms.cust_email) AS first_order_date,
    DATE_DIFF(ms.order_date,
              MIN(ms.order_date) OVER (PARTITION BY ms.cust_email),
              DAY)                                       AS days_since_first,
    ROW_NUMBER() OVER (
      PARTITION BY ms.cust_email
      ORDER BY
        ms.order_date,
        ms.product_name,
        ms.category_name,
        ms.product_price,
        ms.total_sales,
        ms.cust_email                                    -- final tie-breaker to keep sequence stable
    )                                                    AS order_seq,
    CAST(
      ROW_NUMBER() OVER (
        PARTITION BY ms.cust_email
        ORDER BY
          ms.order_date,
          ms.product_name,
          ms.category_name,
          ms.product_price,
          ms.total_sales,
          ms.cust_email
      ) > 1 AS BOOL
    )                                                    AS is_repeat_customer,

    -- Customer revenue context: simple gross LTV and engagement proxy
    SUM(ms.total_sales) OVER (PARTITION BY ms.cust_email) AS customer_ltv_gross,
    COUNT(*)        OVER (PARTITION BY ms.cust_email)     AS customer_total_lines,
    SAFE_DIVIDE(
      SUM(ms.total_sales) OVER (PARTITION BY ms.cust_email),
      COUNT(*)          OVER (PARTITION BY ms.cust_email)
    )                                                    AS customer_avg_line_value
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales` ms
)

SELECT
  b.*,
  -- City tiers: choose per-year or all-time based on toggle
  COALESCE(
    CASE WHEN p.tiers_per_year
      THEN CASE cty_y.city_ntile WHEN 1 THEN 'Tier 1 (Top)' WHEN 2 THEN 'Tier 2' ELSE 'Tier 3 (Emerging)' END
    END,
    CASE cty_a.city_ntile WHEN 1 THEN 'Tier 1 (Top)' WHEN 2 THEN 'Tier 2' ELSE 'Tier 3 (Emerging)' END
  ) AS city_tier,
  COALESCE(
    CASE WHEN p.tiers_per_year THEN cty_y.city_sales END,
    cty_a.city_sales
  ) AS city_sales_total,

  -- Category tiers: same toggle pattern
  COALESCE(
    CASE WHEN p.tiers_per_year
      THEN CASE cat_y.category_ntile WHEN 1 THEN 'Tier 1 (Core)' WHEN 2 THEN 'Tier 2' ELSE 'Tier 3 (Long tail)' END
    END,
    CASE cat_a.category_ntile WHEN 1 THEN 'Tier 1 (Core)' WHEN 2 THEN 'Tier 2' ELSE 'Tier 3 (Long tail)' END
  ) AS category_tier,
  COALESCE(
    CASE WHEN p.tiers_per_year THEN cat_y.category_sales END,
    cat_a.category_sales
  ) AS category_sales_total

FROM base b
CROSS JOIN params p
LEFT JOIN city_tier_all  cty_a ON NOT p.tiers_per_year AND b.cust_city   = cty_a.cust_city
LEFT JOIN city_tier_year cty_y ON     p.tiers_per_year AND b.cust_city   = cty_y.cust_city
                                 AND b.order_year    = cty_y.order_year
LEFT JOIN cat_tier_all   cat_a ON NOT p.tiers_per_year AND b.category_name = cat_a.category_name
LEFT JOIN cat_tier_year  cat_y ON     p.tiers_per_year AND b.category_name = cat_y.category_name
                                 AND b.order_year    = cat_y.order_year
;


-- =========================== C) QUALITY ASSURANCE ======================
-- Intent: verify completeness, ranges, and rollup consistency so the
-- resulting table is safe to wire into dashboards and stakeholder reports.
-- ======================================================================

-- C1. Volume parity & null guards
SELECT
  COUNT(*) AS rows_ext,
  COUNTIF(order_date IS NULL)       AS null_order_date,
  COUNTIF(price_bucket IS NULL)     AS null_price_bucket,
  COUNTIF(first_order_date IS NULL) AS null_first_order_date,
  COUNTIF(city_tier IS NULL)        AS null_city_tier,
  COUNTIF(category_tier IS NULL)    AS null_category_tier
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`;

-- C2. Coverage window
SELECT MIN(order_date) AS min_date, MAX(order_date) AS max_date
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`;

-- C3. Repeat vs first-time distribution
SELECT is_repeat_customer, COUNT(*) AS row_count
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`
GROUP BY is_repeat_customer;

-- C4. Price bucket distribution
SELECT price_bucket, COUNT(*) AS line_count, SUM(total_sales) AS sales
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`
GROUP BY price_bucket
ORDER BY sales DESC;

-- C5. Monthly rollup with tiers (dashboard-ready check)
SELECT
  order_ym,
  city_tier,
  category_tier,
  SUM(total_sales) AS sales,
  SUM(order_qty)   AS qty
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`
GROUP BY order_ym, city_tier, category_tier
ORDER BY order_ym, city_tier, category_tier;

-- C6. Recent sample for quick inspection
SELECT
  order_date, order_ym, cust_email, cust_city, city_tier,
  product_name, category_name, category_tier,
  order_qty, product_price, price_bucket, total_sales,
  order_seq, is_repeat_customer, days_since_first,
  customer_ltv_gross, customer_avg_line_value
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales_ext`
ORDER BY order_date DESC
LIMIT 20;
