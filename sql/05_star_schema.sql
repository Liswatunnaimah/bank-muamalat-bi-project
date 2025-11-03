-- =====================================================================
-- File     : 05_star_schema.sql
-- Purpose  : Build a Kimball-style star schema on top of the curated
--            master layer so analytics can rely on conformed dimensions
--            and a single source of truth for measures.
--
-- Modeling choices and implications:
-- • Grain of fact table = one order line (each row in master_sales).
--   Implication: all measures (qty, revenue) are fully additive across
--   time, customer, product, and any dimensional cut.
-- • Surrogate keys are generated deterministically using INT64 hashes
--   of business keys. Implication: keys are stable across refreshes and
--   safe for joins; no dependency on auto-increment behavior.
-- • Product dimension is kept denormalized with category attributes to
--   maintain a pure star (no snowflaking), which keeps BI queries simple.
-- • Customer attributes are limited to what is consistently available
--   (email, city). Upgrades (e.g., gender, segment) can be added later
--   without breaking the fact grain.
-- =====================================================================


/* ========================= A) PRECHECKS ===============================
   Intent: verify the curated master exists and has a reasonable window.
   Implication: if these checks fail, stop here and fix upstream.
====================================================================== */
SELECT
  COUNT(*) AS rows_master,
  MIN(order_date) AS min_date,
  MAX(order_date) AS max_date
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`;


-- ====================== B) DIMENSIONS (CONFORMED) =====================
-- Notes:
-- • Surrogate INT64 keys are derived with ABS(FARM_FINGERPRINT(...)).
--   This yields stable integers and avoids collisions in small domains.
-- • All dimensions are rebuilt on each run (Type-1 semantics). When
--   richer history is required, upgrade customer/product to SCD-2.

-- ---------- B1) Date Dimension (role-playing calendar) ----------------
-- Rationale: BI tools repeatedly need Y/M/D, week, quarter, DoW, etc.
-- Capturing these once avoids divergent calculations and enables proper
-- sorting (e.g., months by number instead of lexicographic names).
CREATE OR REPLACE TABLE
  `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date` AS
WITH bounds AS (
  SELECT
    DATE '2020-01-01' AS dmin,   -- lower bound chosen from observed data
    DATE '2021-12-31' AS dmax    -- upper bound chosen from observed data
),
calendar AS (
  SELECT
    day AS full_date
  FROM bounds,
  UNNEST(GENERATE_DATE_ARRAY(dmin, dmax, INTERVAL 1 DAY)) AS day
)
SELECT
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) AS date_key,  -- surrogate in YYYYMMDD
  full_date                                      AS date,
  EXTRACT(YEAR  FROM full_date)                  AS year,
  EXTRACT(QUARTER FROM full_date)                AS quarter,
  EXTRACT(MONTH FROM full_date)                  AS month,
  FORMAT_DATE('%b', full_date)                   AS month_name_short,  -- Jan..Dec
  EXTRACT(DAY   FROM full_date)                  AS day_of_month,
  EXTRACT(DAYOFWEEK FROM full_date)              AS day_of_week_num,   -- 1=Sun..7=Sat
  FORMAT_DATE('%a', full_date)                   AS day_of_week_name,  -- Sun..Sat
  EXTRACT(WEEK  FROM full_date)                  AS week_of_year,
  CASE WHEN EXTRACT(DAYOFWEEK FROM full_date) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar;


-- ---------- B2) Customer Dimension -----------------------------------
-- Business key = normalized email. City is carried as a descriptive
-- attribute (changes overwrite: Type-1). If personal data policies
-- require tokenization later, the surrogate key still remains stable.
CREATE OR REPLACE TABLE
  `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_customer` AS
WITH src AS (
  SELECT DISTINCT
    LOWER(TRIM(cust_email)) AS bk_email,     -- normalization prevents duplicate natural keys
    TRIM(cust_city)         AS city
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
)
SELECT
  ABS(FARM_FINGERPRINT(bk_email)) AS customer_sk,  -- deterministic surrogate key
  bk_email                         AS customer_email,
  city                             AS city
FROM src;


-- ---------- B3) Product Dimension ------------------------------------
-- Business key = normalized product_name + category_name to avoid
-- accidental collisions. Category attributes are embedded to keep a
-- single hop star and simpler BI joins.
CREATE OR REPLACE TABLE
  `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_product` AS
WITH src AS (
  SELECT DISTINCT
    TRIM(product_name)  AS product_name,
    TRIM(category_name) AS category_name
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
)
SELECT
  ABS(FARM_FINGERPRINT(CONCAT(UPPER(product_name),'|',UPPER(category_name)))) AS product_sk,
  product_name,
  category_name
FROM src;


-- ============================== C) FACT ================================
-- Fact table at line-item grain with surrogate keys to dimensions.
-- Measures are fully additive; unit price is kept as a transactional
-- attribute to support price-mix analysis.

CREATE OR REPLACE TABLE
  `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales` AS
WITH base AS (
  SELECT
    -- Keys derived using the same normalization as dimension builds
    CAST(FORMAT_DATE('%Y%m%d', ms.order_date) AS INT64)                               AS date_key,
    ABS(FARM_FINGERPRINT(LOWER(TRIM(ms.cust_email))))                                  AS customer_sk,
    ABS(FARM_FINGERPRINT(CONCAT(UPPER(TRIM(ms.product_name)),'|',UPPER(TRIM(ms.category_name))))) AS product_sk,

    -- Measures (additive) and useful degenerate columns for traceability
    ms.order_qty,
    ms.total_sales,
    ms.product_price,
    ms.order_date,               -- degenerate for quick drill
    ms.cust_email,               -- degenerate for ad-hoc trace (avoid in BI joins)
    ms.product_name,             -- idem
    ms.category_name             -- idem
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales` ms
)
SELECT
  b.date_key,
  b.customer_sk,
  b.product_sk,
  b.order_qty,
  b.total_sales,
  b.product_price
FROM base b
-- Enforce referential integrity: inner joins ensure only rows with a valid
-- dimensional mapping are kept. If the data contract guarantees coverage,
-- this will retain all rows; otherwise a mismatch is a signal to fix dims.
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date`     d  ON d.date_key    = b.date_key
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_customer` dc ON dc.customer_sk = b.customer_sk
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_product`  dp ON dp.product_sk  = b.product_sk;


-- ============================= D) QA SUITE =============================
-- The following checks give immediate confidence that the star is sound
-- and safe to wire into dashboards and downstream semantic models.

-- D1) Row parity: fact rows should equal master rows (line grain)
SELECT
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`) AS rows_master,
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales`)   AS rows_fact;

-- D2) Null key guards in fact (should all be zero)
SELECT
  COUNTIF(date_key     IS NULL) AS null_date_key,
  COUNTIF(customer_sk  IS NULL) AS null_customer_sk,
  COUNTIF(product_sk   IS NULL) AS null_product_sk
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales`;

-- D3) Date window in fact (should match master)
SELECT MIN(d.date) AS min_date, MAX(d.date) AS max_date
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales` f
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date` d
  ON d.date_key = f.date_key;

-- D4) Re-aggregate sanity (fact → equals master aggregation)
WITH fact_rollup AS (
  SELECT
    FORMAT_DATE('%Y-%m', d.date) AS ym,
    SUM(f.total_sales) AS sales,
    SUM(f.order_qty)   AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales` f
  JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date` d
    ON d.date_key = f.date_key
  GROUP BY ym
),
master_rollup AS (
  SELECT
    FORMAT_DATE('%Y-%m', order_date) AS ym,
    SUM(total_sales) AS sales,
    SUM(order_qty)   AS qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
  GROUP BY ym
)
SELECT
  m.ym,
  m.sales AS master_sales,
  f.sales AS fact_sales,
  m.qty   AS master_qty,
  f.qty   AS fact_qty
FROM master_rollup m
JOIN fact_rollup   f USING (ym)
ORDER BY ym;

-- D5) Sample star join (demonstrates the intended usage pattern)
SELECT
  d.year,
  d.month,
  dp.category_name,
  SUM(f.total_sales) AS sales,
  SUM(f.order_qty)   AS qty
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.fact_sales` f
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_date`    d  ON d.date_key    = f.date_key
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.dim_product` dp ON dp.product_sk = f.product_sk
GROUP BY d.year, d.month, dp.category_name
ORDER BY d.year, d.month, dp.category_name;
