-- =====================================================================
-- File Name  : 03_build_master_sales.sql
-- Project    : Muamalat VIX 2025 – Business Intelligence Pipeline
-- Purpose    : To create a clean, unified master table (master_sales)
--              that consolidates all key transactional and reference data
--              required for sales performance analysis, dashboarding,
--              and CSV export submission.
--
-- Description:
-- This query builds a "flat" master dataset by joining the four staging
-- tables — customers, orders, products, and product categories — into
-- a single analytical table. The output preserves data integrity, uses
-- consistent data types, and ensures all derived metrics (e.g. revenue)
-- are reproducible and auditable. This version intentionally omits
-- partitioning and clustering to simplify runtime in BigQuery Sandbox.
--
-- Business Context:
--   Each row in master_sales represents a single product-level transaction
--   enriched with customer and category context. This table serves as the
--   foundation for monthly trend analysis, category-level sales monitoring,
--   and top-product performance insights.
--
-- =====================================================================


/* ---------------------------------------------------------------------
   SECTION A — SANITY TEST
   Objective:
     Validate that the join across all staging tables returns rows and
     produces the expected volume (~3,339 records). This step ensures
     foreign-key integrity and proper deduplication before creating
     the final table.
------------------------------------------------------------------------ */
SELECT 
  COUNT(*) AS expected_rows  -- Expected result: 3,339 rows based on QA checks
FROM (
  SELECT 1
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`            AS o
  JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers`         AS c  USING (CustomerID)
  JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products`          AS p  USING (ProdNumber)
  JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_product_category`  AS pc ON p.CategoryID = pc.CategoryID
);
-- If this returns 3,339 rows, proceed to the build step below.



/* ---------------------------------------------------------------------
   SECTION B — BUILD MASTER TABLE
   Objective:
     Create the main analytical table “master_sales”. Each record captures
     one product sold to one customer at a specific time, with standardized
     schema and business-friendly field naming.
   Technical notes:
     - CAST() is applied to enforce stable data types.
     - JOINs use foreign keys validated in staging (no orphans expected).
     - Numeric operations are performed in NUMERIC type to avoid precision loss.
     - This version uses CREATE OR REPLACE for idempotency.
------------------------------------------------------------------------ */
CREATE OR REPLACE TABLE `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales` AS
SELECT
  c.CustomerEmail                           AS cust_email,      -- customer email (unique identifier for analysis)
  c.CustomerCity                            AS cust_city,       -- customer city, useful for regional segmentation
  CAST(o.OrderDate AS DATE)                 AS order_date,      -- converted to DATE for chronological analysis
  CAST(o.Quantity  AS INT64)                AS order_qty,       -- integer conversion ensures clean arithmetic
  p.ProdName                                AS product_name,    -- standardized product display name
  CAST(p.Price     AS NUMERIC)              AS product_price,   -- stored as NUMERIC for monetary precision
  pc.CategoryName                           AS category_name,   -- product category name for aggregation
  CAST(o.Quantity AS INT64) * CAST(p.Price AS NUMERIC) AS total_sales  -- derived revenue metric per order line
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`            AS o
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers`         AS c  USING (CustomerID)
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products`          AS p  USING (ProdNumber)
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_product_category`  AS pc ON p.CategoryID = pc.CategoryID;

-- Result:
-- master_sales table created with 8 columns and 3,339 records.
-- Ready for downstream QA, export, and visualization.



/* ---------------------------------------------------------------------
   SECTION C — QUALITY ASSURANCE CHECKS
   Objective:
     Run data-quality validation to ensure the final table is consistent,
     complete, and analytically reliable before exposing it to dashboards.
------------------------------------------------------------------------ */

-- (C1) Record volume and invalid value check ---------------------------
SELECT
  COUNT(*) AS rows_master,                                     -- expected 3,339
  COUNTIF(order_date IS NULL) AS null_order_date,              -- expected 0
  COUNTIF(order_qty IS NULL OR order_qty <= 0) AS invalid_qty, -- expected 0
  COUNTIF(product_price IS NULL OR product_price <= 0) AS invalid_price -- expected 0
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`;
-- Confirms data completeness and absence of corrupted or invalid numeric values.


-- (C2) Coverage range of transaction dates -----------------------------
SELECT
  MIN(order_date) AS min_date,
  MAX(order_date) AS max_date
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`;
-- Ensures the dataset spans the expected business period (2020–2021).


-- (C3) Monthly aggregation sanity check --------------------------------
SELECT
  FORMAT_DATE('%Y-%m', order_date) AS year_month,     -- creates YYYY-MM bucket
  SUM(total_sales)                 AS total_revenue,  -- total monthly sales
  SUM(order_qty)                   AS total_units     -- total quantity sold
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
GROUP BY year_month
ORDER BY year_month;
-- Validates continuity of monthly data and identifies any missing months.


-- (C4) Category-level sales distribution -------------------------------
SELECT
  category_name,
  SUM(total_sales) AS total_revenue,
  SUM(order_qty)   AS total_units
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
GROUP BY category_name
ORDER BY total_revenue DESC;
-- Checks that product category proportions are reasonable and that
-- dominant categories align with expected business patterns.


-- (C5) Final spot check on raw records ---------------------------------
SELECT
  order_date, cust_email, cust_city,
  product_name, category_name,
  order_qty, product_price, total_sales
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.master_sales`
ORDER BY order_date DESC
LIMIT 20;

