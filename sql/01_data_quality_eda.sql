-- =====================================================================
-- 01_data_quality_eda.sql
-- Purpose   : Perform initial data profiling & data-quality diagnostics
-- Context   : PT Sejahtera Bersama (Rakamin VIX × Bank Muamalat)
-- Scope     : Read-only exploration - no CREATE/REPLACE/MUTATE
-- Project   : muamalat-vix-ptsb-2025
-- Dataset   : pt_sejahtera_bersama_bi
-- ---------------------------------------------------------------------
-- This script validates raw data integrity, completeness, and format
-- consistency before any staging or transformation step. Each section
-- can run independently for troubleshooting or documentation purposes.
-- =====================================================================


/* =====================================================================
   A) QUICK PREVIEW (HEAD)
   Goal: Verify that columns and sample values look reasonable.
   ===================================================================== */

SELECT * FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`  -- inspect customer schema & sample rows
LIMIT 5;

SELECT * FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`     -- sanity check orders format (dates, qty, IDs)
LIMIT 5;

SELECT * FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`   -- check product details & price columns
LIMIT 5;

SELECT * FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category`  -- validate product category reference table
LIMIT 5;



/* =====================================================================
   B) ROW COUNTS (TABLE SIZE)
   Goal: Confirm all source tables loaded completely (row-level sanity).
   ===================================================================== */

SELECT 'raw_customers' AS table_name, COUNT(*) AS n_rows
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`         -- each row represents one customer
UNION ALL
SELECT 'raw_orders', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`  -- transaction records
UNION ALL
SELECT 'raw_products', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`  -- product master data
UNION ALL
SELECT 'raw_product_category', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category`;  -- category lookup table



/* =====================================================================
   B2) SCHEMA SNAPSHOT
   Goal: Capture schema metadata for documentation & audit traceability.
   ===================================================================== */

SELECT
  table_name,                                    -- source table name
  column_name,                                   -- column label
  data_type,                                     -- BigQuery data type
  is_nullable                                   -- whether NULLs are allowed
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name IN ('raw_customers','raw_orders','raw_products','raw_product_category')
ORDER BY table_name, ordinal_position;           -- maintain column order for consistency



/* =====================================================================
   C) DATE RANGE & DISTINCT ENTITIES
   Goal: Understand transaction coverage (2020–2021) & entity diversity.
   ===================================================================== */

SELECT
  MIN(Date)                  AS min_order_date,       -- earliest available transaction
  MAX(Date)                  AS max_order_date,       -- most recent transaction
  COUNT(*)                   AS n_orders,             -- total transactions in dataset
  COUNT(DISTINCT CustomerID) AS unique_customers,     -- distinct customers who made orders
  COUNT(DISTINCT ProdNumber) AS unique_products       -- distinct products sold
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`;



/* =====================================================================
   D) MISSING VALUES & BASIC VALIDITY
   Goal: Detect null or invalid critical fields that could break joins.
   ===================================================================== */

-- D1) Customer completeness check
SELECT
  COUNTIF(CustomerEmail IS NULL OR TRIM(CustomerEmail) = '') AS null_email,  -- missing or blank email
  COUNTIF(CustomerCity  IS NULL OR TRIM(CustomerCity)  = '') AS null_city    -- missing or blank city
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`;

-- D2) Order field validation
SELECT
  COUNTIF(Date IS NULL)        AS null_date,        -- missing order date
  COUNTIF(CustomerID IS NULL)  AS null_customer,    -- missing customer foreign key
  COUNTIF(ProdNumber IS NULL)  AS null_prod,        -- missing product foreign key
  COUNTIF(Quantity IS NULL)    AS null_qty,         -- missing quantity value
  COUNTIF(Quantity <= 0)       AS non_positive_qty  -- negative or zero quantity
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`;

-- D3) Product field validation
SELECT
  COUNTIF(Category IS NULL) AS null_category,      -- missing category id
  COUNTIF(Price IS NULL)    AS null_price,         -- missing price
  COUNTIF(Price <= 0)       AS non_positive_price  -- invalid price (<= 0)
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`;



/* =====================================================================
   E) DUPLICATES (PRIMARY KEY UNIQUENESS)
   Goal: Ensure primary keys are unique — no duplicate IDs per entity.
   ===================================================================== */

WITH
orders_dup AS (
  SELECT COUNT(*) AS dup_rows
  FROM (SELECT OrderID FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`
        GROUP BY OrderID HAVING COUNT(*) > 1)  -- orders that appear multiple times
),
customers_dup AS (
  SELECT COUNT(*) AS dup_rows
  FROM (SELECT CustomerID FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`
        GROUP BY CustomerID HAVING COUNT(*) > 1)
),
products_dup AS (
  SELECT COUNT(*) AS dup_rows
  FROM (SELECT ProdNumber FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`
        GROUP BY ProdNumber HAVING COUNT(*) > 1)
),
categories_dup AS (
  SELECT COUNT(*) AS dup_rows
  FROM (SELECT CategoryID FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category`
        GROUP BY CategoryID HAVING COUNT(*) > 1)
)
SELECT 'raw_orders' AS table_name, (SELECT dup_rows FROM orders_dup) AS duplicate_rows
UNION ALL
SELECT 'raw_customers', (SELECT dup_rows FROM customers_dup)
UNION ALL
SELECT 'raw_products', (SELECT dup_rows FROM products_dup)
UNION ALL
SELECT 'raw_product_category', (SELECT dup_rows FROM categories_dup);



/* =====================================================================
   F) REFERENTIAL INTEGRITY (FOREIGN KEYS)
   Goal: Validate parent-child relationships across raw tables.
   ===================================================================== */

SELECT COUNT(*) AS orphan_orders_no_customer
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` o
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers` c
  ON o.CustomerID = c.CustomerID                       -- orders referencing non-existent customers
WHERE c.CustomerID IS NULL;

SELECT COUNT(*) AS orphan_orders_no_product
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` o
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` p
  ON o.ProdNumber = p.ProdNumber                       -- orders referencing missing products
WHERE p.ProdNumber IS NULL;

SELECT COUNT(*) AS orphan_products_no_category
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` p
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category` pc
  ON p.Category = pc.CategoryID                        -- products with invalid category link
WHERE pc.CategoryID IS NULL;



/* =====================================================================
   G) TYPE / FORMAT / WHITESPACE CHECKS
   Goal: Detect inconsistent formats or non-castable numeric fields.
   ===================================================================== */

-- Email validation using regex
SELECT
  COUNTIF(
    CustomerEmail IS NOT NULL
    AND NOT REGEXP_CONTAINS(LOWER(CustomerEmail), r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
  ) AS bad_email_format                               -- malformed email strings (invalid syntax)
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`;

-- Detect trailing/leading spaces in city names
SELECT
  COUNTIF(CustomerCity IS NOT NULL AND CustomerCity != TRIM(CustomerCity)) AS city_has_whitespace
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`;

-- Numeric casting validity
SELECT
  COUNTIF(CustomerID IS NOT NULL AND SAFE_CAST(CustomerID AS INT64) IS NULL) AS bad_customerid_cast  -- uncastable IDs
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`;

SELECT
  COUNTIF(Quantity IS NOT NULL AND SAFE_CAST(Quantity AS INT64) IS NULL) AS bad_quantity_cast,  -- invalid quantity
  COUNTIF(ProdNumber IS NOT NULL AND TRIM(ProdNumber) = '') AS empty_prodnumber                 -- empty product codes
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`;

SELECT
  COUNTIF(Price IS NOT NULL AND SAFE_CAST(Price AS NUMERIC) IS NULL) AS bad_price_cast,         -- invalid numeric price
  COUNTIF(Category IS NOT NULL AND SAFE_CAST(Category AS INT64) IS NULL) AS bad_category_cast   -- non-numeric category IDs
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`;



/* =====================================================================
   H) VALUE PROFILING & LIGHT OUTLIERS
   Goal: Quantify basic value ranges for numeric fields.
   ===================================================================== */

-- Quantity distribution (detect possible outliers)
SELECT
  MIN(Quantity) AS min_qty,                                -- smallest quantity sold
  APPROX_QUANTILES(Quantity, 4)[OFFSET(1)] AS q1_qty,      -- 25th percentile
  APPROX_QUANTILES(Quantity, 4)[OFFSET(2)] AS median_qty,  -- median (50th percentile)
  APPROX_QUANTILES(Quantity, 4)[OFFSET(3)] AS q3_qty,      -- 75th percentile
  MAX(Quantity) AS max_qty,                                -- highest quantity sold
  AVG(Quantity) AS mean_qty,                               -- average quantity per order
  STDDEV_POP(Quantity) AS std_qty                          -- variation across orders
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`;

-- Price distribution (detect pricing anomalies)
SELECT
  MIN(Price) AS min_price,                                 -- lowest product price
  APPROX_QUANTILES(Price,100)[OFFSET(25)] AS p25_price,    -- 25th percentile
  APPROX_QUANTILES(Price,100)[OFFSET(50)] AS median_price, -- median
  APPROX_QUANTILES(Price,100)[OFFSET(75)] AS p75_price,    -- 75th percentile
  MAX(Price) AS max_price,                                 -- highest price
  AVG(Price) AS mean_price,                                -- average price
  STDDEV_POP(Price) AS std_price,                          -- price variability
  COUNTIF(Price <= 0) AS non_positive_price_cnt            -- invalid pricing records
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`;



/* =====================================================================
   I) ID SEQUENCE GAPS (OPTIONAL)
   Goal: Detect missing IDs if sequence continuity matters.
   ===================================================================== */

WITH ids AS (
  SELECT CAST(OrderID AS INT64) AS oid
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`
),
rng AS (
  SELECT GENERATE_ARRAY(MIN(oid), MAX(oid)) AS arr FROM ids
)
SELECT missing_id
FROM rng, UNNEST(arr) AS missing_id
LEFT JOIN ids ON ids.oid = missing_id
WHERE ids.oid IS NULL
LIMIT 50;  -- show up to 50 missing IDs for inspection



/* =====================================================================
   J) BASIC EDA (TOP DISTRIBUTIONS)
   Goal: Identify dominant cities & top-selling products.
   ===================================================================== */

SELECT CustomerCity, COUNT(*) AS total_customers
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`
GROUP BY CustomerCity
ORDER BY total_customers DESC
LIMIT 10;  -- top 10 cities by customer count

SELECT ProdNumber, AVG(Quantity) AS avg_qty, SUM(Quantity) AS total_qty
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`
GROUP BY ProdNumber
ORDER BY total_qty DESC
LIMIT 10;  -- top 10 most purchased products



/* =====================================================================
   K) RELATIONSHIP COVERAGE
   Goal: Check coverage between entities (product vs order vs category).
   ===================================================================== */

SELECT COUNT(*) AS products_never_ordered
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` p
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` o
  ON o.ProdNumber = p.ProdNumber
WHERE o.ProdNumber IS NULL;  -- products never sold

SELECT COUNT(*) AS categories_without_products
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category` pc
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` p
  ON p.Category = pc.CategoryID
WHERE p.Category IS NULL;  -- categories not used by any product



/* =====================================================================
   L) SUMMARY SNAPSHOT
   Goal: Provide one-row overview for quick executive summary.
   ===================================================================== */

SELECT
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`) AS total_customers,   -- total # of customers
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`) AS total_orders,         -- total # of transactions
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`) AS total_products,     -- total # of products
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category`) AS total_categories, -- total # of product categories
  (SELECT COUNTIF(Quantity <= 0) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`) AS invalid_qty,  -- orders with invalid quantity
  (SELECT COUNTIF(Price <= 0) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`) AS invalid_price, -- products with invalid price
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` o
     LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers` c ON o.CustomerID = c.CustomerID
     WHERE c.CustomerID IS NULL) AS orphan_orders_no_customer,   -- orders without valid customer
  (SELECT COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` o
     LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` p ON o.ProdNumber = p.ProdNumber
     WHERE p.ProdNumber IS NULL) AS orphan_orders_no_product;    -- orders without valid product
