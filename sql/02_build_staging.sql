-- =====================================================================
-- 02_build_staging.sql  (SIMPLE • one-shot • idempotent)
-- Purpose   : Build a clean, typed, and validated STAGING layer (stg_*)
-- Scope     : TRIM, SAFE_CAST, normalization, PK dedup, FK enforcement
-- Outputs   : stg_customers, stg_product_category, stg_products, stg_orders
-- QA Views  : v_stg_rowcounts, v_stg_schema_catalog
-- Project   : muamalat-vix-ptsb-2025
-- Dataset   : pt_sejahtera_bersama_bi
-- =====================================================================


/* =====================================================================
A) STAGING — CUSTOMERS
   Goal: Normalize text fields, enforce PK uniqueness, trim dirty strings.
===================================================================== */

CREATE OR REPLACE TABLE
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers` AS          -- final cleaned customers
SELECT * EXCEPT(_rn)                                                       -- drop helper column after dedup
FROM (
  SELECT
    SAFE_CAST(CustomerID AS INT64)                             AS CustomerID,          -- enforce numeric PK
    TRIM(CAST(FirstName AS STRING))                            AS FirstName,           -- remove leading/trailing spaces
    TRIM(CAST(LastName  AS STRING))                            AS LastName,            -- same for last name
    TRIM(CONCAT(COALESCE(FirstName,''),' ',COALESCE(LastName,''))) AS CustomerName,    -- readable full name
    LOWER(                                                                            -- normalize email casing
      REGEXP_REPLACE(                                                                 -- remove "mailto:" if any
        REGEXP_EXTRACT(CAST(CustomerEmail AS STRING), r'^[^#\s]+'),                   -- strip anchor fragments
        r'^(mailto:)', ''
      )
    )                                                          AS CustomerEmail,       -- cleaned email
    REGEXP_REPLACE(CAST(CustomerPhone AS STRING), r'\D', '')   AS CustomerPhone,       -- keep only digits
    TRIM(CAST(CustomerAddress AS STRING))                      AS CustomerAddress,     -- tidy address
    TRIM(CAST(CustomerCity AS STRING))                         AS CustomerCity,        -- normalized city name
    UPPER(TRIM(CAST(CustomerState AS STRING)))                 AS CustomerState,       -- uppercase standard
    TRIM(CAST(CustomerZip AS STRING))                          AS CustomerZip,         -- preserve leading zeros
    ROW_NUMBER() OVER (PARTITION BY SAFE_CAST(CustomerID AS INT64)
                       ORDER BY SAFE_CAST(CustomerID AS INT64)) AS _rn                 -- deduplicate by PK
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers`                 -- source: raw_customers
)
WHERE CustomerID IS NOT NULL AND _rn = 1;                                              -- exclude null PK, keep first


/* =====================================================================
B) STAGING — PRODUCT CATEGORY
   Goal: Standardize category labels and enforce PK uniqueness.
===================================================================== */

CREATE OR REPLACE TABLE
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_product_category` AS    -- final category dimension
SELECT * EXCEPT(_rn)
FROM (
  SELECT
    SAFE_CAST(CategoryID AS INT64)                          AS CategoryID,           -- typed PK
    TRIM(CAST(CategoryName AS STRING))                      AS CategoryName,         -- clean name
    UPPER(TRIM(CAST(CategoryAbbreviation AS STRING)))       AS CategoryAbbreviation, -- uppercase short code
    ROW_NUMBER() OVER (PARTITION BY SAFE_CAST(CategoryID AS INT64)
                       ORDER BY SAFE_CAST(CategoryID AS INT64)) AS _rn                -- dedup by PK
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category`          -- source: raw_product_category
)
WHERE CategoryID IS NOT NULL AND _rn = 1;                                              -- require valid PK


/* =====================================================================
C) STAGING — PRODUCTS
   Goal: Clean product catalog, enforce FK (category), filter valid price.
===================================================================== */

CREATE OR REPLACE TABLE
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products` AS             -- final product master
SELECT * EXCEPT(_rn)
FROM (
  SELECT
    TRIM(CAST(ProdNumber AS STRING))                       AS ProdNumber,   -- natural PK
    TRIM(CAST(ProdName AS STRING))                         AS ProdName,     -- product name
    SAFE_CAST(Category AS INT64)                           AS CategoryID,   -- FK → category
    SAFE_CAST(Price AS NUMERIC)                            AS Price,        -- numeric price
    ROW_NUMBER() OVER (PARTITION BY TRIM(CAST(ProdNumber AS STRING))
                       ORDER BY TRIM(CAST(ProdNumber AS STRING))) AS _rn    -- dedup by product code
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products`        -- source: raw_products
)
WHERE ProdNumber IS NOT NULL                                              -- must have PK
  AND CategoryID IS NOT NULL                                             -- must have FK
  AND Price IS NOT NULL AND Price > 0                                    -- positive numeric only
  AND _rn = 1;                                                           -- keep first per PK


/* =====================================================================
D) STAGING — ORDERS
   Goal: Parse flexible date formats, enforce FK to master tables,
         ensure positive quantities, guarantee referential integrity.
===================================================================== */

CREATE OR REPLACE TABLE
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders` AS
WITH base AS (
  SELECT
    SAFE_CAST(OrderID AS INT64) AS OrderID,                              -- typed PK
    COALESCE(                                                           -- tolerant multi-format date parser
      SAFE_CAST(`Date` AS DATE),
      SAFE.PARSE_DATE('%Y-%m-%d', CAST(`Date` AS STRING)),
      SAFE.PARSE_DATE('%m/%d/%Y', CAST(`Date` AS STRING)),
      SAFE.PARSE_DATE('%d/%m/%Y', CAST(`Date` AS STRING))
    )                             AS OrderDate,                          -- parsed date
    SAFE_CAST(CustomerID AS INT64) AS CustomerID,                        -- FK → stg_customers
    TRIM(CAST(ProdNumber AS STRING)) AS ProdNumber,                      -- FK → stg_products
    SAFE_CAST(Quantity AS INT64) AS Quantity                             -- numeric qty
  FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders`
)
SELECT
  o.OrderID,                                                             -- PK
  o.OrderDate,                                                           -- order date
  o.CustomerID,                                                          -- FK customer
  o.ProdNumber,                                                          -- FK product
  o.Quantity,                                                            -- order quantity
  FORMAT_DATE('%Y-%m', o.OrderDate) AS order_ym                          -- monthly key (YYYY-MM)
FROM base o
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers` c USING (CustomerID)    -- FK validation (customer)
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products`  p USING (ProdNumber)    -- FK validation (product)
WHERE o.OrderID IS NOT NULL AND o.OrderDate IS NOT NULL AND o.Quantity > 0;                 -- retain valid rows


/* =====================================================================
E) QA VIEW — ROWCOUNTS SNAPSHOT
   Goal: Reconcile raw vs staging row counts to detect anomalies.
===================================================================== */

CREATE OR REPLACE VIEW
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_stg_rowcounts` AS
SELECT 'raw_customers' AS name, COUNT(*) AS n FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_customers` UNION ALL
SELECT 'stg_customers', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers` UNION ALL
SELECT 'raw_product_category', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_product_category` UNION ALL
SELECT 'stg_product_category', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_product_category` UNION ALL
SELECT 'raw_products', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_products` UNION ALL
SELECT 'stg_products', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products` UNION ALL
SELECT 'raw_orders', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.raw_orders` UNION ALL
SELECT 'stg_orders', COUNT(*) FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`;  -- all should > 0


/* =====================================================================
F) QA VIEW — SCHEMA CATALOG
   Goal: Generate column-level metadata for documentation and README.
===================================================================== */

CREATE OR REPLACE VIEW
`muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_stg_schema_catalog` AS
SELECT
  table_name, column_name, data_type, is_nullable                          -- core metadata fields
FROM
  `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.INFORMATION_SCHEMA.COLUMNS`
WHERE
  table_name IN ('stg_customers','stg_product_category','stg_products','stg_orders')
ORDER BY
  table_name, column_name;                                                 -- consistent column order


/* =====================================================================
G) PREVIEW / QA VALIDATION BLOCK
   Goal: Post-build validation & documentation checks (SELECT-only)
===================================================================== */

-- Rowcount summary (reconcile raw → staging) --------------------------
SELECT * FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_stg_rowcounts`
ORDER BY name;                                               -- sorted alphabetically for clarity

-- Schema overview (structure documentation) ---------------------------
SELECT table_name, column_name, data_type, is_nullable
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.v_stg_schema_catalog`
ORDER BY table_name, column_name;                            -- grouped for easy scanning

-- Sanity check: null & integrity counts -------------------------------
SELECT
  COUNT(*)                               AS rows_stg_orders,   -- total valid orders
  COUNTIF(OrderID   IS NULL)             AS null_orderid,      -- PK nulls (should be 0)
  COUNTIF(OrderDate IS NULL)             AS null_orderdate,    -- date nulls (should be 0)
  COUNTIF(CustomerID IS NULL)            AS null_customerid,   -- FK nulls (should be 0)
  COUNTIF(ProdNumber IS NULL)            AS null_prodnumber    -- FK nulls (should be 0)
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`;

-- Data coverage snapshot ----------------------------------------------
SELECT
  MIN(OrderDate)                         AS min_order_date,    -- earliest order
  MAX(OrderDate)                         AS max_order_date,    -- latest order
  COUNT(DISTINCT CustomerID)             AS unique_customers,  -- unique customers in orders
  COUNT(DISTINCT ProdNumber)             AS unique_products    -- unique products in orders
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`;

-- Orphan recheck after FK enforcement ---------------------------------
SELECT
  SUM(CASE WHEN c.CustomerID IS NULL THEN 1 ELSE 0 END) AS orphan_orders_missing_customer,
  SUM(CASE WHEN p.ProdNumber  IS NULL THEN 1 ELSE 0 END) AS orphan_orders_missing_product
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders` o
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers` c USING (CustomerID)
LEFT JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products`  p USING (ProdNumber);

-- Spot samples (visual inspection) ------------------------------------
SELECT OrderDate, FORMAT_DATE('%Y-%m', OrderDate) AS ym, CustomerID, ProdNumber, Quantity
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders`
ORDER BY OrderDate DESC
LIMIT 10;                                   -- latest 10 orders

-- Top 10 cities by customer volume ------------------------------------
SELECT CustomerCity, COUNT(*) AS total_customers
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_customers`
GROUP BY CustomerCity
ORDER BY total_customers DESC
LIMIT 10;

-- Top 10 products by order frequency ----------------------------------
SELECT o.ProdNumber, p.ProdName, COUNT(*) AS order_lines
FROM `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_orders` o
JOIN `muamalat-vix-ptsb-2025.pt_sejahtera_bersama_bi.stg_products` p USING (ProdNumber)
GROUP BY o.ProdNumber, p.ProdName
ORDER BY order_lines DESC
LIMIT 10;
