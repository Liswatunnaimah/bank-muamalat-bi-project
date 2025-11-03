# PT Sejahtera Bersama — Data Dictionary

**Tools:** Google BigQuery, Looker Studio
**Dataset:** `pt_sejahtera_bersama_bi`
**Purpose:** Central reference for schema, transformations, metrics, and business definitions supporting the analytical data mart and dashboard for PT Sejahtera Bersama.

---

## 0. Data Architecture Overview

| Layer           | Description                                                                              | Output Tables                                                         |
| :-------------- | :--------------------------------------------------------------------------------------- | :-------------------------------------------------------------------- |
| **Raw**         | Direct extraction from CSV files (`Customers`, `Products`, `ProductCategory`, `Orders`). | `raw_customers`, `raw_products`, `raw_orders`, `raw_product_category` |
| **Staging**     | Cleansing, typing, normalization, and data validation.                                   | `stg_customers`, `stg_products`, `stg_orders`, `stg_product_category` |
| **Master**      | Consolidated transactional table combining customer, product, and order details.         | `master_sales`                                                        |
| **Extended**    | Enriched table with derived behavioral attributes (e.g., tenure, LTV, repeat flag).      | `master_sales_ext`                                                    |
| **Star Schema** | Analytical model separating facts and dimensions for query efficiency.                   | `dim_*`, `fact_sales`                                                 |
| **Views**       | Semantic metric layer for BI tools (Looker Studio).                                      | `v_*`                                                                 |

---

## 1. Source Mapping

| Source File           | Columns Extracted                                                           | Target Table   |
| :-------------------- | :-------------------------------------------------------------------------- | :------------- |
| `Customers.csv`       | `cust_email`, `cust_city`, `city_tier`, `customer_type`                     | `dim_customer` |
| `Products.csv`        | `product_id`, `product_name`, `product_price`, `category_id`                | `dim_product`  |
| `ProductCategory.csv` | `category_id`, `category_name`                                              | `dim_category` |
| `Orders.csv`          | `order_id`, `order_date`, `customer_id`, `product_id`, `qty`, `total_sales` | `fact_sales`   |

---

## 2. Output Tables & Data Layers

| Layer    | Table          | Purpose                                              | Grain                         | Primary Key                                         | Foreign Keys                                                                    | Source                                       |
| :------- | :------------- | :--------------------------------------------------- | :---------------------------- | :-------------------------------------------------- | :------------------------------------------------------------------------------ | :------------------------------------------- |
| **Dim**  | `dim_date`     | Unified calendar dimension for temporal aggregation. | 1 row per date                | `date_key (INT64)`                                  | —                                                                               | Generated (date range 2020–2021)             |
| **Dim**  | `dim_customer` | Type-1 dimension for customer attributes.            | 1 row per email               | `customer_sk (INT64)`                               | —                                                                               | `master_sales (cust_email, cust_city)`       |
| **Dim**  | `dim_product`  | Product + category dimension (denormalized).         | 1 row per (product, category) | `product_sk (INT64)`                                | —                                                                               | `master_sales (product_name, category_name)` |
| **Fact** | `fact_sales`   | Transactional fact table at **line-item grain**.     | 1 row per transaction line    | composite (`date_key`, `customer_sk`, `product_sk`) | `date_key → dim_date`, `customer_sk → dim_customer`, `product_sk → dim_product` | `master_sales`                               |

---

## 3. Table & Column Definitions

### 3.1 `dim_date` (Date Dimension)

**Grain:** 1 row per calendar date
**Primary Key:** `date_key (INT64)` = `YYYYMMDD`

| Column             | Type   | Description                     | Formula                                           | Source    |
| :----------------- | :----- | :------------------------------ | :------------------------------------------------ | :-------- |
| `date_key`         | INT64  | Surrogate key for date          | `CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64)` | Generated |
| `date`             | DATE   | Calendar date                   | —                                                 | Generated |
| `year`             | INT64  | Year                            | `EXTRACT(YEAR FROM date)`                         | Generated |
| `quarter`          | INT64  | Quarter (1-4)                   | `EXTRACT(QUARTER FROM date)`                      | Generated |
| `month`            | INT64  | Month (1-12)                    | `EXTRACT(MONTH FROM date)`                        | Generated |
| `month_name_short` | STRING | Month abbreviation              | `FORMAT_DATE('%b', date)`                         | Generated |
| `day_of_month`     | INT64  | Day in month                    | `EXTRACT(DAY FROM date)`                          | Generated |
| `day_of_week_num`  | INT64  | Day of week (1 = Sun … 7 = Sat) | `EXTRACT(DAYOFWEEK FROM date)`                    | Generated |
| `day_of_week_name` | STRING | Day name                        | `FORMAT_DATE('%a', date)`                         | Generated |
| `week_of_year`     | INT64  | Week number                     | `EXTRACT(WEEK FROM date)`                         | Generated |
| `is_weekend`       | BOOL   | Weekend flag                    | `CASE WHEN DAYOFWEEK IN (1, 7)`                   | Generated |

---

### 3.2 `dim_customer` (Customer Dimension)

**Grain:** 1 row per normalized email
**Primary Key:** `customer_sk (INT64)`

| Column           | Type   | Description                             | Formula                                          | Source         |
| :--------------- | :----- | :-------------------------------------- | :----------------------------------------------- | :------------- |
| `customer_sk`    | INT64  | Surrogate key (customer)                | `ABS(FARM_FINGERPRINT(LOWER(TRIM(cust_email))))` | `master_sales` |
| `customer_email` | STRING | Normalized email                        | `LOWER(TRIM(cust_email))`                        | `master_sales` |
| `city`           | STRING | Latest customer city (Type-1 overwrite) | `TRIM(cust_city)`                                | `master_sales` |

---

### 3.3 `dim_product` (Product Dimension)

**Grain:** 1 row per (product_name, category_name)
**Primary Key:** `product_sk (INT64)`

| Column          | Type   | Description             | Formula                                                  | Source                           |                |
| :-------------- | :----- | :---------------------- | :------------------------------------------------------- | :------------------------------- | -------------- |
| `product_sk`    | INT64  | Surrogate key (product) | `ABS(FARM_FINGERPRINT(CONCAT(UPPER(TRIM(product_name)),' | ',UPPER(TRIM(category_name)))))` | `master_sales` |
| `product_name`  | STRING | Product name            | `TRIM(product_name)`                                     | `master_sales`                   |                |
| `category_name` | STRING | Product category        | `TRIM(category_name)`                                    | `master_sales`                   |                |

---

### 3.4 `fact_sales` (Sales Fact)

**Grain:** 1 row per transaction line
**Foreign Keys:** `date_key → dim_date`, `customer_sk → dim_customer`, `product_sk → dim_product`

| Column          | Type    | Description      | Formula                                            | Source         |
| :-------------- | :------ | :--------------- | :------------------------------------------------- | :------------- |
| `date_key`      | INT64   | Calendar key     | `CAST(FORMAT_DATE('%Y%m%d', order_date) AS INT64)` | `master_sales` |
| `customer_sk`   | INT64   | Customer key     | `ABS(FARM_FINGERPRINT(LOWER(TRIM(cust_email))))`   | `master_sales` |
| `product_sk`    | INT64   | Product key      | see `dim_product` hash                             | `master_sales` |
| `order_qty`     | INT64   | Quantity sold    | `qty`                                              | `master_sales` |
| `product_price` | NUMERIC | Unit price       | `product_price`                                    | `master_sales` |
| `total_sales`   | NUMERIC | Line sales value | `order_qty × product_price`                        | `master_sales` |

---

## 4. Semantic Views & Core Metrics

> All metrics views (`v_*`) are derived from `v_base_sales` to prevent metric drift and ensure consistent logic.

### 4.1 `v_base_sales`

**Purpose:** Unified view joining fact and dimensions, enriched with temporal and categorical labels.
**Key Fields:** `order_date`, `order_ym`, `customer_sk`, `product_sk`, `category_name`, `city`, `order_qty`, `total_sales`, `product_price`, `day_of_week_*`, `year`, `month`, `quarter`.

---

### 4.2 Metric Dictionary

| Metric                      | Definition                              | Formula (using `v_base_sales`)                          | Additivity     | Notes / Anti-Patterns                                    |                                             |
| :-------------------------- | :-------------------------------------- | :------------------------------------------------------ | :------------- | :------------------------------------------------------- | ------------------------------------------- |
| **Sales (Revenue)**         | Total revenue from transactions         | `SUM(total_sales)`                                      | Fully additive | Do not mix with `master_sales_ext` for headline figures. |                                             |
| **Quantity (Units)**        | Total units sold                        | `SUM(order_qty)`                                        | Fully additive | —                                                        |                                             |
| **Active Customers**        | Unique customers per period             | `COUNT(DISTINCT customer_sk)`                           | Non-additive   | Requires re-distinct across time buckets.                |                                             |
| **ASP (Avg Selling Price)** | Average unit price                      | `SAFE_DIVIDE(SUM(total_sales), SUM(order_qty))`         | —              | Use `SAFE_DIVIDE` to avoid NULL on zero qty.             |                                             |
| **AOV (proxy)**             | Avg order value (proxy by customer-day) | `SUM(total_sales) / COUNT(DISTINCT CONCAT(customer_sk,' | ',date_key))`  | —                                                        | Use customer-day proxy since no `order_id`. |
| **YoY %**                   | Growth vs same month last year          | `(sales – LAG(sales, 12)) / LAG(sales, 12)`             | —              | Needs ≥ 13 months coverage.                              |                                             |
| **MoM %**                   | Growth vs previous month                | `(sales – LAG(sales)) / LAG(sales)`                     | —              | —                                                        |                                             |
| **Index (base 1.0)**        | Normalized trend index                  | `sales / base_month_sales`                              | —              | Base = first month in series.                            |                                             |
| **MA3**                     | 3-month moving average                  | `AVG(sales) OVER (ROWS –2 TO CURRENT)`                  | —              | Smooths volatility.                                      |                                             |
| **Category Mix Share**      | Category sales share per month          | `category_sales / total_month_sales`                    | —              | Ensure month coverage complete.                          |                                             |
| **Top Product Rank**        | Revenue ranking by SKU                  | `DENSE_RANK() OVER (ORDER BY SUM(sales) DESC)`          | —              | Equal values share rank.                                 |                                             |
| **City ASP/AOV**            | City-level pricing behavior             | `SUM(sales)/SUM(qty)` and AOV proxy                     | —              | Compare regional affordability.                          |                                             |

---

### 4.3 Advanced Analytical Views

**`view_rfm`** – RFM Segmentation

* *Recency:* `DATE_DIFF(ref_date, last_purchase, DAY)`
* *Frequency:* `COUNT(DISTINCT date_key)`
* *Monetary:* `SUM(total_sales)`
* *Score:* `NTILE(5)` per dimension (R reversed).
* *Segment:* `Champions`, `Loyal`, `New`, `At Risk`, `Lost`, `Regulars`.

**`view_monthly_cohort`** – Cohort Retention

* *Cohort:* `MIN(order_date)` (`%Y-%m`)
* *Retention Rate:* `active_customers / cohort_size`
* *Usage:* Evaluate customer repeat behavior over time.

**`view_sales_trend`** – Executive Panel

* Consolidates headline KPIs (sales, qty, customers, ASP, AOV) with YoY, MoM, Index, and MA3.

**`view_pareto_products`** – Pareto 80/20 Analysis

* Computes cumulative revenue share and flags `is_top_80_percent`.
* Identifies high-impact products for promotion and inventory focus.

**`view_clv_simple`** – Customer Lifetime Value (Heuristic)

* *Inputs:* `orders_proxy`, `aov_proxy`, `tenure_months`, `revenue_total`
* *Assumed margin:* 30% → `clv_margin_estimate = revenue_total × 0.30`
* *Purpose:* Customer valuation for retention prioritization.

---

## 5. Business Glossary

| Term                 | Definition                                                            |                                            |
| :------------------- | :-------------------------------------------------------------------- | ------------------------------------------ |
| **Order Line**       | Single transaction line (product, qty, price); base grain of fact.    |                                            |
| **Order (Proxy)**    | Represented by unique `customer + date` since no explicit `order_id`. |                                            |
| **Customer**         | Identified by normalized email address.                               |                                            |
| **Product**          | Specific item combined with its category for stable key generation.   |                                            |
| **Category**         | Product group; stored denormalized within `dim_product`.              |                                            |
| **City**             | Latest city captured per customer (Type-1 overwrite).                 |                                            |
| **Sales / Revenue**  | Monetary value of transactions (`total_sales`).                       |                                            |
| **Quantity / Units** | Number of units sold (`order_qty`).                                   |                                            |
| **ASP**              | Average Selling Price = `sales / qty`.                                |                                            |
| **AOV (Proxy)**      | `sales / count(distinct customer_sk                                   | date_key)` — customer-day proxy for order. |
| **Active Customer**  | Distinct `customer_sk` within time bucket.                            |                                            |
| **Cohort**           | Month of first purchase (`MIN(order_date)` formatted `%Y-%m`).        |                                            |
| **Retention Rate**   | % of original cohort remaining active after k months.                 |                                            |
| **Pareto 80/20**     | Subset of products contributing ≈ 80% of total revenue.               |                                            |
| **CLV (Simple)**     | Observed revenue × 30% margin over data window.                       |                                            |

---

## 6. Data Quality & QA Checks

### 6.1 Star Schema Integrity

* Row parity: `COUNT(fact_sales) = COUNT(master_sales)`
* Null guard: `date_key`, `customer_sk`, `product_sk` must be non-NULL
* Date window alignment: min/max dates match between fact and master
* Re-aggregation check: Aggregated fact = aggregated master (by sales, qty)

### 6.2 Metric Validation

* H1 Sales/Qty parity between `v_sales_monthly` and `v_base_sales`
* H2 AOV definition consistent across views
* H3 Coverage: 2020-01 → 2021-12 (24 months) no missing periods
* H4 Totals in dimensional views reconcile to base total
* H5 Sanity: no negative qty/price/sales; ASP not NULL or negative

### 6.3 Advanced Analytics Checks

* RFM: one row per customer, no NULL scores
* Cohort: `k = 0` = 100% retention, no negative intervals
* Pareto: cumulative share monotonic ≤ 1.0
* CLV: `SUM(revenue_total)` matches `SUM(total_sales)` from base

---

## 7. Column-Level Data Quality Rules

* `fact_sales.order_qty` > 0 (INT64)
* `fact_sales.product_price` > 0 (NUMERIC)
* `fact_sales.total_sales` = `order_qty × product_price` ≥ 0
* `dim_customer.customer_email` = `LOWER(TRIM())`, valid email format
* `dim_product.product_name/category_name` = `TRIM()`, non-empty
* `dim_date.date_key` = 8-digit `YYYYMMDD`, consistent with `date`

---

## 8) View Inventory & Output Columns

### 8.1 Summary of Analytical Views

| View                          | Purpose                                                      | Grain                                       | Key Output Fields                                                                                                                     |
| :---------------------------- | :----------------------------------------------------------- | :------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------ |
| `v_sales_daily`               | Daily KPI summary for trend cards and alerts.                | 1 row per `order_date`                      | `order_date, sales, qty, active_customers, asp, aov_proxy`                                                                            |
| `v_sales_monthly`             | Executive-level monthly KPIs.                                | 1 row per `order_ym`                        | `order_ym, month_start, month_end, sales, qty, active_customers, asp`                                                                 |
| `v_aov_monthly`               | Monthly AOV proxy (customer-day as order).                   | 1 row per `order_ym`                        | `order_ym, sales, orders_proxy, aov_proxy`                                                                                            |
| `v_kpi_overview_monthly`      | Combined monthly KPI snapshot for dashboards.                | 1 row per `order_ym`                        | `order_ym, sales, qty, active_customers, asp, aov_proxy`                                                                              |
| `v_sales_by_dow`              | Day-of-week performance and seasonality.                     | 1 row per `day_of_week_num`                 | `day_of_week_num, day_of_week_name, sales, qty, asp, active_customers`                                                                |
| `v_category_performance`      | Category-level performance summary.                          | 1 row per `category_name`                   | `category_name, sales, qty, asp`                                                                                                      |
| `v_top_products`              | Product revenue ranking.                                     | 1 row per `product_name × category_name`    | `product_name, category_name, sales, qty, asp, sales_rank`                                                                            |
| `v_mix_share_monthly`         | Monthly category composition and sales share.                | 1 row per `order_ym × category_name`        | `order_ym, category_name, sales, qty, sales_share, qty_share`                                                                         |
| `v_city_performance`          | City-level sales and customer activity.                      | 1 row per `city`                            | `city, sales, qty, asp, unique_customers`                                                                                             |
| `v_city_category_monthly`     | Monthly breakdown by city and category.                      | 1 row per `order_ym × city × category_name` | `order_ym, city, category_name, sales, qty`                                                                                           |
| `v_price_bucket_distribution` | Price segmentation by sales range (from `master_sales_ext`). | 1 row per `price_bucket`                    | `price_bucket, line_count, sales, qty, asp`                                                                                           |
| `v_customer_activity`         | Customer lifetime and engagement activity.                   | 1 row per `customer_sk`                     | `customer_sk, first_purchase, last_purchase, line_count, active_months, qty, sales, asp`                                              |
| `v_aov_category_monthly`      | Monthly AOV proxy by category.                               | 1 row per `order_ym × category_name`        | `order_ym, category_name, sales, orders_proxy, aov_proxy`                                                                             |
| `v_asp_category_monthly`      | Monthly ASP by category.                                     | 1 row per `order_ym × category_name`        | `order_ym, category_name, sales, qty, asp`                                                                                            |
| `v_aov_city_monthly`          | Monthly AOV proxy by city.                                   | 1 row per `order_ym × city`                 | `order_ym, city, sales, orders_proxy, aov_proxy`                                                                                      |
| `v_sales_monthly_yoy`         | Year-over-year comparison.                                   | 1 row per `order_ym`                        | `order_ym, sales, qty, active_customers, asp, sales_last_year, sales_yoy_pct, qty_last_year, qty_yoy_pct`                             |
| `v_sales_monthly_mom`         | Month-over-month comparison.                                 | 1 row per `order_ym`                        | `order_ym, sales, qty, sales_prev_month, sales_mom_pct, qty_prev_month, qty_mom_pct`                                                  |
| `v_sales_monthly_index`       | Normalized index (baseline = first month).                   | 1 row per `order_ym`                        | `order_ym, sales, sales_index`                                                                                                        |
| `v_sales_monthly_rolling3`    | 3-month moving average for smoothing.                        | 1 row per `order_ym`                        | `order_ym, sales, sales_ma3, qty, qty_ma3`                                                                                            |
| `view_rfm`                    | RFM segmentation view for CRM prioritization.                | 1 row per `customer_sk`                     | `customer_sk, recency_days, frequency_orders, monetary_value, r_score, f_score, m_score, rfm_segment`                                 |
| `view_monthly_cohort`         | Monthly cohort retention analysis.                           | 1 row per `cohort_ym × months_since_first`  | `cohort_ym, months_since_first, active_customers, cohort_size, retention_rate`                                                        |
| `view_sales_trend`            | Consolidated executive trend panel.                          | 1 row per `order_ym`                        | `order_ym, sales, qty, active_customers, asp, sales_yoy_pct, sales_mom_pct, sales_index, sales_ma3`                                   |
| `view_pareto_products`        | 80/20 Pareto revenue distribution.                           | 1 row per `product_sk`                      | `product_sk, product_name, category_name, sales, sales_rank, sales_share, cumulative_share, is_top_80_percent`                        |
| `view_clv_simple`             | Simple CLV estimation (30% margin).                          | 1 row per `customer_sk`                     | `customer_sk, first_purchase, last_purchase, tenure_months, orders_proxy, aov_proxy, revenue_total, margin_rate, clv_margin_estimate` |

> All metric views are derived from `v_base_sales`, except `v_price_bucket_distribution` which sources directly from `master_sales_ext` for price segmentation.

---

### 8.2 Field Definitions by View

* **`v_sales_daily`** — `order_date, sales, qty, active_customers, asp, aov_proxy`
* **`v_sales_monthly`** — `order_ym, month_start, month_end, sales, qty, active_customers, asp`
* **`v_aov_monthly`** — `order_ym, sales, orders_proxy, aov_proxy`
* **`v_kpi_overview_monthly`** — `order_ym, sales, qty, active_customers, asp, aov_proxy`
* **`v_sales_by_dow`** — `day_of_week_num, day_of_week_name, sales, qty, asp, active_customers`
* **`v_category_performance`** — `category_name, sales, qty, asp`
* **`v_top_products`** — `product_name, category_name, sales, qty, asp, sales_rank`
* **`v_mix_share_monthly`** — `order_ym, category_name, sales, qty, sales_share, qty_share`
* **`v_city_performance`** — `city, sales, qty, asp, unique_customers`
* **`v_city_category_monthly`** — `order_ym, city, category_name, sales, qty`
* **`v_price_bucket_distribution`** — `price_bucket, line_count, sales, qty, asp` *(source: `master_sales_ext`)*
* **`v_customer_activity`** — `customer_sk, first_purchase, last_purchase, line_count, active_months, qty, sales, asp`
* **`v_aov_category_monthly`** — `order_ym, category_name, sales, orders_proxy, aov_proxy`
* **`v_asp_category_monthly`** — `order_ym, category_name, sales, qty, asp`
* **`v_aov_city_monthly`** — `order_ym, city, sales, orders_proxy, aov_proxy`
* **Helper Views:**

  * `v_sales_monthly_yoy`: `order_ym, sales, qty, active_customers, asp, sales_last_year, sales_yoy_pct, qty_last_year, qty_yoy_pct`
  * `v_sales_monthly_mom`: `order_ym, sales, qty, sales_prev_month, sales_mom_pct, qty_prev_month, qty_mom_pct`
  * `v_sales_monthly_index`: `order_ym, sales, sales_index`
  * `v_sales_monthly_rolling3`: `order_ym, sales, sales_ma3, qty, qty_ma3`
* **Advanced:**

  * `view_rfm`: `customer_sk, recency_days, frequency_orders, monetary_value, r_score, f_score, m_score, rfm_segment`
  * `view_monthly_cohort`: `cohort_ym, months_since_first, active_customers, cohort_size, retention_rate`
  * `view_sales_trend`: `order_ym, sales, qty, active_customers, asp, sales_yoy_pct, sales_mom_pct, sales_index, sales_ma3`
  * `view_pareto_products`: `product_sk, product_name, category_name, sales, sales_rank, sales_share, cumulative_share, is_top_80_percent`
  * `view_clv_simple`: `customer_sk, first_purchase, last_purchase, tenure_months, orders_proxy, aov_proxy, revenue_total, margin_rate, clv_margin_estimate`

---


