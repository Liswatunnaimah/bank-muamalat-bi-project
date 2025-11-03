# **Calculated Field**

## **Purpose**

This file documents all calculated fields defined within the Looker Studio connection to `master_sales_ext`.
These fields standardize business logic and metric definitions across all visuals, ensuring accuracy and alignment with the underlying SQL logic used in BigQuery.

---

## **A. Time & Label Fields**

| **Field Name**      | **Formula (LoS)**                                                                           | **Type** | **Aggregation** | **Usage**                    |
| :------------------ | :------------------------------------------------------------------------------------------ | :------- | :-------------- | :--------------------------- |
| `YearMonth Key`     | `YEAR(order_date)*100 + MONTH(order_date)`                                                  | Number   | None            | Chronological sorting        |
| `Key Year-Month`    | `CONCAT(CAST(YEAR(order_date) AS TEXT), '-', LPAD(CAST(MONTH(order_date) AS TEXT), 2,'0'))` | Text     | None            | X-axis label                 |
| `Order Month Label` | `FORMAT_DATE('%b %Y', order_date)`                                                          | Text     | None            | Readable month label         |
| `Quarter Label`     | `CONCAT('Q', CAST(QUARTER(order_date) AS TEXT), ' ', CAST(YEAR(order_date) AS TEXT))`       | Text     | None            | Quarterly summary breakdowns |

> These helpers ensure chronological order in line and bar charts that aggregate monthly or quarterly data.

---

## **B. Core Business Metrics**

| **Metric**                    | **Formula**                                                | **Type** | **Aggregation** | **Business Definition**                                 |
| :---------------------------- | :--------------------------------------------------------- | :------- | :-------------- | :------------------------------------------------------ |
| `Total Sales`                 | `SUM(total_sales)`                                         | Number   | Sum             | Total transaction revenue                               |
| `Total Quantity`              | `SUM(order_qty)`                                           | Number   | Sum             | Total units sold                                        |
| `Average Order Value (AOV)`   | `SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(order_qty), 0))` | Number   | Auto            | Average sales value per unit (line-level approximation) |
| `Average Selling Price (ASP)` | Same as AOV                                                | Number   | Auto            | Mean unit price across all transactions                 |
| `Distinct Customers`          | `COUNT_DISTINCT(cust_email)`                               | Number   | Auto            | Unique customer count per filter context                |

> *Note:* The dataset does not include a unique `order_id`; therefore, AOV and ASP are computed at line-item level rather than order-level.

---

## **C. Customer Lifecycle and Retention**

| **Field**                 | **Formula**                                                                                                                                                                                 | **Type** | **Aggregation** | **Purpose**                               |
| :------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :------- | :-------------- | :---------------------------------------- |
| `Customer Type`           | `IF(is_repeat_customer, 'Repeat', 'New')`                                                                                                                                                   | Text     | None            | Customer segmentation                     |
| `% Repeat Sales`          | `SAFE_DIVIDE(SUM(CASE WHEN is_repeat_customer THEN total_sales END), NULLIF(SUM(total_sales), 0))`                                                                                          | Number   | Auto            | Share of revenue from repeat purchases    |
| `# New Customers`         | `COUNT_DISTINCT(CASE WHEN is_repeat_customer = FALSE THEN cust_email END)`                                                                                                                  | Number   | Auto            | Count of first-time buyers                |
| `# Repeat Customers`      | `COUNT_DISTINCT(CASE WHEN is_repeat_customer = TRUE THEN cust_email END)`                                                                                                                   | Number   | Auto            | Returning buyers                          |
| `Repeat Rate`             | `SAFE_DIVIDE(COUNT_DISTINCT(CASE WHEN is_repeat_customer THEN cust_email END), NULLIF(COUNT_DISTINCT(cust_email), 0))`                                                                      | Number   | Auto            | Repeat customer ratio                     |
| `Avg Orders per Customer` | `AVG(customer_total_lines)`                                                                                                                                                                 | Number   | Auto            | Frequency of purchase per customer        |
| `Average LTV (Gross)`     | `AVG(customer_ltv_gross)`                                                                                                                                                                   | Number   | Auto            | Mean lifetime value per customer          |
| `Tenure Segment`          | `CASE WHEN days_since_first < 30 THEN 'New (<30d)' WHEN days_since_first < 180 THEN 'Growing (30–179d)' WHEN days_since_first < 365 THEN 'Established (180–364d)' ELSE 'Loyal (≥365d)' END` | Text     | None            | Behavioral cohort grouping by tenure days |

---

## **D. Price and Product Performance**

| **Field**               | **Formula**                                                                                         | **Type** | **Aggregation** | **Purpose**                        |
| :---------------------- | :-------------------------------------------------------------------------------------------------- | :------- | :-------------- | :--------------------------------- |
| `Avg Price per Product` | `SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(order_qty), 0))`                                          | Number   | Auto            | Category/product ASP               |
| `Price Bucket Sort Key` | `CASE price_bucket WHEN 'Under 20' THEN 1 WHEN '20–49.99' THEN 2 WHEN '50–99.99' THEN 3 ELSE 4 END` | Number   | None            | Ordered sorting for bucket visuals |

---

## **E. Geo and Category Tier Metrics**

From BigQuery export (`city_tier`, `category_tier`, `city_sales_total`, `category_sales_total`).

| **Field**                     | **Formula**                                                | **Type** | **Aggregation** | **Purpose**                        |
| :---------------------------- | :--------------------------------------------------------- | :------- | :-------------- | :--------------------------------- |
| `City Sales Contribution`     | `SAFE_DIVIDE(SUM(total_sales), SUM(city_sales_total))`     | Number   | Auto            | Relative city share of revenue     |
| `Category Sales Contribution` | `SAFE_DIVIDE(SUM(total_sales), SUM(category_sales_total))` | Number   | Auto            | Relative category share of revenue |

---

## **F. Metric Picker (Reusable Control)**

```text
CASE p_metric
  WHEN 'Sales' THEN SUM(total_sales)
  WHEN 'Qty'   THEN SUM(order_qty)
  WHEN 'AOV'   THEN SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(order_qty), 0))
END
```

This field powers multi-metric charts such as Monthly Sales Trends and Category Performance.

---

## **G. Filter Hooks (Show/Hide Logic)**

Reusable boolean fields connecting parameters to charts:

```text
-- Category
IF(category_name = p_category OR p_category = 'All', 'Show', 'Hide')

-- City
IF(cust_city = p_city OR p_city = 'All', 'Show', 'Hide')

-- Customer Type
CASE
  WHEN p_customer_type = 'All' THEN 'Show'
  WHEN p_customer_type = 'New' AND is_repeat_customer = FALSE THEN 'Show'
  WHEN p_customer_type = 'Repeat' AND is_repeat_customer = TRUE THEN 'Show'
  ELSE 'Hide'
END

-- Price Bucket
IF(price_bucket = p_price_bucket OR p_price_bucket = 'All', 'Show', 'Hide')
```

Apply **Include = Show** as chart filter.

---

## **H. Quality Checklist**

1. Validate that all ratio metrics use `SAFE_DIVIDE` and `NULLIF(..., 0)` to prevent divide-by-zero.
2. Confirm `YearMonth Key` is used as the chart sort field, not the textual label.
3. Maintain aggregation consistency: *SUM for totals, AVG for customer-level metrics.*
4. Parameter filters are non-overlapping and visible on every page.
5. KPI cards and charts reference the same calculated fields to avoid metric drift.

---

## **I. Documentation and Governance**

* Every metric is version-controlled in GitHub (`docs/CALCULATED_FIELDS.md` and `PARAMETER_DEFINITION.md`).
* Field descriptions are mirrored in Looker Studio’s *Data Source → Edit Fields* pane.
* Updates to the BigQuery model (`master_sales_ext.sql`) must be reflected here to maintain full lineage.

