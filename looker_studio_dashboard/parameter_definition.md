# **Parameter Definition**

## **Overview**

This document defines the interactive parameters used across the *PT Sejahtera Bersama — Sales & Customer Analytics* dashboard in Looker Studio.
Parameters enable users to dynamically filter and switch views without duplicating charts or rewriting metrics.
They serve as a lightweight semantic layer between the data source (`master_sales_ext`) and report visuals.

---

## **Data Context**

* **Source:** Google Sheets connection to `master_sales_ext` (exported from BigQuery)
* **Data Grain:** One record per product line per transaction (daily transaction granularity)
* **Refresh Policy:** Synced every 15 minutes (Looker Studio embedded source)
* **Primary Dimensions:** `cust_email`, `cust_city`, `category_name`, `order_date`, `price_bucket`

---

## **Parameter Registry**

| **Parameter**     | **Type / Control**         | **Allowed Values**                                | **Default** | **Primary Use Case**                                    | **Applied Pages**           |
| :---------------- | :------------------------- | :------------------------------------------------ | :---------- | :------------------------------------------------------ | :-------------------------- |
| `p_category`      | Text – Dropdown            | Distinct `category_name` + “All”                  | `All`       | Focus or compare specific product categories            | All                         |
| `p_city`          | Text – Dropdown            | Distinct `cust_city` + “All”                      | `All`       | Geo-specific performance views                          | Executive / Sales / Product |
| `p_customer_type` | Text – Dropdown            | `All`, `New`, `Repeat`                            | `All`       | Behavioral segmentation (based on `is_repeat_customer`) | Customer Behavior           |
| `p_metric`        | Text – Dropdown            | `Sales`, `Qty`, `AOV`                             | `Sales`     | Metric selector (used in Metric Picker field)           | Sales & Product pages       |
| `p_price_bucket`  | Text – Dropdown            | `Under 20`, `20–49.99`, `50–99.99`, `100+`, `All` | `All`       | Price-range filtering and comparisons                   | Product Analysis            |
| `p_year`          | Number – Dropdown / Slider | 2020 – 2021 (available years)                     | Latest year | Year scoping for historical analyses                    | All pages                   |

---

## **Parameter-Driven Filter Fields**

To make parameters operational, a simple *Show/Hide* calculated field is created for each parameter and applied as a filter (`Include = Show`).
This ensures consistent logic across charts.

**Filter by Category**

```text
IF(category_name = p_category OR p_category = 'All', 'Show', 'Hide')
```

**Filter by City**

```text
IF(cust_city = p_city OR p_city = 'All', 'Show', 'Hide')
```

**Filter by Customer Type**

```text
CASE
  WHEN p_customer_type = 'All' THEN 'Show'
  WHEN p_customer_type = 'New' AND is_repeat_customer = FALSE THEN 'Show'
  WHEN p_customer_type = 'Repeat' AND is_repeat_customer = TRUE THEN 'Show'
  ELSE 'Hide'
END
```

**Filter by Price Bucket**

```text
IF(price_bucket = p_price_bucket OR p_price_bucket = 'All', 'Show', 'Hide')
```

**Filter by Year (optional)**

```text
IF(EXTRACT(YEAR FROM order_date) = p_year, 'Show', 'Hide')
```

> **Implementation Note:** Apply the corresponding “Show” filter to every chart that must respond to the chosen parameter.

---

## **Metric Picker**

The `p_metric` parameter controls a unified metric field, allowing a single chart to toggle between Sales, Quantity, and AOV.

```text
CASE p_metric
  WHEN 'Sales' THEN SUM(total_sales)
  WHEN 'Qty'   THEN SUM(order_qty)
  WHEN 'AOV'   THEN SAFE_DIVIDE(SUM(total_sales), NULLIF(SUM(order_qty),0))
END
```

* **Type:** Number
* **Aggregation:** Auto
* **Recommended Placement:** Line trend, bar chart, and KPI cards.

---

## **Design Guidelines**

* Parameters are positioned at the page header to maintain consistent navigation.
* Defaults are pre-set (`All`, `Sales`, latest year) for meaningful first-load state.
* Avoid overlapping filters (e.g., a Date Range + p_year + page filter for year) to prevent blank outputs.
* Document parameter behavior in the dashboard glossary for future analysts.

