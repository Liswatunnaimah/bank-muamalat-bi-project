# PT Sejahtera Bersama – Sales, Customer & Product Analytics

**Project-Based Virtual Internship | Bank Muamalat × Rakamin Academy (Oct 2025)**
**Author:** Liswatun Naimah
**Tools:** Google BigQuery · Looker Studio · SQL · Excel

[![View Dashboard](https://img.shields.io/badge/🎯_View_Looker_Dashboard-089e1b61-blue?style=flat-square)](https://lookerstudio.google.com/reporting/089e1b61-6361-45a2-9834-bc3d44992902/page/6iodF/edit)
[![View Repository](https://img.shields.io/badge/💻_GitHub_Repository-bank--muamalat--bi--project-green?style=flat-square)](https://github.com/Liswatunnaimah/bank-muamalat-bi-project)

---

## Business Context

**PT Sejahtera Bersama** is an e-commerce company specializing in selling technology and educational kits across multiple Indonesian cities.
As part of the *Business Intelligence Analyst Internship Program* with **Bank Muamalat × Rakamin**, this project focuses on transforming transactional data into strategic insights that support **sales optimization**, **customer retention**, and **product performance management**.

---

## Project Objectives

| Goal                             | Description                                                                           |
| -------------------------------- | ------------------------------------------------------------------------------------- |
| **1. Data Consolidation**        | Build a clean and reliable analytical data mart using Google BigQuery.                |
| **2. Performance Visibility**    | Create interactive dashboards to monitor sales, product mix, and customer activity.   |
| **3. Business Insights**         | Derive actionable insights on sales trends, pricing behavior, and customer retention. |
| **4. Strategic Recommendations** | Provide data-driven recommendations to enhance business growth and customer loyalty.  |

---

## Analytical Pipeline

```
Raw CSVs  →  Staging Tables  →  Master Sales  →  Enriched (Behavioral) Model  →  Star Schema  →  BI Dashboard
(Customers, Products, Orders)        (Data Cleaning)     (Join + Validation)     (Add LTV, Tiering)       (Dim/Fact)         (Looker Studio)
```

| Layer                | Description                                                            | Output                       |
| -------------------- | ---------------------------------------------------------------------- | ---------------------------- |
| **Raw Layer**        | Source CSV files: `Customers`, `Products`, `Orders`, `ProductCategory` | `raw_*`                      |
| **Staging Layer**    | Cleansing, trimming, data type enforcement, PK deduplication           | `stg_*`                      |
| **Master Layer**     | Join of all entities to form `master_sales`                            | `master_sales`               |
| **Enrichment Layer** | Behavioral features (repeat flag, LTV, city/category tier)             | `master_sales_ext`           |
| **Analytical Layer** | Dimensional model for performance monitoring                           | `fact_sales`, `dim_*`, `v_*` |
| **Dashboard Layer**  | Visualization and executive insights via Looker Studio                 | Dashboard (link below)       |

---

## Key Deliverables

| Output                    | Description                                                           |
| ------------------------- | --------------------------------------------------------------------- |
| **SQL Pipeline (01–07)**  | End-to-end ETL scripts to build and validate analytical layers.       |
| **ERD (Star Schema)**     | Logical data model for BI reporting efficiency.                       |
| **Data Dictionary**       | Business & technical definitions of all tables, columns, and metrics. |
| **Looker Dashboard**      | Executive dashboard visualizing KPIs, trends, and customer retention. |
| **Business Report (PPT)** | Analytical storytelling with strategic insights and recommendations.  |

---

## Dashboard Overview

> [Open Dashboard → Looker Studio](https://lookerstudio.google.com/reporting/089e1b61-6361-45a2-9834-bc3d44992902/page/6iodF/edit)

**Main Pages:**

1. **Executive Summary:** Total Revenue, Repeat Rate, AOV, ASP, Pareto Contribution.
2. **Sales Performance:** Monthly trends, YoY & MoM growth, category mix share.
3. **Product Insights:** Top 10 SKUs, pricing segmentation, Pareto 80/20 analysis.
4. **Customer Behavior:** RFM segmentation, cohort retention, repeat purchase rate.
5. **Geographic View:** City-level sales and customer density map.

---

## Key Business Insights

* **Sales peaked in late 2021**, driven by strong repeat purchases and product diversification.
* **Top 20% of products contributed 80% of total revenue**, confirming a Pareto pattern.
* **Repeat customers generate 3× higher average sales value** compared to first-time buyers.
* **High-tier cities dominate total sales**, but emerging cities show faster growth potential.
* **Mid-range price bucket (20–49.99)** holds the largest market share, aligning with mass affordability.

---

## Strategic Recommendations

| Area                         | Recommendation                                                                                        | Rationale                                        |
| ---------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **Customer Retention**       | Implement loyalty programs for repeat buyers and active cohorts.                                      | Repeat customers show higher CLV and engagement. |
| **Product Mix Optimization** | Focus marketing on top 20% high-revenue products while experimenting with underperforming categories. | Supports efficient inventory and margin control. |
| **Regional Strategy**        | Scale promotional campaigns in Tier 2–3 cities with emerging growth.                                  | Expands untapped markets.                        |
| **Pricing & Promotion**      | Maintain mid-tier pricing range and introduce bundle offers for high-frequency products.              | Captures wider audience without diluting ASP.    |
| **Data-Driven Culture**      | Continue monitoring key KPIs (Sales, Retention, AOV) with automated dashboards.                       | Enables agile business decisions.                |

---

## Repository Structure

```
bank-muamalat-bi-project/
│
├── README.md
│
├── 
│
├── sql/
│   ├── 01_data_quality_eda.sql
│   ├── 02_build_staging.sql
│   ├── 03_build_master_sales.sql
│   ├── 04_build_master_sales_ext.sql
│   ├── 05_build_star_schema.sql
│   ├── 06_metrics_views.sql
│   └── 07_advanced_analysis.sql
│
├── datasets/
│   ├── customers.csv
│   ├── products.csv
│   ├── product_category.csv
│   ├── orders.csv
│   ├── master_sales.csv
│   └── master_sales_ext.csv
│   └── Query Result.xlsx
│
├── looker_studio/
│   ├── looker_studio_dashboard_link.txt
│   ├── parameter_definitions.md
│   ├── calculated_fields.md
│   └── PT Sejahtera Bersama Analytics Dashboard Preview.pdf
│
├── data_dictionary.md
│
├── erd_diagram.png
│
└── PT Sejahtera Bersama Analytics Report.pdf
   

```

---

## Learning Outcomes

* Strengthened understanding of **data modeling and star schema design**.
* Improved ability to create **reusable SQL logic** for metrics and QA validation.
* Enhanced business storytelling skills through **data-driven presentation and visualization**.

---

## Contact

**Liswatun Naimah**
[+6285695858195](Whatsapp:+6285695858195) |
[liswatunnaimah@gmail.com](mailto:liswatunnaimah@gmail.com) |
[LinkedIn](https://linkedin.com/in/liswatunnaimah) | [GitHub](https://github.com/Liswatunnaimah) |
[CV](https://drive.google.com/drive/folders/13C8kO1kPlkZI-qDq93z_BfaClPnGQL81?usp=drive_link) | 
[Portofolio](https://drive.google.com/drive/folders/1SzBSuP3mtCiCOWuZH3U0AKDkJF-2kfze?usp=drive_link)

---

