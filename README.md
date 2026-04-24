# 🏗️ Retail Sales & Returns Lakehouse (Microsoft Fabric)

## 📌 Overview

This project demonstrates an end-to-end **data engineering pipeline** built using **Microsoft Fabric**, implementing the **Medallion Architecture (Bronze → Silver → Gold)** to process, clean, and analyze retail transaction data.

The pipeline ingests raw ecommerce data from Kaggle, transforms it into structured datasets, and delivers business-ready insights through a **Power BI dashboard**.

---

## 🎯 Objectives

* Build a scalable **lakehouse architecture**
* Clean and standardize messy real-world data
* Separate **sales and returns logic**
* Create **analytics-ready datasets**
* Deliver **interactive business insights**

---

## 🧱 Architecture

```
Kaggle Dataset (CSV)
        │
        ▼
📂 Bronze Layer (Raw Data - Lakehouse 1)
        │
        ▼
🧹 Silver Layer (Cleaned & Structured Data)
        │
        ▼
📊 Gold Layer (Business Aggregations - Lakehouse 2)
        │
        ▼
📈 Semantic Model + Power BI Dashboard
```

---

## ⚙️ Tech Stack

* **Microsoft Fabric**
* Lakehouse (OneLake)
* PySpark (Notebooks)
* Delta Tables
* Power BI (Semantic Model & Dashboard)

---

## 📂 Dataset

* **Source:** Kaggle – Online Retail Dataset
* Contains:

  * Transactions (Invoice, Product, Quantity, Price)
  * Customer Information
  * Country-level data
  * Returns / cancellations

---

## 🥉 Bronze Layer

* Raw data ingested from CSV
* Stored as Delta table:

  * `bronze_online_retail_raw`
  * `bronze_online_retail_final`
* Minimal transformation
* Preserves original data for auditability

---

## 🥈 Silver Layer

* Data cleaning and standardization
* Key transformations:

  * Column renaming
  * Data type casting
  * Timestamp parsing
  * Price normalization (comma → decimal)
  * Null handling
  * Feature engineering (sales_amount)

### Tables:

* `silver_transactions_clean`
* `silver_sales_clean`
* `silver_returns`

---

## 🥇 Gold Layer

* Business-ready datasets for reporting

### Fact Tables:

* `gold_fact_sales`
* `gold_fact_returns`

### Dimension Tables:

* `gold_dim_product`
* `gold_dim_customer`

### Aggregated Tables:

* `gold_sales_daily`
* `gold_sales_monthly`
* `gold_country_performance`
* `gold_product_performance`

---

## 📊 Dashboard (Power BI)

### Key KPIs:

* Total Revenue
* Total Orders
* Total Customers
* Return Rate

### Visuals:

* Revenue Trend (Time Series)
* Top Products by Revenue
* Country-wise Performance
* Product Performance Analysis

---

## 🚀 Key Learnings

* Implemented **Medallion Architecture**
* Handled **real-world messy data**
* Built **scalable lakehouse pipelines**
* Designed **data models for analytics**
* Created **business KPIs and dashboards**

---

## 🙌 Author

Sai Kishan
Master’s in Data Engineering | Data Analytics & Engineering Enthusiast
