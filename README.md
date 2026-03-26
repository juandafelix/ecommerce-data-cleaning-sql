# 📊 E-Commerce Data Cleaning & KPI Analysis

## 📌 Project Overview

This project focuses on transforming a messy e-commerce dataset into a clean, reliable dataset and generating business KPIs using SQL.

The goal is to simulate a real-world data cleaning pipeline and produce metrics that stakeholders can trust.

---

## 🧩 Problem

The raw dataset contains multiple data quality issues:

* Inconsistent date formats
* Typo in customer segments (e.g. *premuim*, *standrad*)
* Invalid order values
* Duplicate records

These issues make business KPIs unreliable.

---

## ⚙️ Solution

A structured SQL pipeline was built with the following stages:

1. **Data Profiling**

   * Identify nulls and inconsistencies

2. **Parsing & Typing**

   * Convert date formats and data types

3. **Data Normalisation**

   * Fix customer segment typos

4. **Business Rules Filtering**

   * Remove invalid records (e.g. low order values, invalid hours)

5. **Deduplication**

   * Remove duplicate transactions

6. **KPI Calculation**

   * Compute key business metrics

---

## 📈 Key KPIs

* Average Order Value (AOV)
* Gross Margin %
* Return Rate
* Median Order Value
* Return Rate by Payment Method
* High-value customer contribution
* Monthly GMV trends
* Month-over-Month Growth

---

## 🛠️ Tech Stack

* SQL (CTE, Aggregations, Window Functions)
* Data Cleaning Pipeline Design

---

## 📂 Project Structure

```
ecommerce-project/
 ├── data_cleaning_pipeline.sql
 └── kpi_calculations.sql
```

---

## 🚀 Key Learnings

* Importance of data cleaning before analysis
* Handling messy real-world datasets
* Applying business rules to ensure KPI accuracy
* Structuring SQL for readability and review

---

## 🔗 How to Use

Run the SQL scripts sequentially:

1. data_cleaning_pipeline.sql
2. kpi_calculations.sql
