# GCP Market Segmentation & Forecasting

A comprehensive data analytics and machine learning project for manufacturing and product distribution, leveraging Google Cloud Platform (GCP) services to perform geographical customer clustering and sales and materials forecasting.

**Project Links:**
- [📊 Presentation (PDF)](gcp-market-segmentation-forecasting.pdf)
- [💼 Portfolio Write-up](https://kavyashreebn-en.carrd.co/#gcp)

---

## 🎯 Project Overview

Manufacturing planning requires reliable projections of sales, material costs, and demand across different markets.
This project delivers:
- **Market segmentation** to support regional planning and targeted strategies
- **Sales & material forecasting** for production planning and procurement
- **Interactive dashboards** for decision-makers
- A **cloud-native blueprint** connecting data engineering, ML, and BI

---

## 💡 Business Questions Answered

1. How are customer markets distributed geographically?
2. Which markets offer the highest revenue potential?
3. What are the projected sales volumes for the next 5-7 years?
4. What materials will be required to meet forecasted demand?

---

## 🔄 End-to-End Workflow

### 1. Data Acquisition & Integration
- Extracted data from **SAP Datasphere** (customers, orders, products, materials)
- Integrated via **Cloud Data Fusion** → **BigQuery**

### 2. Data Preparation
- Data cleaning and transformation in **BigQuery**
- Added geographical coordinates using **Google Maps Geocoding API**

### 3. Market Segmentation (Clustering)
- Applied **K-Means clustering** with **BigQuery ML**
- Segmented customers by geography: Europe, North America, South America, Asia
- Visualized clusters in **Looker Studio**

### 4. Market Selection
- Selected **Europe** as primary market (highest customers + revenue)
- 12 customers generating €641.7M revenue

### 5. Forecasting (Sales & Materials)
- Built **ARIMA PLUS** models in **BigQuery ML**
- Generated forecasts for:
  - Product sales (5-7 year horizon)
  - Material requirements by product
- Tracked experiments with **MLflow**

### 6. Dashboards & Reporting
- Interactive visualizations in **Looker Studio**:
  - Geographical customer segmentation
  - Product sales forecast reports
  - Material requirements analysis

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Data Source | SAP Datasphere |
| Data Integration | Cloud Data Fusion |
| Data Warehouse | BigQuery |
| Machine Learning | BigQuery ML (K-Means, ARIMA PLUS) |
| ML Tracking | MLflow |
| Visualization | Looker Studio |
| Programming | Python, SQL |
| APIs | Google Maps Geocoding API |

## 📝 Notes

This project demonstrates cloud-native integration of enterprise data systems (SAP) with GCP analytics platform for real-world manufacturing intelligence.

Implementation executed entirely in Google Cloud Platform (code in SQL and Python).
