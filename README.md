# Quantis 📈  
*"Understand the past, diagnose the present, to predict the future."*

---

## 🚨 The Problem  
Every year, thousands of entrepreneurs navigate blindly.  
They are passionate about their craft, but their financial data remains a mystery — a source of stress.  

Why? Because financial analysis has always been **complex**, **expensive**, and reserved for an **elite group of experts**.  

---

## 💡 Our Solution  
Quantis changes the rules.  

Our platform transforms raw accounting documents into a **smart and automated financial dashboard**.  
In just a few clicks, business owners stop seeing numbers — they see **clear answers** to essential questions:  
> “Where is my cash going?”  
> “Is this new venture profitable?”  

We put the **power of a CFO** into the hands of every SME owner — helping them make better decisions, control growth, and anticipate the future.  

---

## 🧠 Product Description  
**Quantis** is a SaaS (Software-as-a-Service) financial intelligence platform designed for SMEs and their advisors.  
It automates the **collection**, **processing**, and **visualization** of financial data.  

By simply uploading accounting documents (*balance sheets*, *income statements*, etc.),  
users instantly gain access to **interactive dashboards** that translate financial data into **actionable insights**.  

---

## ⚙️ Key Features  
- 🔄 **Automated Data Collection** — Upload financial statements directly through a secure Google Form.  
- 🧮 **Smart KPI Computation** — Automated ETL process calculates indicators (profitability, debt ratio, liquidity, etc.).  
- 📊 **Interactive Dashboards** — Grafana dashboards visualize company health in real time.  
- 🔐 **Secure & Private** — Each company’s data is stored and processed securely on Google Cloud.  

---

## ☁️ Technical Architecture (End-to-End)  
Below is the full pipeline from **Google Form submission to Grafana visualization**:  

### 🧩 1. Data Ingestion — *Google Forms + Apps Script*  
The user uploads their financial documents (CSV files).  
A Google Apps Script automatically sends these files to **Google Cloud Storage** in two buckets:  
- `quantis_income_statements`  
- `quantis_balance_sheets`  

---

### ☁️ 2. Cloud Storage — *Data Lake*  
Buckets act as the **staging layer** for raw accounting CSV files.  
Each upload triggers an **Event Notification** to a Pub/Sub topic (`quantis-csv`).  

---

### ⚡ 3. Cloud Function (trigger_job) — *Event Processing*  
A **Cloud Function (Gen2)** listens to the Pub/Sub topic.  
When a new file is uploaded, it automatically triggers a **Cloud Run Job** (`quantis-kpi-job`).  

---

### 🧮 4. Cloud Run Job — *ETL & KPI Generation*  
The job runs the core Quantis Python backend (`main.py`):  
1. Reads the latest balance sheet & income statement CSVs from Cloud Storage.  
2. Computes financial KPIs (profitability, leverage, liquidity, etc.) using `kpi_lib.py`.  
3. Cleans, merges, and uploads the results to **BigQuery** → Table `quantis_analytics.financial_kpis`.  

---

### 🗄️ 5. BigQuery — *Data Warehouse*  

---

### 6. Grafana — *Dashboard* 
