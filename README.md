# Airflow Financial KPI Pipeline

This project uses Apache Airflow to orchestrate a daily financial data pipeline on Google Cloud Platform. The pipeline extracts raw data, transforms it using dbt, and loads Key Performance Indicators (KPIs) into BigQuery.

## Airflow's Role & Workflow

The core of this project is the Airflow DAG (`daily_financial_kpi_ingestion`) which automates the following ELT (Extract, Load, Transform) process:

1.  **Schedule**: The DAG is scheduled to run once every day (`@daily`).
2.  **Extract & Load**: It starts by running two `GCSToBigQueryOperator` tasks in parallel. Each task reads a specific CSV file (`income_statement.csv` and `balance_sheet.csv`) from its dedicated GCS bucket and loads the data into a raw table in BigQuery, overwriting the previous day's data (`WRITE_TRUNCATE`).
3.  **Transform**: Once both loading tasks are successful, a `BashOperator` is triggered. This operator executes `dbt run`, which runs all the dbt models to clean the raw data, join it, and calculate the final KPIs. The results are then loaded into the `financial_kpis` table in BigQuery.

---

## Setup Commands for Airflow

These commands will build the Airflow application image, publish it, and deploy it to your GKE cluster.

### 1. Build & Push the Docker Image

Run these commands from your project's root directory. The image packages Airflow with all your DAGs and dbt models.

```powershell
# 1. Build the image (use --no-cache to ensure updates are included)
docker build --no-cache -t my-finance-app:latest .

# 2. Tag the image for Google Artifact Registry
docker tag my-finance-app:latest europe-west1-docker.pkg.dev/fluid-stratum-475417-s9/airflow-apps/my-finance-app:latest

# 3. Push the image to the registry
docker push europe-west1-docker.pkg.dev/fluid-stratum-475417-s9/airflow-apps/my-finance-app:latest