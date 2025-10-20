# dags/daily_finance_dag.py

import pendulum
from airflow.decorators import dag, task_group
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.gcs import GCSListOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator

# --- Configuration from Airflow Variables ---
GCP_CONN_ID = "google_cloud_default"
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET")
INCOME_BUCKET = Variable.get("INCOME_STATEMENT_BUCKET")
BALANCE_BUCKET = Variable.get("BALANCE_SHEET_BUCKET")
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

@dag(
    dag_id="daily_full_refresh_financials",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule_interval="@daily",  # <-- Runs once per day
    catchup=False,
    tags=["finance", "daily-refresh"],
)
def daily_financials_pipeline():

    @task_group(group_id="process_buckets")
    def process_buckets():
        # --- Process Income Statement Bucket ---
        GCSToBigQueryOperator(
            task_id="load_income_statement_to_bq",
            bucket=INCOME_BUCKET,
            source_objects=["income_statement.csv"], # <-- Reads a specific file from the root
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw_income_statements",
            field_delimiter=";",
            write_disposition="WRITE_TRUNCATE", # <-- Erases and replaces the table data
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID
        )

        # --- Process Balance Sheet Bucket ---
        GCSToBigQueryOperator(
            task_id="load_balance_sheet_to_bq",
            bucket=BALANCE_BUCKET,
            source_objects=["balance_sheet.csv"], # <-- Reads a specific file from the root
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw_balance_sheets",
            field_delimiter=";",
            write_disposition="WRITE_TRUNCATE", # <-- Erases and replaces the table data
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID
        )
    
    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    process_buckets() >> run_dbt_transformations

daily_financials_pipeline()