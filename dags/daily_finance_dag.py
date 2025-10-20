import pendulum
from airflow.decorators import dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

@dag(
    dag_id="daily_financial_kpi_ingestion",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="@daily",
    catchup=False,
    tags=["finance", "kpi"],
)
def financial_kpi_ingestion():
    load_income_statement = GCSToBigQueryOperator(
        task_id="load_income_statement",
        bucket="{{ var.value.INCOME_STATEMENT_BUCKET }}",
        source_objects=["income_statement.csv"],
        destination_project_dataset_table="{{ var.value.BIGQUERY_DATASET }}.raw_income_statements",
        write_disposition="WRITE_TRUNCATE",
        field_delimiter=";",
        skip_leading_rows=1,
        autodetect=True,
        gcp_conn_id="google_cloud_default",
    )
    load_balance_sheet = GCSToBigQueryOperator(
        task_id="load_balance_sheet",
        bucket="{{ var.value.BALANCE_SHEET_BUCKET }}",
        source_objects=["balance_sheet.csv"],
        destination_project_dataset_table="{{ var.value.BIGQUERY_DATASET }}.raw_balance_sheets",
        write_disposition="WRITE_TRUNCATE",
        field_delimiter=";",
        skip_leading_rows=1,
        autodetect=True,
        gcp_conn_id="google_cloud_default",
    )
    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )
    [load_income_statement, load_balance_sheet] >> run_dbt_transformations

financial_kpi_ingestion()
