import pendulum
from airflow.decorators import dag, task_group
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.gcs import GCSListOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.operators.bash import BashOperator

# --- Configuration from Airflow Variables ---
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BIGQUERY_DATASET = Variable.get("BIGQUERY_DATASET")
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
INCOME_BUCKET = Variable.get("INCOME_STATEMENT_BUCKET")
BALANCE_BUCKET = Variable.get("BALANCE_SHEET_BUCKET")


@dag(
    dag_id="daily_multi_bucket_financials",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule_interval="0 8 * * *",  # Tous les matins à 8h
    catchup=False,
    tags=["finance", "multi-bucket", "incremental"],
)
def multi_bucket_financials_pipeline():

    # --- Branche 1: Traitement du Bucket "Income Statement" ---
    @task_group(group_id="process_income_statement_bucket")
    def process_income_statements():
        list_files = GCSListOperator(
            task_id="list_new_income_files",
            bucket=INCOME_BUCKET,
            prefix="landing/",
            gcp_conn_id=GCP_CONN_ID,
        )

        load_to_bq = GCSToBigQueryOperator(
            task_id="load_income_files_to_bq",
            bucket=INCOME_BUCKET,
            source_objects=list_files.output,
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw_income_statements",
            field_delimiter=";",
            write_disposition="WRITE_APPEND",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID,
        )

        # ... (Tâche pour archiver les fichiers, omise pour la simplicité)
        list_files >> load_to_bq

    # --- Branche 2: Traitement du Bucket "Balance Sheet" ---
    @task_group(group_id="process_balance_sheet_bucket")
    def process_balance_sheets():
        list_files = GCSListOperator(
            task_id="list_new_balance_files",
            bucket=BALANCE_BUCKET,
            prefix="landing/",
            gcp_conn_id=GCP_CONN_ID,
        )

        load_to_bq = GCSToBigQueryOperator(
            task_id="load_balance_files_to_bq",
            bucket=BALANCE_BUCKET,
            source_objects=list_files.output,
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw_balance_sheets",
            field_delimiter=";",
            write_disposition="WRITE_APPEND",
            skip_leading_rows=1,
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID,
        )

        # ... (Tâche pour archiver les fichiers, omise pour la simplicité)
        list_files >> load_to_bq

    # --- Étape Finale: Transformations dbt ---
    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    # Définition de l'ordre: les 2 task groups en parallèle, puis dbt
    [process_income_statements(), process_balance_sheets()] >> run_dbt_transformations


multi_bucket_financials_pipeline()
