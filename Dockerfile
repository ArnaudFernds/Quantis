FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git

COPY dbt_project/ /opt/airflow/dbt_project/
COPY dags/ /opt/airflow/dags/

RUN chmod +x /opt/airflow/dags/startup.sh

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-google dbt-bigquery