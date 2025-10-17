FROM apache/airflow:2.8.1
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-google dbt-bigquery
COPY dbt_project/ /opt/airflow/dbt_project/
COPY dags/ /opt/airflow/dags/