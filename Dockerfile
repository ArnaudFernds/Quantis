# Dockerfile

FROM apache/airflow:2.8.1

# Stay as root user to install things and change permissions
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git

# Copy your files first
COPY dbt_project/ /opt/airflow/dbt_project/
COPY dags/ /opt/airflow/dags/

# Now, as root, make the script executable
RUN chmod +x /opt/airflow/dags/startup.sh

# Finally, switch to the less-privileged airflow user
USER airflow

# This can stay here, it will run as the airflow user
RUN pip install --no-cache-dir apache-airflow-providers-google dbt-bigquery