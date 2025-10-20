FROM apache/airflow:2.8.1

# Stay as the root user to install and set permissions
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git

# Copy your project files
COPY dbt_project/ /opt/airflow/dbt_project/
COPY dags/ /opt/airflow/dags/

# Make the startup script executable
RUN chmod +x /opt/airflow/dags/startup.sh

# Now, switch to the airflow user for security
USER airflow

# Install python packages as the airflow user
RUN pip install --no-cache-dir apache-airflow-providers-google dbt-bigquery