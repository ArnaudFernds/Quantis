#!/bin/bash
airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@albertschool.com \
    --password airflow || true 

airflow scheduler &
exec airflow webserver