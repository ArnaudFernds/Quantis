#!/bin/bash

# Initialize the database
airflow db init

# Start the webserver
exec airflow webserver