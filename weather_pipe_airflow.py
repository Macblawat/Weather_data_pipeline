from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Use scripts in same folder as DAG
WORKDIR = os.path.dirname(__file__)

default_args = {
    "owner": "Maciek",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Explicitly create DAG
weather_dag = DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Fetch weather data and upload to Azure Blob",
    schedule="@daily",
    start_date=datetime(2025, 8, 23),
    catchup=False,
)

# Define tasks
task_fetch = BashOperator(
    task_id="fetch_weather_data",
    bash_command=f"python3 scrap_weather.py",
    cwd=WORKDIR,
    dag=weather_dag
)

task_upload = BashOperator(
    task_id="upload_to_blob",
    bash_command=f"python3 blob_load.py",
    cwd=WORKDIR,
    dag=weather_dag)

# Set task dependencies
task_fetch >> task_upload