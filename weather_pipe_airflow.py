from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

WORKDIR = os.path.join(os.path.dirname(__file__), "..")

default_args = {
    "owner": "Maciek",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Fetch weather data and upload to Azure Blob",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 24),
    catchup=False,
) as dag:
    
    task_fetch = BashOperator(
        task_id="fetch_weather_data",
        bash_command=f"python {WORKDIR}/fetch_weather.py"
    )

    task_upload = BashOperator(
        task_id="upload_to_blob",
        bash_command=f"python {WORKDIR}/upload_to_blob.py"
    )

    task_fetch >> task_upload