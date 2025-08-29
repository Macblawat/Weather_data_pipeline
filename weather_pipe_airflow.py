from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from airflow.operators.email import EmailOperator

WORKDIR = os.path.dirname(__file__)
load_dotenv()
email = os.getenv("EMAIL")
default_args = {
    "owner": "Maciek",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": email,
}


weather_dag = DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    description="Fetch weather data and upload to Azure Blob",
    schedule="@daily",
    start_date=datetime(2025, 8, 23),
    catchup=False,
)


task_scrap= BashOperator(
    task_id="scrap_weather_data",
    bash_command=f"python3 scrap_weather.py",
    cwd=WORKDIR,
    dag=weather_dag
)

task_upload_blob = BashOperator(
    task_id="upload_to_blob",
    bash_command=f"python3 blob_load.py",
    cwd=WORKDIR,
    dag=weather_dag)

task_transform_db=BashOperator(
    task_id="upload_to_db",
    bash_command=f"python3 transform.py",
    cwd=WORKDIR,
    dag=weather_dag
)

delete_data=BashOperator(
    task_id="delete_local_data",
    bash_command=f"python3 delete_local_data.py",
    cwd=WORKDIR,
    dag=weather_dag,
    trigger_rule=TriggerRule.ALL_DONE 
)


notify_error = EmailOperator(
    task_id="notify_error",
    to=email,
    subject="Airflow Error: Unable to load data",
    html_content="""
        <h3>Weather Pipeline Failure</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Task: {{ task_instance.task_id }}</p>
        <p>Execution Date: {{ ts }}</p>
        <p>Log URL: <a href="{{ task_instance.log_url }}">View Logs</a></p>
    """,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=weather_dag,
)

task_scrap >> task_upload_blob >> task_transform_db >> delete_data

[task_scrap, task_upload_blob, task_transform_db] >> notify_error>> delete_data