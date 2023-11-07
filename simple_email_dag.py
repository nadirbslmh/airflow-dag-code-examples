from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nadir",
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "email": ["hey@mail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
    dag_id="simple_email_dag_v1",
    start_date=datetime(2023, 10, 31),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="wrong_task_change_dir",
        bash_command="cd not_exist_directory",
    )
