from datetime import datetime, timedelta

from airflow import DAG

from custom_operator.average_operator import AverageOperator

default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="average_dag_v1",
    default_args=default_args,
    description="dag with custom ops",
    start_date=datetime(2023, 10, 29),
    schedule_interval="@daily",
) as dag:
    average_task = AverageOperator(
        task_id="average_task",
        data=[1, 2, 3, 4, 5, 6, 7, 8, 9],
    )
