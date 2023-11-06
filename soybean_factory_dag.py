from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="soy_factory_v1",
    default_args=default_args,
    description="soy factory simulation",
    start_date=datetime(2023, 10, 22, 2),
    schedule_interval="@daily",
) as dag:
    task1 = BashOperator(
        task_id="task_1",
        bash_command="echo collect soy beans",
    )

    task2 = BashOperator(
        task_id="task_2",
        bash_command="echo create tempe",
    )

    task3 = BashOperator(
        task_id="task_3",
        bash_command="echo create soy bean milk",
    )

    task4 = BashOperator(task_id="task_4", bash_command="echo distribute items")

    # task1 --> task2
    #   |----> task3
    task1 >> [task2, task3]

    # task2 --> task4
    # task3 --> task4
    task2 >> task4
    task3 >> task4
