from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def add(number1, number2):
    result = number1 + number2
    return f"{number1} + {number2} = {result}"


default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="simple_python_dag_v1",
    default_args=default_args,
    description="simple DAG with python",
    start_date=datetime(2023, 10, 24),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="add_task",
        python_callable=add,
        op_kwargs={"number1": 1, "number2": 5},
    )
