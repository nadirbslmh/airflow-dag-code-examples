from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from custom_operator.collect_operator import CollectOperator

default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="collect_data_from_api_dag_v1",
    default_args=default_args,
    description="collect data from API",
    start_date=datetime(2023, 10, 31),
    schedule_interval="@daily",
) as dag:
    create_table_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id="my_postgres",
        sql="sql/create_table.sql",
    )

    collect_data_task = CollectOperator(task_id="collect_data_task")

    insert_data_task = PostgresOperator(
        task_id="insert_data_task",
        postgres_conn_id="my_postgres",
        sql="sql/insert_data.sql",
    )

    create_table_task >> collect_data_task >> insert_data_task
