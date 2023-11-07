from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def send_gift(ti):
    ti.xcom_push(key="gift", value="a cool gift")


def send_mail(ti):
    ti.xcom_push(key="mail", value="hello how are you?")


def receive_items(ti):
    gift = ti.xcom_pull(task_ids="send_gift", key="gift")
    mail = ti.xcom_pull(task_ids="send_mail", key="mail")

    print(f"received gift: {gift}")
    print(f"received mail: {mail}")


default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="send_gift_mail_dag_v1",
    default_args=default_args,
    description="send and receive some items",
    start_date=datetime(2023, 10, 24),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="send_gift",
        python_callable=send_gift,
    )

    task2 = PythonOperator(
        task_id="send_mail",
        python_callable=send_mail,
    )

    task3 = PythonOperator(
        task_id="receive_items",
        python_callable=receive_items,
    )

    # task1 >> task3
    # task2 >> task3

    [task1, task2] >> task3
