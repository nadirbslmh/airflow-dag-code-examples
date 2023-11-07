from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "nadir",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="delivery_service_dag_v1",
    default_args=default_args,
    start_date=datetime(2023, 10, 26),
    schedule_interval="@daily",
)
def delivery_service_etl():
    @task()
    def get_items():
        return ["cheese", "milk", "secret box"]

    @task()
    def send_items(items):
        for item in items:
            print(f"sending item: {item}...")

    items = get_items()
    send_items(items)


delivery_service_dag = delivery_service_etl()
