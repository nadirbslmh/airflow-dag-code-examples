# Airflow DAG Code Examples

Code examples for creating DAG in Apache Airflow.

## How to Use

1. Clone this repository.

2. If you are using `docker compose` for running Airflow. Put the codes and other directories (`custom_operator` and `sql`) inside `dags` directory.

## Notes

1. The `insert_table.sql` is actually empty because the insert query will be generated automatically.

2. The `simple_email_dag.py` is an example for sending email in Airflow. When using this sample code, make sure the email configuration is configured properly. Learn more [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html).
