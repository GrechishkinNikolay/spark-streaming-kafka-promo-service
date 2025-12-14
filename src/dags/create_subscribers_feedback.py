from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="init_subscribers_feedback",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_feedback_table = PostgresOperator(
        task_id="create_subscribers_feedback_table",
        postgres_conn_id="postgres_local",
        sql="scripts/DDL_create_subscribers_feedback.sql",
    )
