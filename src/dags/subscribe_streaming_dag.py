from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="restaurant_subscribe_streaming",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start_streaming = BashOperator(
        task_id="start_streaming",
        bash_command="spark-submit /opt/airflow/scripts/favorite_restaurant_compaigns.py",
    )
