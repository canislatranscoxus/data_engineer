from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="simple_scheduling_dag",
         schedule_interval="*/15 * * * *",
         start_date=datetime(2020, 12, 1),
         default_args=default_args,
         catchup=False) as dag:

    task_1 = DummyOperator(task_id="task_1")
    task_2 = DummyOperator(task_id="task_2")
    task_1 >> task_2

