from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

@dag(
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def my_dag():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end

dag_instance = my_dag()