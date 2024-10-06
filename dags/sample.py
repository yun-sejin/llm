from __future__ import annotations

import json
import pendulum
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

dag_info = Variable.get("dag_info", deserialize_json=True)
# dag_info_str = Variable.get("dag_info")
# dag_info = json.loads(dag_info_str)
@dag(
    dag_id=dag_info.get("dag_id"),
    schedule=dag_info.get("schedule", "*/10 * * * *"),
    start_date=pendulum.parse(dag_info.get("start_date", "2024-04-10T00:00:00Z")).in_timezone("UTC"),
    catchup=dag_info.get("catchup", False),
)
def my_dag():
    @task
    def start_task():
        logger.info("This is the first task.")
        # pass  # 실제 작업이 없으므로 pass 사용

    @task
    def end_task():
        pass  # 실제 작업이 없으므로 pass 사용

    start_task() >> end_task()

dag_instance = my_dag()

if __name__ == "__main__":
    dag_instance.test()