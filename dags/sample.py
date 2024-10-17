from __future__ import annotations

import json
import pendulum
import logging
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

dag_info = Variable.get("dag_info", deserialize_json=True)

@dag(
    dag_id="delete",
    schedule="*/10 * * * *",
    start_date=pendulum.parse(dag_info.get("start_date", "2024-04-10T00:00:00Z")).in_timezone("UTC"),
    catchup=False,
)

def delete():
    @task
    def check_upload_delete():
        logger.info("This is the first task.")
        # pass  # 실제 작업이 없으므로 pass 사용

    @task
    def get_job_id_list():
        job_id = []
        return job_id
    
    @task
    def load_raw(job_id, **kwargs):
        return job_id
    
    @task
    def dedup():
        pass
    
    @task
    def parse():
        pass

    check_upload_delete() >> get_job_id_list() >> load_raw() >> dedup() >> parse()

dag_instance = delete()

if __name__ == "__main__":
    dag_instance.test()