import pendulum
import logging
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

logger = logging.getLogger(__name__)

dag_info = Variable.get("dag_info", deserialize_json=True)

@dag(
    dag_id="delete3",
    schedule="*/10 * * * *",
    start_date=pendulum.parse(dag_info.get("start_date", "2024-04-10T00:00:00Z")).in_timezone("UTC"),
    catchup=False,
    is_paused_upon_creation=True,  # DAG가 생성될 때 자동으로 사용 중지
)
def delete3():
    @task
    def check_upload_delete():
        logger.info("This is the first task.")
        # 태스크를 무조건 skip 처리
        raise AirflowSkipException("Skipping this task and all downstream tasks.")

    @task
    def get_job_id_list():
        job_id = []
        return job_id
    
    @task
    def dedup():
        pass
    
    @task
    def parse():
        pass

    # 태스크 의존성 설정
    check_upload_delete() >> get_job_id_list() >> dedup() >> parse()

dag_instance = delete3()






# ################
# 설명
# DAG 기본 설정:

# default_args를 정의하여 DAG의 기본 설정을 지정합니다.
# DAG 정의:

# @dag 데코레이터를 사용하여 delete3라는 이름의 DAG을 정의합니다.
# schedule_interval을 */10 * * * *로 설정하여 10분마다 DAG이 실행되도록 합니다.
# is_paused_upon_creation=True 속성을 사용하여 DAG가 생성될 때 자동으로 사용 중지되도록 설정합니다.
# 태스크 정의:

# @task 데코레이터를 사용하여 check_upload_delete 함수를 태스크로 정의합니다.
# AirflowSkipException을 발생시켜 해당 태스크와 그 하위 태스크들이 모두 skip 상태로 처리되도록 합니다.
# 예외 메시지를 "해당 DAG는 사용중지되었습니다"로 설정합니다.
# 태스크 정의:

# @task 데코레이터를 사용하여 get_job_id_list 함수를 태스크로 정의합니다.
# 이 코드는 Airflow에서 AirflowSkipException을 사용하여 특정 태스크를 무조건 skip 상태로 처리하고, 예외 메시지를 "해당 DAG는 사용중지되었습니다"로 설정하는 예제입니다. 이를 통해 특정 태스크를 조건 없이 건너뛰고, DAG의 흐름을 제어할 수 있습니다.

# 이 예제에서는 check_upload_delete 태스크를 무조건 skip 상태로 처리하고, 이 태스크 이후의 모든 하위 태스크들도 skip 상태로 처리합니다. 이를 통해 DAG의 실행을 제어할 수 있습니다.

# ###################