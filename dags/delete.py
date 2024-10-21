import pendulum
import logging
from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

dag_info = Variable.get("dag_info", deserialize_json=True)

@dag(
    dag_id="delete",
    schedule="*/10 * * * *",
    start_date=pendulum.parse(dag_info.get("start_date", "2024-04-10T00:00:00Z")).in_timezone("UTC"),
    catchup=False,
    is_paused_upon_creation=True,  # DAG가 생성될 때 자동으로 사용 중지
)
def delete():
    @task
    def start():
        return "skip_check_upload_delete"

    @task
    def skip_task_callable():
        logger.info("Skipping task")
        return

    skip_check_upload_delete = skip_task_callable.override(task_id="skip_check_upload_delete")()
    skip_get_job_id_list = skip_task_callable.override(task_id="skip_get_job_id_list")()
    skip_dedup = skip_task_callable.override(task_id="skip_dedup")()
    skip_parse = skip_task_callable.override(task_id="skip_parse")()

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
    start() >> skip_check_upload_delete >> skip_get_job_id_list >> skip_dedup >> skip_parse

dag_instance = delete()


##############
Airflow에서 @task 데코레이터를 사용하여 특정 태스크를 무조건 skip 상태로 처리하려면, BranchPythonOperator 대신 @task 데코레이터를 사용하여 태스크를 정의하고, 태스크 내부에서 조건을 확인하여 태스크를 건너뛰도록 설정할 수 있습니다. 여기서는 @task 데코레이터를 사용하여 태스크를 정의하고, skip 상태로 처리하는 예제를 작성하겠습니다.

수정된 코드

#############

############

설명
DAG 기본 설정:

default_args를 정의하여 DAG의 기본 설정을 지정합니다.
DAG 정의:

@dag 데코레이터를 사용하여 delete라는 이름의 DAG을 정의합니다.
schedule_interval을 */10 * * * *로 설정하여 10분마다 DAG이 실행되도록 합니다.
is_paused_upon_creation=True 속성을 사용하여 DAG가 생성될 때 자동으로 사용 중지되도록 설정합니다.
@task 데코레이터를 사용하여 start 태스크를 무조건 skip 처리:

@task 데코레이터를 사용하여 start 함수를 정의합니다.
start 함수는 항상 "skip_check_upload_delete"를 반환하여 start 태스크를 건너뛰도록 설정합니다.
@task 데코레이터를 사용하여 나머지 태스크도 모두 skip 처리:

@task 데코레이터를 사용하여 skip_task_callable 함수를 정의합니다.
skip_task_callable 함수는 단순히 로그를 출력하고 아무 작업도 수행하지 않습니다.
skip_task_callable.override(task_id="skip_check_upload_delete")()를 사용하여 skip_check_upload_delete, skip_get_job_id_list, skip_dedup, skip_parse 태스크를 정의합니다.
태스크 정의:

@task 데코레이터를 사용하여 get_job_id_list, dedup, parse 태스크를 정의합니다.
태스크 의존성 설정:

start 태스크가 항상 skip_check_upload_delete 태스크로 건너뛰도록 설정합니다.
skip_check_upload_delete 태스크가 완료된 후 나머지 태스크들이 순차적으로 실행되도록 의존성을 설정합니다.
이 코드는 Airflow에서 @task 데코레이터를 사용하여 특정 태스크를 skip 상태로 처리하고, 그 하위 태스크들도 모두 skip 상태로 처리하는 예제입니다. 이를 통해 특정 태스크를 조건 없이 건너뛰고, DAG의 흐름을 제어할 수 있습니다.

##################
