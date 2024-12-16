from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'start_date': days_ago(1),
}

@dag(schedule_interval='@daily', default_args=default_args, catchup=False)
def example_dag():
    @task
    def print_dag_id(**kwargs):
        # 실행 컨텍스트에서 DAG ID를 추출합니다.
        dag_id = kwargs['dag_run'].dag_id
        print(f"The DAG ID is: {dag_id}")

    @task
    def print_execution_date(**kwargs):
        # 실행 컨텍스트에서 실행 날짜를 추출합니다.
        execution_date = kwargs['execution_date']
        print(f"The execution date is: {execution_date}")

    def insert_dag_id(**kwargs):
        # SQL 쿼리에서 DAG ID를 동적으로 삽입합니다.
        dag_id = kwargs['dag_run'].dag_id
        sql = "INSERT INTO your_table (dag_id) VALUES (%(dag_id)s);"
        return PostgresOperator(
            task_id='insert_dag_id',
            postgres_conn_id='your_postgres_connection_id',
            sql=sql,
            params={'dag_id': dag_id}  # SQL 쿼리에 DAG ID를 전달합니다.
        )

    print_dag_id_task = print_dag_id()
    print_execution_date_task = print_execution_date()
    insert_dag_id_task = insert_dag_id()

example_dag_instance = example_dag()

# Airflow에서 context는 현재 실행 중인 task instance에 대한 메타데이터와 설정 정보를 담고 있는 사전(dictionary)입니다. Task가 실행될 때, Airflow는 이 context를 task 함수나 operator에 자동으로 전달합니다. 이를 통해 사용자는 task의 실행 환경에 대한 세부 정보에 접근할 수 있습니다.

# context 사전에 포함될 수 있는 정보는 다음과 같습니다:

# dag: 현재 실행 중인 DAG에 대한 정보를 담고 있는 객체입니다. 예를 들어, context['dag'].dag_id를 통해 DAG의 ID를 알 수 있습니다.
# task: 현재 실행 중인 task에 대한 정보를 담고 있는 객체입니다.
# execution_date: 현재 task가 실행되는 날짜와 시간입니다.
# prev_execution_date: 이전에 task가 실행된 날짜와 시간입니다.
# ds: execution_date를 문자열로 표현한 것입니다. 형식은 YYYY-MM-DD입니다.
# ds_nodash: execution_date를 하이픈 없이 표현한 것입니다. 형식은 YYYYMMDD입니다.
# ts: 타임스탬프 형식의 execution_date입니다.
# ts_nodash: 하이픈과 공백 없이 표현한 타임스탬프 형식의 execution_date입니다.
# run_id: 현재 실행 중인 DAG Run의 ID입니다.
# conf: 실행 시 전달된 configuration 정보입니다.
# 이 외에도 context는 task의 재시도 횟수, task의 상태 등 task 실행과 관련된 다양한 정보를 포함할 수 있습니다. 이 정보들을 활용하여 task 실행 시 필요한 동적인 처리를 구현할 수 있습니다. 예를 들어, context를 사용하여 실행 날짜에 따라 다른 데이터 파일을 처리하거나, task의 실행 결과를 기반으로 다음 task의 동작을 조건적으로 변경하는 등의 로직을 구현할 수 있습니다.


# Airflow에서 context와 template은 서로 다른 개념입니다.

# Context
# 정의: context는 현재 실행 중인 task instance에 대한 메타데이터와 설정 정보를 담고 있는 사전(dictionary)입니다. 이 정보는 task가 실행될 때 Airflow에 의해 자동으로 task 함수나 operator에 전달됩니다.
# 용도: context는 task의 실행 환경에 대한 세부 정보에 접근할 수 있게 해줍니다. 예를 들어, 현재 실행 중인 DAG의 ID, task의 실행 날짜, 이전 실행 날짜 등을 포함합니다. 이를 통해 사용자는 task 실행 시 필요한 동적인 처리를 구현할 수 있습니다.
# 예시: context['dag'].dag_id를 사용하여 현재 실행 중인 DAG의 ID를 알 수 있습니다.
# Template
# 정의: template은 Airflow에서 제공하는 템플릿 기능을 말합니다. Jinja 템플릿 언어를 사용하여 task에서 사용할 변수나 명령어를 동적으로 생성할 수 있습니다.
# 용도: template는 task 실행 전에 변수를 동적으로 채워넣거나, 설정 값을 조정하는 등의 작업에 사용됩니다. 예를 들어, SQL 쿼리, 파일 경로, 스크립트 명령어 등을 task 실행 시점의 데이터에 맞게 동적으로 생성할 수 있습니다.
# 예시: {{ ds }}를 사용하여 task가 실행되는 날짜를 SQL 쿼리 내에 동적으로 삽입할 수 있습니다.
# 차이점
# 목적과 사용 시점: context는 실행 시점에 task에 대한 환경 정보를 제공하는 반면, template는 실행 전에 task의 입력값이나 명령어 등을 동적으로 생성하기 위해 사용됩니다.
# 사용 방법: context는 Python 함수나 operator 내에서 사전 형태로 접근되며, template는 주로 Jinja 템플릿 형식으로 표현되어 task의 인자나 설정 값에 사용됩니다.
# 결론적으로, context와 template는 Airflow에서 task를 동적으로 제어하고 구성하기 위해 서로 보완적으로 사용되는 두 가지 중요한 기능입니다.