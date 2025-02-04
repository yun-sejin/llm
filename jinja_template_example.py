from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.models import Variable  # Variable 모듈 추가
from airflow.decorators import dag

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['example'], start_date=datetime(2023, 1, 1))
def qna_processing_dag():
    @task
    def push_data():
        try:
        data = {'file_path': '{{ var.value.json_file_path }}'}  # Jinja 템플릿을 사용하여 변수 값 동적으로 설정
            data = {'file_path': '{{ var.value.json_file_path }}'}  # Jinja 템플릿을 사용하여 변수 값 동적으로 설정
            fail_files = ['file1.json', 'file2.json']
            return data, fail_files
        except Exception as e:
            print(f"Error in push_data: {e}")
            raise

    @task
    def process_json_file(data):
        try:
            # XCom에서 데이터를 풀합니다.
            # data = ti.xcom_pull(task_ids='push_data')
            # file_path = data['file_path']
            # 파일 경로를 사용하여 JSON 파일 처리
            print(f"Processed {data}")
        except Exception as e:
            print(f"Error in process_json_file: {e}")
            raise

    push_data_task = push_data()
    process_file_task = process_json_file(push_data_task)

    push_data_task >> process_file_task
    
dag_instance = qna_processing_dag()

if __name__ == "__main__":
    dag_instance.test()



