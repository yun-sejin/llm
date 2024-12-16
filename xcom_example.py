from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['example'], start_date=datetime(2023, 1, 1))
def qna_processing_dag():
    @task
    def push_data():
        # 여기서는 예시 데이터를 생성하고 XCom으로 푸시합니다.
        data = {'file_path': '/path/to/json_file.json'}
        return data

    @task
    def process_json_file(ti):
        # XCom에서 데이터를 풀합니다.
        data = ti.xcom_pull(task_ids='push_data')
        file_path = data['file_path']
        # 파일 경로를 사용하여 JSON 파일 처리
        print(f"Processed {file_path}")

    push_data_task = push_data()
    process_file_task = process_json_file(push_data_task)

dag_instance = qna_processing_dag()

if __name__ == "__main__":
    dag_instance.test()