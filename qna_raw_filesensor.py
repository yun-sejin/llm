from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# 'qna.json' 파일에서 데이터 읽기
from llm.qna_insert import read_json_file, upsert_data

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['example'], start_date=datetime(2023, 1, 1))
def qna_processing_dag():
    @task
    def process_json_file(file_path: str):
        data = read_json_file(file_path)
        upsert_data(data)
        print(f"Processed {file_path}")

    # Assuming 'path/to/your/json/files' is a directory where new files are expected
    file_sensor_task = FileSensor(
        task_id='wait_for_json',
        fs_conn_id='fs_default',
        filepath='path/to/your/json/files/*.json',
        poke_interval=300,
        timeout=600
    )

    process_file_task = process_json_file('{{ ti.xcom_pull(task_ids="wait_for_json") }}')

    file_sensor_task >> process_file_task

dag_instance = qna_processing_dag()

if __name__ == "__main__":
    # 지정된 디렉토리의 모든 .json 파일 처리
    dag_instance.cli()