from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
import os

# 'qna.json' 파일에서 데이터 읽기
from llm.qna_insert import read_json_file, upsert_data

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['example'], start_date=datetime(2023, 1, 1))
def qna_processing_dag():
    @task
    def process_json_files_in_directory(directory: str):
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if filename.endswith('.json'):
                    file_path = os.path.join(root, filename)
                    data = read_json_file(file_path)
                    upsert_data(data)
                    print(f"Processed {file_path}")

    process_json_files_in_directory('path/to/your/json/files')

dag_instance = qna_processing_dag()

if __name__ == "__main__":
    # 지정된 디렉토리의 모든 .json 파일 처리
    dag_instance.cli()