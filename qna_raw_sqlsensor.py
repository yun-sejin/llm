from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor

# Assuming the rest of the imports and the 'qna_insert' module are correctly set up as before

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

    # SQL Sensor to check if there is at least one record in the 'job_hist' table
    check_job_hist_exists = SqlSensor(
        task_id='check_job_hist_exists',
        conn_id='your_connection_id',  # Specify your Airflow connection ID here
        sql="SELECT COUNT(*) FROM job_hist WHERE id IS NOT NULL",
        success=lambda count: count[0][0] > 0,  # Proceed if count is greater than 0
        timeout=600,  # Timeout in seconds
        poke_interval=300,  # Check every 5 minutes
    )

    # Assuming 'process_json_file' is defined elsewhere in your DAG
    process_file_task = process_json_file('{{ ti.xcom_pull(task_ids="check_job_hist_exists") }}')

    check_job_hist_exists >> process_file_task

dag_instance = qna_processing_dag()

if __name__ == "__main__":
    # 지정된 디렉토리의 모든 .json 파일 처리
    dag_instance.cli()