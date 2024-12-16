from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
import psycopg2
from datetime import datetime, timedelta
from json_read import read_json_files_to_dict  # Assuming this function is defined in json_read.py
from db import upsert_data  # Ensure this import is correct based on your project structure
from airflow.sensors.sql_sensor import SqlSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval=timedelta(days=1), description='Upsert data into dsllm_raw table using psycopg2 with Airflow connection', start_date=datetime(2023, 1, 1), tags=['example'])
def create_dag():

    #SQL Sensor to check for NULL success_yn
    check_for_null_success_yn = SqlSensor(
        task_id='check_for_null_success_yn',
        conn_id='postgres_conn_id',  # Define your connection ID
        sql="SELECT COUNT(*) FROM dsllm_job_hist WHERE success_yn IS NULL;",
        poke_interval=60,  # Check every 60 seconds
        timeout=600,  # Timeout after 10 minutes
        mode='poke',
        success=lambda x: x[0][0] > 0  # Success criteria
    )


    @task(task_id="upsert_all")
    def upsert_all_data():
        #base_dir = 'path_to_your_json_files'
        base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
        conn_details = BaseHook.get_connection('my_postgres_conn').get_uri()
        
        conn = psycopg2.connect(conn_details)
        
        all_data = read_json_files_to_dict(base_dir)
        print(all_data)
        print('#'*20)
        print(f"Loaded {len(all_data)} JSON files.")

        for index, data in all_data.items():
            upsert_data(index, data, conn)
        
        conn.close()

    #Setting up task dependencies
    check_for_null_success_yn >> upsert_all_data()

dag_instance = create_dag()

if __name__ == "__main__":
    dag_instance.test()