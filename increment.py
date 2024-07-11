import uuid
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


@task(task_id="upload_raw")
def upload_raw():
    base_dir = r'C:\upload\{}'.format(datetime.now().strftime('%Y%m%d'))
    conn_details = BaseHook.get_connection('my_postgres_conn').get_uri()
    
    conn = psycopg2.connect(conn_details)
    cursor = conn.cursor()
    
    # dsllm_job_hist 테이블에서 count 조회
    cursor.execute("SELECT COUNT(*) FROM dsllm_job_hist")
    job_count = cursor.fetchone()[0]
    
    
    # dsllm_raw 테이블에 데이터 insert
    insert_query = """
    INSERT INTO dsllm_raw (data, job_count) VALUES (%s, %s) RETURNING job_id;
    """
    upload_id = uuid.uuid4()
    job_id = job_id

    all_data = read_json_files_to_dict(base_dir)
    print(f"Loaded {len(all_data)} JSON files.")
    
    job_ids = []
    for index, data in all_data.items():
        # 각 JSON 파일의 데이터와 job_count를 dsllm_raw 테이블에 insert
        cursor.execute(insert_query, (data, job_count))
        job_id = cursor.fetchone()[0]
        job_ids.append((upload_id, job_id))
        conn.commit()
    
    cursor.close()
    conn.close()
    
    print(f"Inserted data with job_ids: {job_ids}")
    return job_ids

@task(task_id="dedup")
def dedup(job_ids):
    conn_details = BaseHook.get_connection('my_postgres_conn').get_uri()
    conn = psycopg2.connect(conn_details)
    cursor = conn.cursor()

    try:
        # dsllm_dedup 테이블에 데이터 insert
        insert_query = "INSERT INTO dsllm_dedup (column_names) VALUES (values)"
        cursor.execute(insert_query)
        conn.commit()
        print("Data inserted into dsllm_dedup successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
        # 에러 발생 시 dsllm_job_hist 테이블에서 job_ids를 조건으로 사용하여 데이터 update
        update_query = """
        UPDATE dsllm_job_hist
        SET after_job_id = %s
        WHERE job_id = %s;
        """
        for job_id in job_ids:
            cursor.execute(update_query, (job_id[1], job_id[0]))
        conn.commit()
        print(f"dsllm_job_hist table updated for job_ids: {job_ids}")
    finally:
        cursor.close()
        conn.close()

    return job_ids

@task(task_id="parse")
def parse(job_ids):
    conn_details = BaseHook.get_connection('my_postgres_conn').get_uri()
    conn = psycopg2.connect(conn_details)
    cursor = conn.cursor()

    try:
        # dsllm_parse 테이블에 데이터 insert
        insert_query = "INSERT INTO dsllm_parse (column_names) VALUES (values)"
        cursor.execute(insert_query)
        conn.commit()
        print("Data inserted into dsllm_parse successfully.")
    except Exception as e:
        print(f"Error occurred: {e}")
        # 에러 발생 시 dsllm_job_hist 테이블에서 job_ids를 조건으로 사용하여 데이터 update
        update_query = """
        UPDATE dsllm_job_hist
        SET column_name = value
        WHERE job_id IN %s;
        """
        cursor.execute(update_query, (tuple(job_ids),))
        conn.commit()
        print(f"dsllm_job_hist table updated for job_ids: {job_ids}")
    finally:
        cursor.close()
        conn.close()

    #Setting up task dependencies
    check_for_null_success_yn >> upload_raw() >> dedup() >> parse()

dag_instance = create_dag()

if __name__ == "__main__":
    dag_instance.test()