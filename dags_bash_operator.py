from airflow import DAG
from datetime import timedelta
import pendulum
import os
import zipfile
import psycopg2
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.sensors.sql_sensor import SqlSensor # type: ignore

@DAG(
    dag_id="example_bash_operator",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example", "example2"],
    params={"example_key": "example_value"},
)
def example_bash_operator():
    # python Operation 
    def sel_t():
        # Your implementation her
        print("Modified code")
        return "hello world"
    
    t0 = BashOperator(
        task_id='task0',
        bash_command='echo "Task 0"',
    )

    t1 = PythonOperator(
        task_id='task1',
        python_callable=sel_t,
    )

    t2 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='your_postgres_conn_id',
        sql="""
        DROP TABLE IF EXISTS llm_raw;
        CREATE TABLE llm_raw (
            src_id varchar,
            job_id varchar,
            data varchar[],
            create_time timestamp
        );
        """,
    )
    
    t2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='your_postgres_conn_id',
        sql="""
        INSERT INTO llm_raw (src_id, job_id, data, create_time)
        VALUES ('value1', 'value2', '{value3, value4}', '2022-01-01 00:00:00');
        """,
    )
               
    t3 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='your_postgres_conn_id',
        sql="""
        INSERT INTO llm_raw (src_id, job_id, data, create_time)
        VALUES ('value1', 'value2', '{value3, value4}', '2022-01-01 00:00:00');
        """,
    )
    
    @task
    def unzip_files():
        zip_path = '/path/to/your/zip/file.zip'
        extract_path = '/path/to/extract/files'
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        return extract_path

    @task
    def load_json_files():
        extract_path = unzip_files()
        json_files = [file for file in os.listdir(extract_path) if file.endswith('.json')]
        
        conn = psycopg2.connect(
            host='your_host',
            port='your_port',
            database='your_database',
            user='your_user',
            password='your_password'
        )
        
        cursor = conn.cursor()
        
        for file in json_files:
            file_path = os.path.join(extract_path, file)
            
            with open(file_path, 'r') as json_file:
                json_data = json_file.read()
                
                cursor.execute("""
                    INSERT INTO dll_raw (data)
                    VALUES (%s)
                """, (json_data,))
        
        conn.commit()
        cursor.close()
        conn.close()

    t4 = load_json_files()

    t0 >> t1 >> t2 >> t3 >> t4
    
dag = example_bash_operator()

if __name__ == '__main__':
    dag.test()
