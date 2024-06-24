from airflow.decorators import dag, task
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import ShortCircuitOperator
import psycopg2
from datetime import datetime, timedelta
from json_read import read_json_files_to_dict  # Assuming this function is defined in json_read.py
from db import upsert_data  # Ensure this import is correct based on your project structure


# Given the excerpt from your insert_raw.py file and the issue with the SqlSensor import, you can replace the SqlSensor with an equivalent functionality using the ShortCircuitOperator from Airflow's providers. The ShortCircuitOperator allows you to execute a Python function and continue the DAG's execution only if the function returns True. This can be used to mimic the behavior of the SqlSensor by executing a SQL query and checking the condition directly in Python.

# Here's how you can modify your DAG to use the ShortCircuitOperator instead of the SqlSensor:

# Import the ShortCircuitOperator from Airflow's operators.
# Define a Python function that executes your SQL query and checks the condition.
# Use the ShortCircuitOperator to execute this function.
# Below is the modified part of your insert_raw.py file incorporating these changes:

# This modification uses the ShortCircuitOperator to execute a Python function (check_for_null_success_yn_fn) 
# that connects to your PostgreSQL database, executes the SQL query, and checks if the result meets your condition. 
# If the condition is met (True), the DAG execution continues; otherwise, it stops.

# Other imports and default_args remain the same
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval=timedelta(days=1), description='Upsert data into dsllm_raw table using psycopg2 with Airflow connection', start_date=datetime(2023, 1, 1), tags=['example'])
def create_dag():

    def check_for_null_success_yn_fn():
        conn_id = 'postgres_conn_id'  # Define your connection ID
        conn = BaseHook.get_connection(conn_id)
        dsn = f"dbname='{conn.schema}' user='{conn.login}' password='{conn.password}' host='{conn.host}'"
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dsllm_job_hist WHERE success_yn IS NULL;")
                result = cur.fetchone()
                return result[0] > 0  # Return True if condition is met

    check_for_null_success_yn = ShortCircuitOperator(
        task_id='check_for_null_success_yn',
        python_callable=check_for_null_success_yn_fn,
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

    # The @task definition for upsert_all_data remains the same

    check_for_null_success_yn >> upsert_all_data()

dag = create_dag()

if __name__ == "__main__":
    dag.test()