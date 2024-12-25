from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import psycopg2
import uuid

from llm.dags.validate import ValidateDocument
from llm.dags.validateFactory import ValidateFactory

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def list_s3_files(bucket_name, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        return [item['Key'] for item in response['Contents']]
    return []

# def validate_files(file_keys):
#     # Implement your validation logic here
#     # Return True if all files are valid, otherwise return False and an error message
#     for file_key in file_keys:
#         if not file_key.endswith('.zip'):
#             return False, f"Invalid file type for {file_key}"
#     return True, ""

def insert_history_record(file_key, success_yn, msg=None):
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="your_host"
    )
    try:
        with conn:
            with conn.cursor() as cur:
                insert_sql = """
                INSERT INTO history_table (uuid, file_key, success_yn, msg, created_at)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                """
                cur.execute(insert_sql, (str(uuid.uuid4()), file_key, success_yn, msg))
    finally:
        conn.close()

def validate_and_insert(**kwargs):
    bucket_name = 'your-s3-bucket-name'
    execution_date = kwargs['execution_date']
    date_str = (execution_date - timedelta(days=1)).strftime('%Y%m%d')
    prefix = f'{date_str}/'
    
    file_keys = list_s3_files(bucket_name, prefix)
    # is_valid, error_msg = validate_files(file_keys)
    # validator = ValidateDocument()
    # is_valid, error_msg = validator.validate_files(file_keys)
    
    doc_validator = ValidateFactory.create_validator("document")
    is_valid, error_msg = doc_validator.validate_files(file_keys)

    
    if is_valid:
        for file_key in file_keys:
            insert_history_record(file_key, True)
    else:
        for file_key in file_keys:
            insert_history_record(file_key, False, error_msg)

with DAG('s3_to_db_dag', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    validate_and_insert_task = PythonOperator(
        task_id='validate_and_insert',
        provide_context=True,
        python_callable=validate_and_insert,
        dag=dag
    )
