from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
import tempfile
import pendulum
import os
import shutil
import uuid
import zipfile
from pathlib import Path
import csv
import requests
from common_postgres_methods import CommonPostgresMethods
import json
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

@dag(
    dag_id='new_confluence_dag',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "llm"],
    dag_display_name="New Sample DAG with Display Name",
)
def new_confluence_dag():
    @task
    def create_temp_dir():
        base_tmp_dir = Path(tempfile.mkdtemp(dir='/tmp', prefix='confluence_'))
        print(f"Base temporary directory created: {base_tmp_dir}")
        return str(base_tmp_dir)

    @task
    def generate_uuid(base_tmp_dir):
        uuid_dir = Path(base_tmp_dir) / str(uuid.uuid4())
        uuid_dir.mkdir(parents=True, exist_ok=True)
        print(f"UUID directory created: {uuid_dir}")
        return str(uuid_dir)

    @task
    def copy_and_extract_file(uuid_dir):
        data_folder = Path(__file__).parent / 'data2'
        source_file = data_folder / 'space4.zip'
        destination_file = Path(uuid_dir) / 'space4.zip'
        shutil.copy(source_file, destination_file)
        print(f"Copied {source_file} to {destination_file}")
        
        # Extract the zip file directly into the UUID folder
        with zipfile.ZipFile(destination_file, 'r') as zip_ref:
            zip_ref.extractall(uuid_dir)
        print(f"Extracted {destination_file} to {uuid_dir}")
        
        # Delete the zip file
        os.remove(destination_file)
        print(f"Deleted {destination_file}")
        return str(uuid_dir)

    @task
    def validate_and_store_data(uuid_dir):
        valid_data = []
        invalid_data = []
        for txt_file in Path(uuid_dir).glob('*.txt'):
            with open(txt_file, 'r') as f:
                text = f.read()
                if validate_row(text):
                    valid_data.append(text)
                else:
                    invalid_data.append({"file": txt_file.name, "error": "Validation failed"})
        store_valid_data(valid_data)
        invalid_data_dir, invalid_data_file = save_invalid_data(invalid_data)
        send_invalid_data_api(invalid_data, invalid_data_dir, invalid_data_file)

    def validate_row(row):
        # Add your validation logic here
        required_columns = [0, 1]  # Example: columns 0 and 1 are required
        for col in required_columns:
            if not row[col]:
                return False
        return True

    def store_valid_data(data):
        postgres_conn_id = 'your_postgres_conn_id'
        common_postgres_methods = CommonPostgresMethods(postgres_conn_id)
        for row in data:
            common_postgres_methods.insert_into_dsllm_raw(row[0], row[1])
        print("Valid data stored in the database")

    def save_invalid_data(data):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        invalid_data_dir = Path(tempfile.mkdtemp(dir='/tmp/confluence_invalid_data')) / timestamp
        invalid_data_dir.mkdir(parents=True, exist_ok=True)
        invalid_data_file = invalid_data_dir / 'invalid_data.json'
        with open(invalid_data_file, 'w') as f:
            json.dump(data, f, indent=4)
        print(f"Invalid data saved to {invalid_data_file}")
        return invalid_data_dir, invalid_data_file

    def send_invalid_data_api(data, invalid_data_dir, invalid_data_file):
        api_url = 'https://api.example.com/send-email'
        payload = {
            'subject': 'Invalid Data Notification',
            'body': 'The following data rows are invalid:\n\n' + '\n'.join([str(row) for row in data]),
            'recipient': 'recipient@example.com'
        }
        # with open(invalid_data_file, 'rb') as file:
        # data = file.read()
        # # 파일 데이터를 처리하는 코드
        # print(data)
        files = {'attachment': open(invalid_data_file, 'rb')}
        response = requests.post(api_url, json=payload, files=files)
        if response.status_code == 200:
            print("Invalid data email sent via API")
            # Delete the invalid data directory
            shutil.rmtree(invalid_data_dir)
            print(f"Invalid data directory {invalid_data_dir} deleted")
        else:
            print(f"Failed to send email via API. Status code: {response.status_code}")

    @task(trigger_rule="all_success")
    def cleanup(base_tmp_dir):
        shutil.rmtree(base_tmp_dir)
        print(f"Temporary directory {base_tmp_dir} deleted")

    base_tmp_dir = create_temp_dir()
    uuid_dir = generate_uuid(base_tmp_dir)
    extracted_dir = copy_and_extract_file(uuid_dir)
    validate_and_store_data(extracted_dir)
    cleanup(base_tmp_dir)

    chain(base_tmp_dir, uuid_dir, extracted_dir, validate_and_store_data, cleanup)
    
dag = new_confluence_dag()

if __name__ == "__main__":
    dag.test()
    # dag.run()
