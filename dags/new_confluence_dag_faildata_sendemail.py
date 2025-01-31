from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from airflow.notifications.basenotifier import BaseNotifier
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

class KnoxNotifier(BaseNotifier):
    def send_notification(self, subject, body, recipient, attachment_paths=None):
        api_url = 'https://api.example.com/send-email'
        payload = {
            'subject': subject,
            'body': body,
            'recipient': recipient
        }
        files = [('attachment', open(path, 'rb')) for path in attachment_paths] if attachment_paths else None
        response = requests.post(api_url, json=payload, files=files)
        if response.status_code == 200:
            print("Notification sent via API")
            if attachment_paths:
                for path in attachment_paths:
                    invalid_data_dir = Path(path).parent
                    shutil.rmtree(invalid_data_dir)
                    print(f"Invalid data directory {invalid_data_dir} deleted")
        else:
            print(f"Failed to send notification via API. Status code: {response.status_code}")

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
        csv_file_path = save_invalid_data_to_csv(invalid_data)
        return invalid_data_dir, invalid_data_file, invalid_data, csv_file_path

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

    def save_invalid_data_to_csv(data):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        invalid_data_dir = Path(tempfile.mkdtemp(dir='/tmp/confluence_invalid_data')) / timestamp
        invalid_data_dir.mkdir(parents=True, exist_ok=True)
        csv_file_path = invalid_data_dir / 'invalid_data.csv'
        with open(csv_file_path, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['file', 'error'])
            for item in data:
                writer.writerow([item['file'], item['error']])
        print(f"Invalid data saved to {csv_file_path}")
        return csv_file_path

    def create_html_content(invalid_data):
        html_content = "<html><body><h1>Invalid Data Notification</h1><table border='1'><tr><th>File</th><th>Error</th></tr>"
        for item in invalid_data:
            html_content += f"<tr><td>{item['file']}</td><td>{item['error']}</td></tr>"
        html_content += "</table></body></html>"
        return html_content

    @task
    def send_invalid_data_api(file_path, invalid_data, csv_file_path):
        if file_path:
            notifier = KnoxNotifier()
            html_content = create_html_content(invalid_data)
            notifier.send_notification(
                subject='Invalid Data Notification',
                body=html_content,
                recipient='recipient@example.com',
                attachment_paths=[file_path, csv_file_path]
            )

    @task(trigger_rule="all_success")
    def cleanup(base_tmp_dir):
        shutil.rmtree(base_tmp_dir)
        print(f"Temporary directory {base_tmp_dir} deleted")

    base_tmp_dir = create_temp_dir()
    uuid_dir = generate_uuid(base_tmp_dir)
    extracted_dir = copy_and_extract_file(uuid_dir)
    invalid_data_dir, invalid_data_file, invalid_data, csv_file_path = validate_and_store_data(extracted_dir)
    send_invalid_data_api(invalid_data_file, invalid_data, csv_file_path)
    cleanup(base_tmp_dir)

    chain(base_tmp_dir, uuid_dir, extracted_dir, invalid_data_dir, invalid_data_file, send_invalid_data_api, cleanup)
    
dag = new_confluence_dag()

if __name__ == "__main__":
    dag.test()
    # dag.run()
