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
        try:
            base_tmp_dir = Path(tempfile.mkdtemp(dir='/tmp', prefix='confluence_'))
            print(f"Base temporary directory created: {base_tmp_dir}")
            return str(base_tmp_dir)
        except Exception as e:
            print(f"Error in create_temp_dir: {e}")
            raise

    @task
    def generate_uuid(base_tmp_dir):
        try:
            uuid_dir = Path(base_tmp_dir) / str(uuid.uuid4())
            uuid_dir.mkdir(parents=True, exist_ok=True)
            print(f"UUID directory created: {uuid_dir}")
            return str(uuid_dir)
        except Exception as e:
            print(f"Error in generate_uuid: {e}")
            raise

    @task
    def copy_and_extract_file(uuid_dir):
        try:
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
        except Exception as e:
            print(f"Error in copy_and_extract_file: {e}")
            raise

    @task(trigger_rule="all_success")
    def cleanup(base_tmp_dir):
        try:
            shutil.rmtree(base_tmp_dir)
            print(f"Temporary directory {base_tmp_dir} deleted")
        except Exception as e:
            print(f"Error in cleanup: {e}")
            raise

    base_tmp_dir = create_temp_dir()
    uuid_dir = generate_uuid(base_tmp_dir)
    copy_and_extract_file_task = copy_and_extract_file(uuid_dir)
    cleanup_task = cleanup(base_tmp_dir)

    chain(base_tmp_dir, uuid_dir, copy_and_extract_file_task, cleanup_task)
    
dag = new_confluence_dag()

if __name__ == "__main__":
    dag.test()
    # dag.run()
