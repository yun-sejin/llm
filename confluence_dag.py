
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import tempfile
import pendulum
import os
import zipfile
from pathlib import Path
import re
import shutil

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

        data = {'file_path': '{{ var.value.json_file_path }}'}  # Jinja 템플릿을 사용하여 변수 값 동적으로 설정
        fail_files = ['file1.json', 'file2.json']
        # Airflow Variable에서 'json_file_path' 변수의 값을 직접 가져옵니다.
        # file_path = Variable.get("json_file_path")
        # data = {'file_path': file_path}

        return data, fail_files

    @task
    def process_json_file(data):
        # XCom에서 데이터를 풀합니다.
        # data = ti.xcom_pull(task_ids='push_data')
        # file_path = data['file_path']
        # 파일 경로를 사용하여 JSON 파일 처리
        print(f"Processed {data}")

    push_data_task = push_data()
    process_file_task = process_json_file(push_data_task)

    push_data_task >> process_file_task
    
dag_instance = qna_processing_dag()

if __name__ == "__main__":
    dag_instance.test()
@dag(
    dag_id='confluence_dag',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "llm"],
    dag_display_name="Sample DAG with Display Name",
)
def confluence_dag():
    @task
    def get_job_list():
        try:
            job_id_list = ["uuid1", "uuid2", "uuid3"]
            return job_id_list
        except Exception as e:
            print(f"Error in get_job_list: {e}")
            raise

    @task
    def setup_directory():
        try:
            base_tmp_dir = Path(tempfile.mkdtemp(dir='/tmp'))
            print(f"Base temporary directory created: {base_tmp_dir}")
            return {"base_tmp_dir": str(base_tmp_dir)}
        except Exception as e:
            print(f"Error in setup_directory: {e}")
            raise

    @task
    def load_raw(base_tmp_dir_dict, job_id):
        try:
            base_tmp_dir = base_tmp_dir_dict["base_tmp_dir"]
            job_dir = Path(base_tmp_dir) / job_id
            job_dir.mkdir(parents=True, exist_ok=True)
            print(f"Job directory created: {job_dir}")

            data_folder = Path(__file__).parent / 'data'
            file_id = 'space1'
            local_zip_path = data_folder / f'{file_id}.zip'
            
            # Extract the zip file
            with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
                zip_ref.extractall(job_dir)
            print(f"Extracted {local_zip_path} to {job_dir}")
            
            return {"tmp_dir": str(job_dir), "job_id": job_id}
        except Exception as e:
            print(f"Error in load_raw: {e}")
            raise

    @task
    def dedup(load_raw_task_instance):
        try:
            tmp_dir = load_raw_task_instance[0]["tmp_dir"]
            job_id_list = [instance["job_id"] for instance in load_raw_task_instance]
            match = re.search(r'\/([^\/]+)\/([^\/]+)', tmp_dir)
            if match:
                extracted_temp_dir = match.group(0)
                print(f"Extracted value: {extracted_temp_dir}")
            else:
                print("No match found")
            print(f"load_raw_task_instance: {load_raw_task_instance}")
            
            return {"tmp_dir": str(extracted_temp_dir), "job_id_list": job_id_list}
        except Exception as e:
            print(f"Error in dedup: {e}")
            raise

    @task
    def parse(load_raw_task_instance):
        try:
            tmp_dir = load_raw_task_instance["tmp_dir"]
            job_id_list = load_raw_task_instance["job_id_list"]
            return {"tmp_dir": str(tmp_dir), "job_id_list": job_id_list}
        except Exception as e:
            print(f"Error in parse: {e}")
            raise

    @task(trigger_rule="all_success")
    def cleanup(tmp_dir_dict):
        try:
            tmp_dir = tmp_dir_dict["tmp_dir"]
            shutil.rmtree(tmp_dir)
            print(f"Temporary directory {tmp_dir} deleted")
        except Exception as e:
            print(f"Error in cleanup: {e}")
            raise

    job_id_list_task = get_job_list()
    base_tmp_dir_dict_task = setup_directory()
    load_raw_task = load_raw.expand(base_tmp_dir_dict=base_tmp_dir_dict_task, job_id=job_id_list_task)
    parse_task = parse(dedup_task)


if __name__ == "__main__":
    dag.test()
    # dag.run()
    job_id_list_task >> base_tmp_dir_dict_task >> load_raw_task >> dedup_task >> parse_task >> cleanup_task
dag = confluence_dag()
    
    cleanup_task = cleanup(parse_task)
    dedup_task = dedup(load_raw_task)
from datetime import timedelta, datetime
from airflow.models import Variable  # Variable 모듈 추가
from airflow.decorators import dag

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        # 여기서는 예시 데이터를 생성하고 XCom으로 푸시합니다.
}

@dag(schedule_interval=None, default_args=default_args, catchup=False, tags=['example'], start_date=datetime(2023, 1, 1))
def qna_processing_dag():
    @task
    def push_data():
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator, task