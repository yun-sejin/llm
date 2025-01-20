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
        job_id_list = ["uuid1", "uuid2", "uuid3"]
        return job_id_list

    @task
    def setup_directory():
        base_tmp_dir = Path(tempfile.mkdtemp(dir='/tmp'))
        print(f"Base temporary directory created: {base_tmp_dir}")
        return {"base_tmp_dir": str(base_tmp_dir)}

    @task
    def load_raw(base_tmp_dir_dict, job_id):
        base_tmp_dir = base_tmp_dir_dict[1]
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

    @task
    def dedup(load_raw_task_instance):
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

    @task
    def parse(load_raw_task_instance):
        tmp_dir = load_raw_task_instance["tmp_dir"]
        job_id_list = load_raw_task_instance["job_id_list"]
        return {"tmp_dir": str(tmp_dir), "job_id_list": job_id_list}

    @task(trigger_rule="all_success")
    def cleanup(tmp_dir_dict):
        tmp_dir = tmp_dir_dict["tmp_dir"]
        shutil.rmtree(tmp_dir)
        print(f"Temporary directory {tmp_dir} deleted")

    job_id_list_task = get_job_list()
    base_tmp_dir_dict_task = setup_directory()
    load_raw_task = load_raw.expand(base_tmp_dir_dict=base_tmp_dir_dict_task, job_id=job_id_list_task)
    dedup_task = dedup(load_raw_task)
    parse_task = parse(dedup_task)
    cleanup_task = cleanup(parse_task)

    job_id_list_task >> base_tmp_dir_dict_task >> load_raw_task >> dedup_task >> parse_task >> cleanup_task
    
dag = confluence_dag()

if __name__ == "__main__":
    dag.test()
    # dag.run()
