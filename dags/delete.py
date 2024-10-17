import pendulum
from airflow import DAG
from airflow.decorators import dag, task, task_sensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# DAG 정의
@dag(
    default_args=default_args,
    schedule_interval='@hourly',  # 매시간마다 DAG 실행
    catchup=False,
)
def check_upload_delete_dag():
    
    # SQL 센서 정의
    @task_sensor(poke_interval=300, timeout=60, mode='poke')  # 5분마다 실행
    def check_upload_delete():
        hook = PostgresHook(postgres_conn_id='your_postgres_connection')
        records = hook.get_records("""
            SELECT 1 
            FROM dsllm_job_his 
            WHERE job_end_point = '/delete' 
              AND job_type = 'fastapi' 
              AND after_job_id IS NULL 
              AND success_yn = TRUE
        """)
        return len(records) > 0

    # get_job_id_list 태스크 정의
    @task
    def get_job_id_list():
        hook = PostgresHook(postgres_conn_id='your_postgres_connection')
        records = hook.get_records("""
            SELECT job_id 
            FROM dsllm_job_his 
            WHERE job_end_point = '/delete' 
              AND job_type = 'fastapi' 
              AND after_job_id IS NULL 
              AND success_yn = TRUE
        """)
        return [record[0] for record in records]

    # load_raw 태스크 정의
    @task
    def load_raw(job_id):
        print(f"Processing job_id: {job_id}")
        # 여기에 실제 로드 로직을 추가합니다.

    # delete_image 태스크 정의
    @task
    def delete_image(job_id):
        image_id = job_id  # job_id를 image_id로 사용
        delete_url = f"http://repo.image.com:8080/delete/{image_id}"
        response = requests.delete(delete_url)
        if response.status_code == 200:
            print(f"Successfully deleted image with ID: {image_id}")
        else:
            print(f"Failed to delete image with ID: {image_id}, Status Code: {response.status_code}")

    # 태스크 의존성 설정
    job_ids = get_job_id_list()
    check_upload_delete() >> job_ids.expand(job_id=job_ids) >> load_raw.partial().expand(job_id=job_ids) >> delete_image.partial().expand(job_id=job_ids)

# DAG 인스턴스 생성
dag_instance = check_upload_delete_dag()