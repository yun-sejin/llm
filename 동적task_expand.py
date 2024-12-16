from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime

# DAG 정의
@dag(schedule_interval=None, start_date=days_ago(1), tags=['example'])
def example_dag():
    @task(task_id="check_for_null_success_yn")
    def check_for_null_success_yn():
        print("Checking for nulls in success_yn column...")

    @task(task_id="upload_raw", multiple_outputs=True)
    def upload_raw(job_id: str):
        print(f"Processing job ID: {job_id}")
        return {"job_id": job_id, "status": "Success"}

    @task(task_id="dedup")
    def dedup(job_id: str):
        print(f"Deduplicating data for job ID: {job_id}")

    @task(task_id="parse")
    def parse():
        print("Parsing data...")

    # 태스크 인스턴스 생성
    check_task = check_for_null_success_yn()
    upload_tasks = upload_raw.expand(job_id=["job1", "job2", "job3"])
    
    # 각 upload_raw 태스크의 결과를 dedup 태스크에 연결
    for upload_task in upload_tasks:
        dedup_task = dedup(upload_task['job_id'])
    
    parse_task = parse()

    # 의존성 설정
    check_task >> upload_tasks
    upload_tasks >> dedup_task
    dedup_task >> parse_task


# upload_raw 태스크에서 반환된 job_id를 dedup 태스크의 파라미터로 사용하도록 변경하는 과정은 다음과 같습니다:

# dedup 함수를 수정하여 job_id를 인자로 받을 수 있도록 합니다.
# upload_raw 태스크의 결과를 dedup 태스크에 전달하는 방식으로 코드를 조정합니다.
# expand 메서드를 사용하여 upload_raw 태스크를 동적으로 생성한 후, 각 인스턴스의 결과를 dedup 태스크에 연결합니다.

# 이 변경을 통해, upload_raw 태스크에서 처리된 각 job_id는 dedup 태스크로 전달되어, 해당 작업 ID에 대한 데이터 중복 제거 작업이 수행됩니다. expand 메서드를 사용하여 생성된 각 upload_raw 태스크 인스턴스의 결과는 해당 job_id를 사용하여 dedup 태스크에 동적으로 연결됩니다.

dag_instance = example_dag()