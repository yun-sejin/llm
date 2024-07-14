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
        return {"job_id": job_id, "status": "Deduped"}

    @task(task_id="parse")
    def parse(job_id: str):
        print(f"Parsing data for job ID: {job_id}")

    # 태스크 인스턴스 생성
    check_task = check_for_null_success_yn()
    upload_tasks = upload_raw.expand(job_id=["job1", "job2", "job3"])
    
    # 각 upload_raw 태스크의 결과를 dedup 태스크에 연결하고, 그 결과를 parse 태스크에 연결
    dedup_tasks = []
    for upload_task in upload_tasks:
        dedup_task = dedup(upload_task['job_id'])
        dedup_tasks.append(dedup_task)
        parse_task = parse(dedup_task['job_id'])
    
    # 의존성 설정
    check_task >> upload_tasks
    upload_tasks >> dedup_tasks
    dedup_tasks >> parse_task


# parse 태스크를 dedup 태스크의 결과에 동적으로 연결하려면, 다음 단계를 따릅니다:

# parse 함수를 수정하여 job_id를 인자로 받을 수 있도록 합니다.
# dedup 태스크의 결과를 parse 태스크에 전달하는 방식으로 코드를 조정합니다.
# upload_raw 태스크의 결과를 dedup 태스크에 연결한 것처럼, dedup 태스크의 결과를 parse 태스크에 연결합니다.


# 이 코드는 upload_raw 태스크에서 처리된 각 job_id를 dedup 태스크로 전달하고, dedup 태스크의 결과를 다시 parse 태스크로 전달합니다. 이를 통해 데이터 처리 파이프라인에서 각 단계가 동적으로 연결되어, 각 작업 ID에 대해 순차적으로 데이터 중복 제거 및 파싱 작업이 수행됩니다.

dag_instance = example_dag()