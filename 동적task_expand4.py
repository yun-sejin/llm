from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from datetime import datetime
import psycopg2

# DAG 정의
@dag(schedule_interval=None, start_date=days_ago(1), tags=['example'])
def example_dag():
    @task(task_id="check_for_null_success_yn")
    def check_for_null_success_yn():
        print("Checking for nulls in success_yn column...")

    @task(task_id="get_job_list")
    def get_job_list():
        # 여기에 작업 목록을 가져오는 로직 구현
        # 예시: 데이터베이스 쿼리 결과나 API 호출 결과
        job_list = ["job1", "job2", "job3"]
        print(f"Retrieved job list: {job_list}")
        return job_list

    @task()
    def get_db_connection():
        try:
            # 데이터베이스 연결 정보 설정
            connection = psycopg2.connect(
                host="your_host",  # 데이터베이스 호스트
                database="your_database_name",  # 데이터베이스 이름
                user="your_username",  # 데이터베이스 사용자 이름
                password="your_password"  # 데이터베이스 비밀번호
            )
            return connection
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error while connecting to PostgreSQL database: {error}")
            return None
    
    @task(task_id="upload_raw", multiple_outputs=True)
    def upload_raw(job_id: str, **context):
    # 데이터베이스 연결 설정
        db_connection = get_db_connection()
        cursor = db_connection.cursor()

        # for job_id in job_ids:
        try:
            # dsllm_job_hist 테이블에 INSERT
            insert_job_hist_sql = f"""
            INSERT INTO dsllm_job_hist (job_id, start_date, status)
            VALUES ('{job_id}', '{datetime.datetime.now()}', 'In Progress')
            """
            cursor.execute(insert_job_hist_sql)
            db_connection.commit()

            # dsllm_raw 테이블에 INSERT
            # 여기서는 예시 데이터를 사용합니다. 실제 데이터로 대체해야 합니다.
            insert_raw_sql = f"""
            INSERT INTO dsllm_raw (job_id, data)
            VALUES ('{job_id}', 'example_data')
            """
            cursor.execute(insert_raw_sql)

            # dsllm_job_hist 테이블의 end_date 업데이트
            update_job_hist_sql = f"""
            UPDATE dsllm_job_hist
            SET end_date = '{datetime.datetime.now()}', status = 'Completed'
            WHERE job_id = '{job_id}'
            """
            cursor.execute(update_job_hist_sql)

            # 변경 사항 커밋
            db_connection.commit()
        except psycopg2.extensions.TransactionRollbackError as e:
            # 데드락 감지 및 err_msg 업데이트
            err_msg_update_sql = f"""
            UPDATE dsllm_job_hist
            SET err_msg = 'Deadlock detected: {e}'
            WHERE job_id = '{job_id}'
            """
            cursor.execute(err_msg_update_sql)
            db_connection.commit()
            print(f"Deadlock error handled for job_id {job_id}")

        except Exception as e:
            # 기타 예외 처리
            print(f"An error occurred: {e}")

        # 데이터베이스 연결 종료
        cursor.close()
        db_connection.close()
# 이 코드는 dsllm_job_hist 테이블을 업데이트하는 과정에서 psycopg2.extensions.TransactionRollbackError 예외를 캐치하여 데드락이 감지될 경우, 해당 작업의 err_msg 컬럼을 업데이트합니다. 이를 통해 데드락 발생 시 데이터베이스에 에러 메시지를 기록할 수 있습니다. 또한, 모든 데이터베이스 작업 후에는 커서와 연결을 닫아 리소스를 정리합니다.
    print(f"Processed job IDs: {job_id}")

    @task(task_id="dedup")
    def dedup():
        print("Deduplicating data...")

    @task(task_id="parse")
    def parse():
        print("Parsing data...")

    # 태스크 인스턴스 생성
    check_task = check_for_null_success_yn()
    job_ids = get_job_list()  # Assuming get_job_list() returns a list of job IDs
    upload_tasks = upload_raw.expand(job_id=job_ids)
    dedup_task = dedup()
    parse_task = parse()

    # 태스크 의존성 설정
    chain(check_task, upload_tasks, dedup_task, parse_task)

# 기존 코드에서 file_path를 job_ids로 변경하는 과정을 반영하면, upload_raw 함수의 인자와 expand 메서드에서 사용하는 인자 이름을 모두 job_ids로 수정해야 합니다. 이 변경은 upload_raw 태스크가 파일 경로 대신 작업 ID를 기반으로 데이터를 업로드하도록 조정하는 것입니다.

# 이 코드에서 upload_raw 함수의 인자는 이제 job_id를 받으며, expand 메서드는 여러 job_id에 대해 upload_raw 태스크를 동적으로 생성합니다. 이 변경을 통해, 각 upload_raw 태스크 인스턴스는 주어진 작업 ID에 대해 데이터 업로드 작업을 수행합니다.


dag_instance = example_dag()