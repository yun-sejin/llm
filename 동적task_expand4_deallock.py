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
    
#     기존 코드에서 발생한 데드락 문제를 해결하기 위해, 다음과 같은 수정 사항을 적용할 수 있습니다:

# 트랜잭션 관리 개선: 각 INSERT 및 UPDATE 작업 전후로 트랜잭션을 명시적으로 관리하여 데드락 발생 가능성을 줄입니다.
# 재시도 로직 추가: 데드락이 감지될 경우, 작업을 재시도하는 로직을 추가합니다.
# 리소스 접근 순서 통일: 가능하다면, 모든 작업이 데이터베이스 리소스에 접근하는 순서를 통일하여 데드락 발생 가능성을 줄입니다.

# 이 수정된 코드는 다음과 같은 방식으로 데드락 문제를 해결합니다:

# 명시적 트랜잭션 관리: autocommit을 False로 설정하여 명시적으로 트랜잭션을 시작하고, 작업 성공 시 커밋하거나 실패 시 롤백합니다.
# 파라미터화된 쿼리: SQL 인젝션을 방지하기 위해 파라미터화된 쿼리를 사용합니다.
# 재시도 로직: 데드락이 감지될 경우, 작업을 재시도합니다. 이 예시에서는 간단히 1초 후에 재시도하며, 실제 환경에서는 지수 백오프 등의 방식을 고려할 수 있습니다.


    @task(task_id="upload_raw", multiple_outputs=True)
    def upload_raw(job_id: str, **context):
        db_connection = get_db_connection()
        if db_connection is None:
            return {"status": "Failed", "job_id": job_id}
        cursor = db_connection.cursor()

        try:
            # 트랜잭션 시작
            db_connection.autocommit = False

            # dsllm_job_hist 테이블에 INSERT
            insert_job_hist_sql = """
            INSERT INTO dsllm_job_hist (job_id, start_date, status)
            VALUES (%s, %s, 'In Progress')
            """
            cursor.execute(insert_job_hist_sql, (job_id, datetime.datetime.now()))

            # dsllm_raw 테이블에 INSERT
            insert_raw_sql = """
            INSERT INTO dsllm_raw (job_id, data)
            VALUES (%s, 'example_data')
            """
            cursor.execute(insert_raw_sql, (job_id,))

            # dsllm_job_hist 테이블의 end_date 업데이트
            update_job_hist_sql = """
            UPDATE dsllm_job_hist
            SET end_date = %s, status = 'Completed'
            WHERE job_id = %s
            """
            cursor.execute(update_job_hist_sql, (datetime.datetime.now(), job_id))

            # 트랜잭션 커밋
            db_connection.commit()

        except psycopg2.extensions.TransactionRollbackError as e:
            db_connection.rollback()  # 롤백
            print(f"Deadlock detected for job_id {job_id}, retrying...")

            # 재시도 로직 (간단한 예시, 실제로는 지수 백오프 등을 고려할 수 있음)
            time.sleep(1)  # 잠시 대기 후 재시도
            return upload_raw(job_id, **context)  # 재귀적으로 재시도

        except Exception as e:
            db_connection.rollback()  # 예외 발생 시 롤백
            print(f"An error occurred for job_id {job_id}: {e}")
            return {"status": "Failed", "job_id": job_id}

        finally:
            if cursor is not None:
                cursor.close()
            if db_connection is not None:
                db_connection.close()

        return {"status": "Success", "job_id": job_id}

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