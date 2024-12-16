from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from datetime import datetime
import psycopg2
from psycopg2 import IntegrityError
from psycopg2 import connect, extras
import time

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


# 이 코드는 예외가 발생할 경우 dsllm_job_hist 테이블의 err_msg 컬럼에 에러 메시지를 업데이트합니다. 에러 메시지에 포함될 수 있는 단일 인용부호(')가 SQL 쿼리를 깨뜨리지 않도록 메시지를 포맷팅하는 과정을 포함합니다. 또한, psycopg2.extensions.TransactionRollbackError 예외(데드락 감지)의 경우에는 재시도 로직을 계속 진행합니다.
                                                                                           

    @task(task_id="upload_raw", multiple_outputs=True)
    def upload_raw(job_id: str, **context):
        # db_connection = get_db_connection()
        # if db_connection is None:
        #     return {"status": "Failed", "job_id": job_id}
        # cursor = db_connection.cursor()

        max_retries = 5  # 최대 재시도 횟수
        retry_delay = 1  # 초기 대기 시간 (초)

        for attempt in range(max_retries):
            db_connection = get_db_connection()
            if db_connection is None:
                print(f"Failed to connect to the database on attempt {attempt + 1}")
                time.sleep(retry_delay)
                retry_delay *= 2  # 지수 백오프: 대기 시간을 2배로 증가
                continue

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
                data_list = [
                    ("key1", "value1"),
                    ("key2", "value2"),
                    # 여기에 더 많은 데이터 튜플을 추가할 수 있습니다.
                ]
                insert_query = "INSERT INTO dsllm_raw (key_column, data_column) VALUES (%s, %s)"
                extras.execute_batch(cursor, insert_query, data_list)

                # dsllm_job_hist 테이블의 end_date 업데이트
                update_job_hist_sql = """
                UPDATE dsllm_job_hist
                SET end_date = %s, status = 'Completed'
                WHERE job_id = %s
                """
                cursor.execute(update_job_hist_sql, (datetime.datetime.now(), job_id))

                # 트랜잭션 커밋
                db_connection.commit()
                return {"status": "Success", "job_id": job_id}
            # 이 코드는 update_error_msg 함수를 호출하여 dsllm_job_hist 테이블의 err_msg 컬럼을 업데이트합니다. update_error_msg 함수의 정확한 구현은 이미 제공되었다고 가정하며, 여기서는 그 함수를 호출하는 방식만을 보여줍니다. IntegrityError 예외가 발생할 때 이 함수를 사용하여 에러 메시지를 데이터베이스에 기록합니다.
            except IntegrityError as e:
                # 키 중복 에러 발생 시 처리
                db_connection.rollback()
                error_message = str(e).replace("'", "''")  # SQL 쿼리를 위한 에러 메시지 포맷팅
                # dsllm_job_hist 테이블의 err_msg 컬럼 업데이트
                update_error_msg_sql = """
                UPDATE dsllm_job_hist
                SET err_msg = %s
                WHERE job_id = %s
                """
                cursor.execute(update_error_msg_sql, (error_message, job_id))
                db_connection.commit()

                # update_error_msg 함수를 사용하여 에러 메시지 업데이트
                # update_error_msg(db_connection, job_id, str(e))
                print(f"Key duplication error for job_id {job_id}: {e}")
            except psycopg2.extensions.TransactionRollbackError as e:
                db_connection.rollback()
                print(f"Deadlock detected for job_id {job_id}, retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # 지수 백오프: 대기 시간을 2배로 증가
                if attempt == max_retries - 1:  # 마지막 시도에서도 실패한 경우
                    update_error_msg(db_connection, job_id, str(e))
                continue  # 데드락 예외의 경우 재시도

            except Exception as e:
                db_connection.rollback()  # 예외 발생 시 롤백
                if attempt == max_retries - 1:  # 마지막 시도에서도 실패한 경우
                    update_error_msg(db_connection, job_id, str(e))
                print(f"An error occurred for job_id {job_id}: {e}")
                return {"status": "Failed", "job_id": job_id}

            finally:
                cursor.close()
                db_connection.close()

        print(f"Failed to complete job {job_id} after {max_retries} attempts.")
        return {"status": "Failed", "job_id": job_id}

    def update_error_msg(db_connection, job_id, error_message):
        error_message = error_message.replace("'", "''")  # SQL 쿼리를 위한 에러 메시지 포맷팅
        with db_connection.cursor() as cursor:
            update_error_msg_sql = """
            UPDATE dsllm_job_hist
            SET err_msg = %s
            WHERE job_id = %s
            """
            cursor.execute(update_error_msg_sql, (error_message, job_id))
        db_connection.commit()  # 에러 메시지 업데이트를 커밋
        print(f"Error message updated for job_id {job_id}.")

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