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
                # data_list는 삽입할 데이터의 리스트로, 각 항목은 (src_id, data_column) 형태의 튜플입니다.
                data_list = [
                (1, 'example_data_1'),
                (2, 'example_data_2'),
                (3, 'example_data_3'),
                    # 여기에 더 많은 데이터 튜플을 추가할 수 있습니다.
                ]
               
                # 임시 테이블 생성
                cursor.execute("""
                    CREATE TEMP TABLE temp_table AS
                    SELECT * FROM dsllm_raw WHERE 1=0;
                """)

                # execute_batch를 사용하여 temp_table에 데이터 삽입
                query = "INSERT INTO temp_table (src_id, data_column) VALUES (%s, %s)"
                psycopg2.extras.execute_batch(cursor, query, data_list)

                # execute_values를 사용하여 temp_table에 데이터 삽입
                query = "INSERT INTO temp_table (src_id, data_column) VALUES %s"
                psycopg2.extras.execute_values(cursor, query, data_list)
                
                # temp_table과 dsllm_raw를 src_id로 조인하여 dsllm_raw_insert에 데이터 삽입
                cursor.execute("""
                    INSERT INTO dsllm_raw_insert (src_id, data_column)
                    SELECT t.src_id, t.data_column
                    FROM temp_table t
                    JOIN dsllm_raw d ON t.src_id = d.src_id
                """)


# execute_batch의 실행 시간을 10초 이내로 단축하기 위해 몇 가지 방법을 시도할 수 있습니다. 데이터베이스 삽입 작업의 성능을 향상시키는 일반적인 접근 방법은 다음과 같습니다:

# Batch Size 조정: execute_batch의 배치 크기를 조정하여 최적의 성능을 찾습니다. 너무 큰 배치 크기는 메모리 사용량을 증가시킬 수 있고, 너무 작은 배치 크기는 네트워크 호출 오버헤드를 증가시킬 수 있습니다.
    # 시도할 배치 크기 리스트
    # batch_sizes = [10, 100, 500, 1000, 5000]
# 인덱스 일시적 제거: 데이터 삽입 전에 대상 테이블의 인덱스를 일시적으로 제거하고, 데이터 삽입 후 인덱스를 다시 생성합니다. 인덱스 재구성은 데이터 삽입 속도를 늦출 수 있기 때문입니다.

        # 인덱스를 일시적으로 제거하는 작업은 데이터 삽입 성능을 향상시키기 위한 일반적인 전략 중 하나입니다. 이 접근법은 특히 대량의 데이터를 삽입할 때 유용합니다. 인덱스가 있을 경우, 각 삽입 작업마다 인덱스가 업데이트되어야 하므로 성능이 저하될 수 있습니다. 인덱스를 제거하고 데이터 삽입 작업을 완료한 후에 다시 생성하면, 이러한 오버헤드를 줄일 수 있습니다.

        # 아래는 PostgreSQL을 사용하여 dsllm_raw 테이블의 인덱스를 일시적으로 제거하고, 데이터 삽입 작업 후에 다시 생성하는 방법을 보여줍니다:

        # 인덱스 제거:

        # 먼저, 제거할 인덱스의 이름을 확인합니다. 이는 \d dsllm_raw 명령을 통해 psql 콘솔에서 확인할 수 있습니다.
        # 인덱스를 제거합니다. 예를 들어, 인덱스 이름이 dsllm_raw_idx라면, 다음 SQL 명령을 사용합니다:
        
                # DROP INDEX dsllm_raw_idx;
        
        # 데이터 삽입:

        # 인덱스 제거 후, 필요한 데이터 삽입 작업을 수행합니다.
        # 인덱스 재생성:

                # CREATE INDEX dsllm_raw_idx ON dsllm_raw(key_column);

        # 데이터 삽입 작업이 완료된 후, 인덱스를 다시 생성합니다. 인덱스 생성 시, 원래의 인덱스 정의를 사용합니다. 예를 들어, key_column에 대한 인덱스를 다시 생성한다면, 다음과 같은 SQL 명령을 사용할 수 있습니다:
        # 이 절차를 수행할 때는 데이터베이스의 다운타임과 성능 저하를 고려해야 합니다. 인덱스를 제거하고 재생성하는 동안, 해당 인덱스를 사용하는 쿼리의 성능이 저하될 수 있으므로, 이 작업은 저통량 시간대에 수행하는 것이 좋습니다.

        # 또한, 인덱스 재생성은 데이터베이스의 리소스를 상당량 사용할 수 있으므로, 이 작업을 시작하기 전에 충분한 리소스가 확보되어 있는지 확인해야 합니다.

        # 임시 테이블을 생성하고 dsllm_dedup 테이블과 같은 구조를 가지도록 하되, 인덱스 없이 생성하는 작업은 다음과 같은 절차를 따릅니다:

        # dsllm_dedup 테이블의 구조를 조회합니다. 이를 위해 INFORMATION_SCHEMA.COLUMNS 또는 pg_catalog.pg_attribute를 사용할 수 있습니다.
        # 조회한 구조를 기반으로 임시 테이블 생성 쿼리를 작성합니다. 이 때, 인덱스는 포함하지 않습니다.
        # 생성 쿼리를 실행하여 임시 테이블을 생성합니다.
        # 아래는 PostgreSQL을 사용하는 예시 코드입니다:

            # cursor.execute("""
            #     CREATE TEMP TABLE temp_dsllm_dedup AS
            #     SELECT *
            #     FROM dsllm_dedup
            #     WHERE 1=0;
            # """)

        # 이 코드는 dsllm_dedup 테이블의 구조를 복사하여 temp_dsllm_dedup라는 임시 테이블을 생성합니다. WHERE 1=0 조건은 실제 데이터를 복사하지 않고 구조만 복사하기 위해 사용됩니다. 이 방법은 인덱스를 포함하지 않으므로, 요구사항에 부합합니다.

# 트랜잭션 관리: 가능하다면, 전체 배치 작업을 단일 트랜잭션으로 처리합니다. 이는 트랜잭션 오버헤드를 줄이고 성능을 향상시킬 수 있습니다.
        # 전체 배치 작업을 단일 트랜잭션으로 처리하려면, 작업 시작 전에 트랜잭션을 시작하고, 모든 작업이 성공적으로 완료된 후에 트랜잭션을 커밋해야 합니다. 만약 작업 중에 오류가 발생하면, 트랜잭션을 롤백하여 변경사항을 취소해야 합니다. 아래는 이 과정을 구현한 예시 코드입니다:
        # 트랜잭션 시작
        # conn.autocommit = False

        #  # 모든 작업이 성공적으로 완료되면 트랜잭션 커밋
        # conn.commit()

        #  # 작업 중 오류 발생 시 트랜잭션 롤백
        # conn.rollback()
        # 이 코드는 다음 단계를 따릅니다:

        # 데이터베이스에 연결합니다.
        # 트랜잭션을 시작하기 위해 autocommit을 False로 설정합니다.
        # 배치 작업을 실행합니다. 여기서는 temp_table에 데이터를 삽입하는 작업을 예로 들었습니다.
        # 모든 작업이 성공적으로 완료되면 commit()을 호출하여 트랜잭션을 커밋합니다.
        # 작업 중 오류가 발생하면 rollback()을 호출하여 트랜잭션

# 병렬 처리: 데이터를 여러 부분으로 나누고, 각 부분을 별도의 스레드나 프로세스에서 병렬로 삽입합니다. 이 방법은 데이터베이스와 애플리케이션 서버 모두에서 충분한 리소스가 있을 때 효과적일 수 있습니다.

        # 병렬 처리를 위해 Python의 concurrent.futures 모듈을 사용하여 별도의 스레드나 프로세스에서 데이터를 병렬로 삽입하는 방법을 구현할 수 있습니다. 여기서는 스레드를 사용하는 방법을 예시로 들겠습니다. ThreadPoolExecutor를 사용하면 간단하게 병렬 처리를 구현할 수 있습니다.

        # 병렬 처리를 구현하기 위한 단계는 다음과 같습니다:

        # 데이터를 삽입할 함수를 정의합니다.
        # concurrent.futures.ThreadPoolExecutor를 사용하여 병렬 처리를 구현합니다.
        # 데이터를 여러 부분으로 나누고 각 부분을 별도의 스레드에서 처리하도록 합니다.
        # 아래는 이를 구현한 예시 코드입니다:


        # import concurrent.futures
        # import psycopg2

        # def insert_data(conn_info, data_chunk):
        #     """
        #     데이터베이스에 데이터를 삽입하는 함수.
        #     conn_info: 데이터베이스 연결 정보
        #     data_chunk: 삽입할 데이터의 부분 리스트
        #     """
        #     conn = psycopg2.connect(conn_info)
        #     cur = conn.cursor()
        #     try:
        #         for data in data_chunk:
        #             # 데이터 삽입 쿼리 실행. 예시 쿼리입니다.
        #             cur.execute("INSERT INTO your_table (column1, column2) VALUES (%s, %s)", (data[0], data[1]))
        #         conn.commit()
        #     except Exception as e:
        #         conn.rollback()
        #         print(f"Error: {e}")
        #     finally:
        #         cur.close()
        #         conn.close()

        # def parallel_insert(conn_info, data_list, num_threads):
        #     """
        #     병렬로 데이터를 삽입하는 함수.
        #     conn_info: 데이터베이스 연결 정보
        #     data_list: 삽입할 전체 데이터 리스트
        #     num_threads: 사용할 스레드의 수
        #     """
        #     # 데이터 리스트를 스레드 수에 맞게 분할
        #     chunk_size = len(data_list) // num_threads
        #     data_chunks = [data_list[i:i + chunk_size] for i in range(0, len(data_list), chunk_size)]

        #     # ThreadPoolExecutor를 사용하여 병렬 처리
        #     with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        #         futures = [executor.submit(insert_data, conn_info, chunk) for chunk in data_chunks]
        #         for future in concurrent.futures.as_completed(futures):
        #             future.result()  # 결과(또는 예외)를 기다립니다.

        # # 데이터베이스 연결 정보
        # conn_info = "dbname='your_db' user='your_user' host='your_host' password='your_password'"

        # # 삽입할 데이터 리스트 예시
        # data_list = [
        #     (1, 'data1'),
        #     (2, 'data2'),
        #     # 추가 데이터...
        # ]

        # # 병렬 삽입 실행
        # parallel_insert(conn_info, data_list, num_threads=4)

        # 이 코드는 데이터를 여러 스레드에서 병렬로 삽입하여 성능을 향상시키는 방법을 보여줍니다. insert_data 함수는 데이터베이스에 데이터를 삽입하는 역할을 하며, parallel_insert 함수는 이를 병렬로 실행합니다. 데이터 리스트를 스레드 수에 맞게 분할하여 각 스레드가 데이터의 한 부분을 처리하도록 합니다.

# 임시 테이블 사용: 데이터를 임시 테이블에 먼저 삽입하고, 그 후에 대량의 데이터를 메인 테이블로 이동합니다. 이 방법은 데이터베이스에 따라 성능을 크게 향상시킬 수 있습니다.

        # 이 요청을 수행하기 위해, 우리는 다음 단계를 따를 것입니다:

        # 임시 테이블 생성: 메인 테이블과 동일한 구조를 가진 임시 테이블을 생성합니다.
        # 임시 테이블에 데이터 삽입: 병렬 처리를 사용하여 임시 테이블에 데이터를 삽입합니다.
        # 메인 테이블로 데이터 이동: 임시 테이블에서 메인 테이블로 데이터를 이동합니다. 이 과정에서는 INSERT INTO ... SELECT FROM 구문을 사용할 수 있습니다.
        # 임시 테이블 정리: 데이터 이동 후 임시 테이블을 정리합니다.
        # 아래는 이 과정을 구현한 파이썬 코드의 예시입니다:

                # import psycopg2
                # from threading import Thread

                # def create_temp_table(conn_info):
                #     with psycopg2.connect(conn_info) as conn:
                #         with conn.cursor() as cur:
                #             cur.execute("""
                #                 CREATE TEMP TABLE temp_data_like_main (
                #                     id SERIAL PRIMARY KEY,
                #                     data TEXT
                #                 );
                #             """)

                # def insert_into_temp_table(conn_info, data_list):
                #     with psycopg2.connect(conn_info) as conn:
                #         with conn.cursor() as cur:
                #             psycopg2.extras.execute_batch(cur, """
                #                 INSERT INTO temp_data_like_main (data) VALUES (%s);
                #             """, data_list)

                # def move_data_to_main_table(conn_info):
                #     with psycopg2.connect(conn_info) as conn:
                #         with conn.cursor() as cur:
                #             cur.execute("""
                #                 INSERT INTO main_table (id, data)
                #                 SELECT id, data FROM temp_data_like_main;
                #             """)

                # def parallel_insert(conn_info, data_list, num_threads=4):
                #     # 데이터 리스트를 스레드 수에 따라 분할
                #     chunk_size = len(data_list) // num_threads
                #     threads = []

                #     for i in range(0, len(data_list), chunk_size):
                #         chunk = data_list[i:i + chunk_size]
                #         thread = Thread(target=insert_into_temp_table, args=(conn_info, chunk))
                #         threads.append(thread)
                #         thread.start()

                #     for thread in threads:
                #         thread.join()

                #     # 메인 테이블로 데이터 이동
                #     move_data_to_main_table(conn_info)

                # # 데이터베이스 연결 정보
                # conn_info = "dbname='your_db' user='your_user' host='your_host' password='your_password'"

                # # 임시 테이블 생성
                # create_temp_table(conn_info)

                # # 삽입할 데이터 리스트 예시
                # data_list = [
                #     ('data1',),
                #     ('data2',),
                #     # 추가 데이터...
                # ]

                # # 병렬 삽입 실행
                # parallel_insert(conn_info, data_list, num_threads=4)

        # 이 코드는 다음과 같은 작업을 수행합니다:

        # create_temp_table 함수는 임시 테이블을 생성합니다.
        # insert_into_temp_table 함수는 임시 테이블에 데이터를 삽입합니다.
        # move_data_to_main_table 함수는 임시 테이블의 데이터를 메인 테이블로 이동합니다.
        # parallel_insert 함수는 데이터 리스트를 여러 스레드로 분할하여 병렬로 임시 테이블에 삽입한 후, 메인 테이블로 데이터를 이동합니다.
        # 이 접근 방식은 대량의 데이터 삽입 작업을 최적화하고, 데이터베이스의 성능을 향상시키는 데 도움이 될 수 있습니다.

# 데이터베이스 설정 최적화: 데이터베이스의 설정을 조정하여 삽입 성능을 향상시킬 수 있습니다. 예를 들어, PostgreSQL에서는 wal_level, checkpoint_segments, max_wal_size 등의 설정을 조정할 수 있습니다.
        # 데이터베이스의 설정을 조정하여 삽입 성능을 향상시키는 방법은 데이터베이스 유형(예: PostgreSQL, MySQL 등)에 따라 다릅니다. PostgreSQL을 예로 들어, 성능 향상을 위해 조정할 수 있는 몇 가지 설정을 소개하겠습니다. 이 설정들은 대량의 데이터 삽입 작업을 최적화하는 데 도움이 될 수 있습니다.

        # wal_level 설정 조정: wal_level을 minimal로 설정하면 WAL(Write-Ahead Logging) 데이터의 양을 줄여 성능을 향상시킬 수 있습니다. 하지만, 이 설정은 복제가 필요하지 않은 경우에만 사용해야 합니다.

        # checkpoint_segments와 max_wal_size 조정: 이 설정들을 늘리면 체크포인트 사이의 간격이 커져서 쓰기 성능이 향상될 수 있습니다. 체크포인트가 덜 자주 발생하므로, 디스크 I/O 부하가 줄어듭니다.

        # synchronous_commit 비활성화: synchronous_commit을 off로 설정하면 트랜잭션이 디스크에 완전히 기록되기를 기다리지 않아도 됩니다. 이는 응답 시간을 개선하지만, 시스템 장애 시 데이터 손실 위험이 증가할 수 있습니다.

        # work_mem 설정 조정: work_mem을 증가시키면 정렬 작업과 해시 테이블 작업에 사용할 수 있는 메모리 양이 증가합니다. 대량의 데이터를 처리할 때 성능을 향상시킬 수 있습니다.

        # 이러한 설정을 조정하기 위해 PostgreSQL의 설정 파일인 postgresql.conf를 수정하거나, SQL 명령어를 사용할 수 있습니다. 예를 들어, synchronous_commit을 비활성화하는 방법은 다음과 같습니다:

        # -- SQL 명령어를 사용하여 설정 변경
        # ALTER SYSTEM SET synchronous_commit TO 'off';
        # -- 변경 사항 적용을 위해 데이터베이스 재시작 또는 로드 필요
        # SELECT pg_reload_conf();

        # 데이터베이스 설정을 조정할 때는 변경 사항이 시스템에 미치는 영향을 충분히 이해하고, 테스트 환경에서 성능 테스트를 수행한 후 프로덕션 환경에 적용하는 것이 중요합니다. 설정 변경은 데이터베이스의 안정성과 성능에 큰 영향을 줄 수 있으므로, 주의 깊게 진행해야 합니다.




                # dsllm_job_hist 테이블의 end_date 업데이트
                update_job_hist_sql = """
                UPDATE dsllm_job_hist
                SET end_date = %s, status = 'Completed'
                WHERE job_id = %s
                """
                cursor.execute(update_job_hist_sql, (datetime.datetime.now(), job_id))

                 # 모든 작업이 성공적으로 완료되면 트랜잭션 커밋
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
                # 작업 중 오류 발생 시 트랜잭션 롤백
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