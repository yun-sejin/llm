주어진 코드를 Storage라는 부모 클래스로 리팩토링하고, storage 메서드에 INSERT INTO dsllm_deduped 쿼리를 추가하도록 하겠습니다. 또한, QnaStorage와 PisStorage 클래스를 정의하여 각각의 작업을 수행하도록 하겠습니다.

부모 클래스 Storage 정의


import psycopg2

class Storage:
    def __init__(self, conn_info, src_type, src_sub_type):
        self.conn_info = conn_info
        self.src_type = src_type
        self.src_sub_type = src_sub_type

    def get_db_connection(self):
        return psycopg2.connect(self.conn_info)

    def handle_deadlock(self, cursor, job_id, error_message):
        err_msg_update_sql = f"""
        UPDATE dsllm_job_hist
        SET err_msg = 'Deadlock detected: {error_message}'
        WHERE job_id = '{job_id}'
        """
        cursor.execute(err_msg_update_sql)
        cursor.connection.commit()
        print(f"Deadlock error handled for job_id {job_id}")

    def storage(self, job_id, data, error_message):
        db_connection = None
        cursor = None
        try:
            db_connection = self.get_db_connection()
            cursor = db_connection.cursor()
            insert_raw_sql = f"""
            INSERT INTO dsllm_deduped (job_id, data, src_type, src_sub_type)
            VALUES ('{job_id}', '{data}', '{self.src_type}', '{self.src_sub_type}')
            """
            cursor.execute(insert_raw_sql)
            db_connection.commit()
            print(f"Query executed successfully for job_id {job_id}")
        except psycopg2.extensions.TransactionRollbackError as e:
            # 데드락 예외 처리
            self.handle_deadlock(cursor, job_id, str(e))
        except Exception as e:
            # 기타 예외 처리
            print(f"An error occurred: {e}")
        finally:
            # 데이터베이스 연결 종료
            if cursor:
                cursor.close()
            if db_connection:
                db_connection.close()


                자식 클래스 QnaStorage 및 PisStorage 정의

class QnaStorage(Storage):
    def dedup(self, job_id_list):
        for job_id in job_id_list:
            data = 'example_data'  # 실제 데이터로 대체해야 합니다.
            self.storage(job_id, data, "Deduplication error")
            print(f"Deduplicating data for job_id {job_id}")

class PisStorage(Storage):
    def parse(self):
        print("Parsing data...")
        # 실제 파싱 로직을 여기에 추가


        
사용 예시

conn_info = "dbname='your_db' user='your_user' host='your_host' password='your_password'"
src_type = "example_src_type"
src_sub_type = "example_src_sub_type"

# QnaStorage 사용
qna_storage = QnaStorage(conn_info, src_type, src_sub_type)
job_id_list = ['job1', 'job2', 'job3']
qna_storage.dedup(job_id_list)

# PisStorage 사용
pis_storage = PisStorage(conn_info, src_type, src_sub_type)
pis_storage.parse()

주요 변경 사항
부모 클래스 Storage: 데이터베이스 연결 및 예외 처리를 포함한 공통 기능을 제공하는 Storage 클래스를 정의했습니다. __init__ 메서드에 src_type과 src_sub_type을 추가하여 초기화합니다.
storage 메서드: 데이터베이스 작업을 처리하고 예외를 처리하는 storage 메서드를 추가했습니다. INSERT INTO dsllm_deduped 쿼리를 포함하여 데이터를 삽입합니다.
자식 클래스 QnaStorage 및 PisStorage: Storage 클래스를 상속받아 각각 중복 제거 작업과 데이터 파싱 작업을 수행하는 자식 클래스를 정의했습니다.
사용 예시: QnaStorage와 PisStorage 클래스를 인스턴스화하고, 각각의 작업을 수행하는 예시를 추가했습니다.
이 구조를 사용하면 공통 기능을 Storage 클래스에 캡슐화하고, 각 작업을 수행하는 자식 클래스에서 이를 재사용할 수 있습니다. src_type과 src_sub_type을 초기화하여 각 작업에서 사용할 수 있습니다.

