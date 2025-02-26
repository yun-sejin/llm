import json
from sqlalchemy import create_engine, text
import boto3
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.operators.python import get_current_context

# Build the PostgreSQL connection URL
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@localhost:5432/llmdp"

default_args = {
    "owner": "airflow",
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["read"],
    params={"bucket_name": "your-s3-bucket-name"}  # Set to "upload_dev" or "upload_prod" as needed.
)
def read_dsllm_job_history_pipeline():
    
    @task
    def read_job_history():
        try:
            # Create an SQLAlchemy engine with pooling options.
            
#             이 코드는 SQLAlchemy의 create_engine 함수를 사용하여 PostgreSQL 데이터베이스에 연결하기 위한 엔진 객체를 생성합니다. 각 옵션의 의미는 다음과 같습니다:

# DATABASE_URL: 데이터베이스 연결 URL로, 여기서는 PostgreSQL에 연결하기 위한 URL입니다.
# pool_size=5: 연결 풀에서 유지할 최초의 연결 수를 5개로 설정합니다.
# max_overflow=10: 기본 풀 크기(pool_size)를 초과하여 동시에 추가로 생성할 수 있는 최대 연결 수를 10개로 지정합니다. 즉, 최대 5 + 10 = 15개의 연결이 사용될 수 있습니다.
# pool_recycle=3600: 연결이 3600초(1시간) 후에 재활용(리사이클)되도록 설정하여, 장시간 유지되어 서버측에서 연결이 끊어지는(stale connection) 것을 방지합니다.
# pool_pre_ping=True: 연결을 사용하기 전에 자동으로 ping 테스트를 실행하여, 연결이 정상적인지 확인합니다. 만약 연결이 유효하지 않으면 새 연결을 생성하도록 합니다.
# 이러한 옵션들을 통해 데이터베이스 연결의 재사용, 안정성, 및 성능을 향상시킬 수 있습니다.

# max_overflow 옵션에는 SQLAlchemy 자체에서 정해진 상한이 없습니다.
# 즉, 원하는 만큼의 정수 값을 설정할 수 있지만, 실제로는 PostgreSQL 서버의 max_connections 설정과 시스템 자원에 따라 제한됩니다.
# 너무 큰 값을 설정하면 연결 관리에 부정적인 영향을 줄 수 있으므로, 서버와 애플리케이션 환경에 맞게 적절한 값을 선택해야 합니다.


# PostgreSQL 서버의 max_connections 값을 확인하는 방법은 다음과 같습니다:

# SQL 쿼리 실행
# 데이터베이스에 접속하여 아래 SQL 쿼리를 실행하면 됩니다:
    
# SHOW max_connections;

# 또는 다음과 같이 current_setting 함수를 사용할 수도 있습니다:
    
# SELECT current_setting('max_connections');

# PostgreSQL 설정 파일 확인
# PostgreSQL의 설정 파일인 postgresql.conf 내의 max_connections 항목을 확인할 수 있습니다. 터미널에서 다음 명령어를 사용해 보세요:

# grep max_connections /path/to/your/postgresql.conf

# 위의 방법들을 사용하면 현재 PostgreSQL 서버에서 허용하는 최대 연결 수를 확인할 수 있습니다.


            engine = create_engine(
                DATABASE_URL, 
                pool_size=5,              # initial pool size
                max_overflow=10,          # allow up to 10 extra connections
                pool_recycle=3600,        # recycle connections after 1 hour 
                pool_pre_ping=True        # test connections before using
            )
            query = text("""
                    SELECT h1.file_path_lists
                    FROM dsllm_job_hist h1
                    JOIN dsllm_job_hist h2 ON h1.after_job_id = h2.job_id
                    WHERE h1.job_type = 'fastapi'
                      AND h1.success_yn = TRUE
                      AND h1.after_job_id IS NOT NULL
                      AND h2.job_type = 'airflow'
                      AND h2.success_yn = TRUE
                      AND h2.err_msg IS NULL
                    """)
            with engine.connect() as connection:
                result = connection.execute(query)
                all_rows = result.fetchall()
                print(f"Total file_path_list rows: {len(all_rows)}")
            engine.dispose()
            return all_rows
        except Exception as e:
            print(f"Error reading job history: {e}")
            raise
        
#         이 부분의 코드는 SQLAlchemy 엔진을 사용하여 데이터베이스 쿼리를 실행하고, 결과를 가져온 후 연결 자원을 정리하는 역할을 합니다. 각 라인은 다음과 같이 동작합니다:

# with engine.connect() as connection:

# 엔진이 관리하는 연결 풀에서 데이터베이스 연결을 하나 가져와 connection 변수에 할당합니다.
# with 문을 사용하면 블록이 끝날 때 자동으로 연결이 반환(닫힘)되므로, 수동으로 연결을 닫을 필요가 없습니다.
# result = connection.execute(query)

# 지정된 query를 실행하고 그 결과를 result 객체에 저장합니다.
# query는 텍스트 형태의 SQL 쿼리로, 데이터베이스에서 원하는 데이터를 조회합니다.
# all_rows = result.fetchall()

# 실행된 쿼리의 모든 결과 행을 리스트 형식으로 가져옵니다.
# all_rows에는 조회된 모든 데이터가 저장됩니다.
# print(f"Total file_path_list rows: {len(all_rows)}")

# 조회된 결과 행의 총 개수를 출력하여 몇 개의 행이 조회되었는지 확인합니다.
# engine.dispose()

# 엔진이 사용한 연결 풀의 모든 연결을 종료합니다.
# 더 이상 해당 엔진을 사용하지 않을 때 호출하여, 관련 리소스를 해제할 수 있습니다.
# 이렇게 작성된 코드는 데이터베이스에 안전하게 연결하고 쿼리 결과를 처리한 후, 사용한 리소스를 올바르게 정리하도록 설계되었습니다.


# SQLAlchemy에서 엔진(engine)은 내부적으로 연결 풀(connection pool)을 관리합니다. 연결 풀은 데이터베이스에 대한 여러 연결들을 미리 생성해 놓고 재사용함으로써, 매번 새로운 연결을 생성하는 오버헤드를 줄여 성능과 효율성을 높이는 역할을 합니다.

# 아래는 연결 풀과 관련된 옵션들이 어떻게 작동하는지 설명한 내용입니다:

# pool_size
# 연결 풀에 유지될 초기 연결 수를 지정합니다.
# 예를 들어, pool_size=5라면 엔진을 생성할 때 5개의 연결이 미리 생성되어 풀에 저장됩니다.

# max_overflow
# 초기 풀 크기를 초과하여 동시에 추가로 생성할 수 있는 최대 연결 수를 지정합니다.
# 예를 들어, max_overflow=10이면 기본 5개의 연결 외에 최대 10개의 연결까지 추가로 생성할 수 있어, 총 15개의 연결을 사용할 수 있습니다.

# pool_recycle
# 지정된 시간(초) 이후 연결을 재활용(리사이클)하도록 설정합니다.
# 이 옵션은 오래된 연결로 인해 발생할 수 있는 서버 측의 연결 종료(stale connection)를 방지하는 데 유용합니다.
# 예: pool_recycle=3600은 1시간이 지난 연결을 폐기하고 새 연결로 교체합니다.

# pool_pre_ping
# 연결을 사용하기 전에 연결이 유효한지 확인하기 위해 “ping” 테스트를 실행합니다.
# 만약 기존 연결이 유효하지 않다면 새 연결을 생성하여 반환합니다.
# 이 옵션은 네트워크 문제나 서버 재시작 등으로 인해 끊긴 연결을 사용하지 않도록 보장합니다.

# 예제 코드에서 SQLAlchemy의 create_engine 함수를 통해 생성된 엔진은 이러한 옵션들을 활용하여 내부적으로 연결 풀을 관리하고,


    @task
    def process_file_path(row):
        import json
        import boto3
        from datetime import datetime, timedelta, timezone
        context = get_current_context()
        bucket_name = context["params"].get("bucket_name", "default-bucket")
        
        if bucket_name == "upload_dev":
            delete_days = int(Variable.get("delete_days_upload_dev", 3))
        elif bucket_name == "upload_prod":
            delete_days = int(Variable.get("delete_days_upload_prod", 7))
        else:
            delete_days = int(Variable.get("delete_days_default", 3))
            
        cutoff = datetime.now(timezone.utc) - timedelta(days=delete_days)
        s3 = boto3.client('s3')
        
        file_data = row[0]
        try:
            file_dict = file_data if isinstance(file_data, dict) else json.loads(file_data)
        except Exception as e:
            print(f"Error parsing file_data: {e}")
            file_dict = {}
        
        folder_list = file_dict.get("file_path", [])
        if not isinstance(folder_list, list):
            print("file_path value is not a list; converting to list.")
            folder_list = [folder_list]
        
        for folder in folder_list:
            print(f"Checking S3 folder with prefix: {folder} in bucket: {bucket_name}")
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            if "Contents" in response:
                for obj in response["Contents"]:
                    last_modified = obj.get("LastModified")
                    if last_modified and last_modified < cutoff:
                        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
                        print(f"Deleted s3://{bucket_name}/{obj['Key']} (LastModified: {last_modified})")
                    else:
                        print(f"Skipping s3://{bucket_name}/{obj['Key']} (LastModified: {last_modified})")
            else:
                print(f"No objects found with prefix: {folder} in bucket: {bucket_name}")
        return folder_list

    rows = read_job_history()
    process_file_path.expand(row=rows)

dag_read_dsllm_job_history = read_dsllm_job_history_pipeline()

# --- Test block ---
if __name__ == "__main__":
    result = dag_read_dsllm_job_history.test()
    print("Processed file_path list results:")
    print(result)
