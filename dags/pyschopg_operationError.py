import json
import psycopg2
# import boto3
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.operators.python import get_current_context
import time

default_args = {
    "owner": "airflow",
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["read"],
    params={"bucket_name": "your-s3-bucket-name"}
)
def read_dsllm_job_history_pipeline():
    
    @task
    def read_job_history():
        max_retries = 3
        retry_delay = 5  # seconds
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(
                    dbname='llmdp',
                    user='airflow',
                    password='airflow',
                    host='localhost',
                    port='5432'
                )
                with conn.cursor() as cur:
                    query = """
                        SELECT h1.file_path_lists
                        FROM dsllm_job_hist h1
                        JOIN dsllm_job_hist h2 ON h1.after_job_id = h2.job_id
                        WHERE h1.job_type = 'fastapi'
                          AND h1.success_yn = TRUE
                          AND h1.after_job_id IS NOT NULL
                          AND h2.job_type = 'airflow'
                          AND h2.success_yn = TRUE
                          AND h2.err_msg IS NULL
                    """
                    cur.execute(query)
                    all_rows = []
                    while True:
                        chunk = cur.fetchmany(100)
                        if not chunk:
                            break
                        all_rows.extend(chunk)
                    print(f"Total file_path_list rows: {len(all_rows)}")
                conn.close()
                return all_rows
            except psycopg2.OperationalError as e:
                print(f"OperationalError: {e}")
                if attempt < max_retries - 1:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise
            except Exception as e:
                print(f"Error reading job history: {e}")
                raise

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

if __name__ == "__main__":
    result = dag_read_dsllm_job_history.test()
    print("Processed file_path list results:")
    print(result)
    
    
    
    
    
# ;     오류는 PostgreSQL 서버가 클라이언트와의 연결을 예기치 않게 종료했을 때 발생합니다. 이 오류의 원인은 여러 가지가 있을 수 있으며, 이를 해결하기 위한 몇 가지 방법을 제안합니다.

# ; 원인
# ; 서버 타임아웃: 서버가 일정 시간 동안 유휴 상태인 연결을 닫을 수 있습니다.
# ; 서버 재시작: 서버가 재시작되거나 충돌하여 연결이 끊어질 수 있습니다.
# ; 네트워크 문제: 네트워크 문제로 인해 연결이 끊어질 수 있습니다.
# ; 리소스 부족: 서버의 리소스가 부족하여 연결을 유지할 수 없을 수 있습니다.
# ; 해결 방법
# ; 서버 설정 확인: PostgreSQL 서버의 타임아웃 설정을 확인하고, 필요에 따라 조정합니다.
# ; 재시도 로직 추가: 연결이 끊어졌을 때 재시도하는 로직을 추가합니다.
# ; 연결 유지: 주기적으로 쿼리를 실행하여 연결이 유휴 상태가 되지 않도록 합니다.
# ; 네트워크 상태 확인: 네트워크 상태를 확인하고, 안정적인 연결을 유지합니다.
# ; 코드 수정 예제
# ; 다음은 재시도 로직을 추가하여 연결이 끊어졌을 때 재시도하는 예제입니다:



# When encountering a psycopg2.OperationalError "server closed the connection unexpectedly" error, consider the following steps:

# • Check your PostgreSQL server logs to see if the server crashed or if there was a timeout. • Ensure your network is stable and that there are no interruptions (e.g., firewalls or load balancers closing idle connections). • Use connection pooling or a library like SQLAlchemy, and configure your pool to recycle connections before they expire. • Enable TCP keepalive settings (e.g., using the "tcp_keepalive_idle", "tcp_keepalive_interval", and "tcp_keepalive_count" connection parameters) so that idle connections remain active. • Consider catching this error in your code and re-establishing the connection as needed.