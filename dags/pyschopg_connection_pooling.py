import json
import psycopg2
from psycopg2.pool import SimpleConnectionPool  # New import for connection pooling
import boto3
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow.models import Variable  # New import for variable management
from airflow.operators.python import get_current_context

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
#             **최소 연결 수(1)**와 **최대 연결 수(10)**를 지정합니다.
# 데이터베이스 이름은 'llmdp', 사용자명은 'airflow', 암호는 'airflow', 그리고 호스트와 포트는 각각 'localhost'와 '5432'로 설정되어 있습니다.
# 이 풀을 통해 작업이 완료된 후에 연결을 반환하여 여러 쿼리에서 이를 재활용할 수 있습니다.
# 이 코드는 psycopg2의 SimpleConnectionPool 클래스를 사용하여 PostgreSQL 데이터베이스에 대한 연결 풀(최소 1개, 최대 10개)을 생성합니다. 이렇게 생성된 연결 풀을 사용하면 여러 작업이나 쿼리에서 데이터베이스 연결을 재사용할 수 있으므로, 매번 새로운 연결을 생성하는 오버헤드를 줄이고 성능을 개선할 수 있습니다.
            
#             psycopg2의 SimpleConnectionPool 자체에는 고정된 최대 연결 수 제한이 없습니다.
# 즉, 풀을 생성할 때 사용자가 최대 연결 수(maxconn)를 임의의 정수로 설정할 수 있습니다. 다만, 실제로 사용할 수 있는 최대 연결 수는 다음에 의해 결정됩니다:

# PostgreSQL 서버의 max_connections 설정:
# 서버 설정에 따라 동시에 허용되는 연결 수가 제한됩니다. 기본값은 보통 100이지만, 서버 설정에 따라 다릅니다.

# 시스템 자원:
# 메모리나 CPU 등의 시스템 자원에 따라 많은 연결을 유지하는 것이 비효율적일 수 있습니다.

# 따라서 풀의 최대 연결 수는 사용자의 설정 값과 PostgreSQL 서버의 max_connections 및 시스템 자원에 의존합니다.

            # Create a connection pool with 1 to 10 connections.
            pool = SimpleConnectionPool(
                1, 10,
                dbname='llmdp',
                user='airflow',
                password='airflow',
                host='localhost',
                port='5432'
            )
            conn = pool.getconn()
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
            pool.putconn(conn)
            pool.closeall()
            return all_rows
        except Exception as e:
            print(f"Error reading job history: {e}")
            raise

    @task
    def process_file_path(row):
        import json
        import boto3
        from datetime import datetime, timedelta, timezone
        # Retrieve bucket_name from the task context using DAG parameters.
        context = get_current_context()
        bucket_name = context["params"].get("bucket_name", "default-bucket")
        
        # Determine delete_days via Variables based on bucket_name.
        if bucket_name == "upload_dev":
            delete_days = int(Variable.get("delete_days_upload_dev", 3))
        elif bucket_name == "upload_prod":
            delete_days = int(Variable.get("delete_days_upload_prod", 7))
        else:
            delete_days = int(Variable.get("delete_days_default", 3))
            
        cutoff = datetime.now(timezone.utc) - timedelta(days=delete_days)
        s3 = boto3.client('s3')
        
        # Parse file_path_lists column value (assumed to be JSON string or dict)
        file_data = row[0]
        try:
            file_dict = file_data if isinstance(file_data, dict) else json.loads(file_data)
        except Exception as e:
            print(f"Error parsing file_data: {e}")
            file_dict = {}
        
        # Extract folder list from key 'file_path'
        folder_list = file_dict.get("file_path", [])
        if not isinstance(folder_list, list):
            print("file_path value is not a list; converting to list.")
            folder_list = [folder_list]
        
        # For each folder, delete objects older than the cutoff timestamp.
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
