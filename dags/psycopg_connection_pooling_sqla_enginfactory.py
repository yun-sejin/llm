import json
from sqlalchemy import text
import boto3
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.operators.python import get_current_context

# Import the common EngineFactory class.
from dags.engine_factory import EngineFactory

# Build the PostgreSQL connection URL.
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
            # Use EngineFactory to create an engine with pooling options.
            engine_factory = EngineFactory(
                db_url=DATABASE_URL,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                pool_pre_ping=True
            )
            engine = engine_factory.get_engine()
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
