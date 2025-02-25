import psycopg2
from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    "owner": "airflow",
}

@dag(default_args=default_args, schedule= None, start_date=datetime(2023, 1, 1), catchup=False, tags=["read"])
def read_dsllm_parsed_pipeline():
    
    @task
    def read_parsed_data():
        try:
            conn = psycopg2.connect(
                dbname='llmdp',
                user='airflow',
                password='airflow',
                host='localhost',
                port='5432'
            )
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM dsllm_parsed")
                rows = cur.fetchall()
                print(f"Total rows read from dsllm_parsed: {len(rows)}")
            conn.close()
        except Exception as e:
            print(f"Error reading dsllm_parsed: {e}")
            raise

    read_parsed_data()

dag_read_dsllm_parsed = read_dsllm_parsed_pipeline()


# --- Test block ---
if __name__ == "__main__":
    # For testing outside Airflow, call the tasks directly
    print("Testing S3 upload and delete tasks:")
    dag_read_dsllm_parsed.test()

