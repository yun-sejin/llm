import psycopg2
import requests
from airflow.decorators import dag, task
from datetime import datetime

default_args = {
    "owner": "airflow",
}

@dag(default_args=default_args, schedule=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["read"])
def read_dsllm_parsed_filtered_pipeline():
    
    @task
    def read_filtered_data():
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
                    SELECT i.img_id
                    FROM dsllm_parsed p
                    LEFT OUTER JOIN dsllm_parsed_img i ON p.src_id = i.src_id
                    WHERE p.del_yn = true
                """
                cur.execute(query)
                rows = cur.fetchall()
                print(f"Total filtered img_id rows: {len(rows)}")
            conn.close()
            return rows
        except Exception as e:
            print(f"Error reading filtered data: {e}")
            raise

    @task
    def delete_single_image(row):
        img_id = row[0]
        api_url = f"http://example.com/api/delete?img_id={img_id}"  # Replace with your API endpoint.
        try:
            response = requests.delete(api_url)
            if response.status_code == 200:
                print(f"Successfully deleted image with id: {img_id}")
            else:
                print(f"Failed to delete image with id: {img_id}. Status code: {response.status_code}")
        except Exception as ex:
            print(f"Error calling delete API for img_id {img_id}: {ex}")

    rows = read_filtered_data()
    delete_single_image.expand(row=rows)

dag_read_dsllm_parsed_filtered = read_dsllm_parsed_filtered_pipeline()

# --- Test block ---
if __name__ == "__main__":
    # For testing outside Airflow, call the pipeline's test method.
    result = dag_read_dsllm_parsed_filtered.test()
    print("Returned img_id rows:")
    print(result)
