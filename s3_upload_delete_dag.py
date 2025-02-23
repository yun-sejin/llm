import os
import boto3
from botocore.exceptions import ClientError
from airflow.decorators import dag, task
from datetime import datetime

BUCKET_NAME = "your-s3-bucket-name"      # Replace with your bucket name.
PREFIX = "uploaded_images"               # Replace with your desired S3 folder.
IMAGES_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "images")

default_args = {
    "owner": "airflow",
}

@dag(default_args=default_args, schedule_interval="@once", start_date=datetime(2023, 1, 1), catchup=False, tags=["s3"], params={"del_yn": "N"})
def s3_upload_delete_pipeline():
    
    @task
    def upload_images():
        s3 = boto3.client('s3')
        for root, _, files in os.walk(IMAGES_FOLDER):
            for file in files:
                if file.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp")):
                    full_path = os.path.join(root, file)
                    s3_key = os.path.join(PREFIX, file)
                    try:
                        s3.upload_file(full_path, BUCKET_NAME, s3_key)
                        print(f"Uploaded {full_path} to s3://{BUCKET_NAME}/{s3_key}")
                    except ClientError as e:
                        print(f"Error uploading {full_path}: {e}")
    
    @task
    def delete_images(del_yn: str):
        if del_yn.upper() != "Y":
            print("Deletion not requested.")
            return
        s3 = boto3.client('s3')
        try:
            response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
            objects = response.get("Contents", [])
            if not objects:
                print("No objects found to delete.")
                return
            for obj in objects:
                key = obj["Key"]
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                print(f"Deleted s3://{BUCKET_NAME}/{key}")
        except ClientError as e:
            print(f"Error deleting objects: {e}")
    
    up = upload_images()
    # Pass the DAG parameter "del_yn" to the delete_images task.
    dl = delete_images(del_yn="{{ params.del_yn }}")
    up >> dl

s3_upload_dag = s3_upload_delete_pipeline()
