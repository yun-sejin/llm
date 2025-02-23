import os
import boto3
from botocore.exceptions import ClientError

def upload_file_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"Error uploading {file_path}: {e}")

def upload_images_from_folder(images_folder, bucket_name, prefix=""):
    for root, _, files in os.walk(images_folder):
        for file in files:
            if file.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp")):
                full_path = os.path.join(root, file)
                s3_key = os.path.join(prefix, file) if prefix else file
                upload_file_to_s3(full_path, bucket_name, s3_key)

if __name__ == "__main__":
    # Adjust the images folder path if necessary.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    images_folder = os.path.join(base_dir, "images")  
    bucket_name = "your-s3-bucket-name"  # Replace with your bucket name.
    prefix = "uploaded_images"           # Replace with your desired S3 folder.
    upload_images_from_folder(images_folder, bucket_name, prefix)
