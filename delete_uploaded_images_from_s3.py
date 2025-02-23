import argparse
import boto3
from botocore.exceptions import ClientError

def delete_images_from_s3(bucket_name, prefix=""):
    s3 = boto3.client('s3')
    try:
        # List objects under the prefix
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get('Contents', [])
        if not objects:
            print("No objects found to delete.")
            return
        # Delete each object
        for obj in objects:
            key = obj['Key']
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted s3://{bucket_name}/{key}")
    except ClientError as e:
        print(f"Error deleting objects: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete uploaded images from S3.")
    parser.add_argument("--del_yn", type=str, default="N", help="Enter Y to delete uploaded images.")
    args = parser.parse_args()
    
    if args.del_yn.strip().upper() == "Y":
        bucket_name = "your-s3-bucket-name"      # Replace with your bucket name.
        prefix = "uploaded_images"               # Replace with your desired S3 folder prefix.
        delete_images_from_s3(bucket_name, prefix)
    else:
        print("Deletion canceled.")
