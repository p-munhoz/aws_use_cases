import boto3
import os

# Create a boto3 client for S3, pointing to LocalStack
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

def create_bucket(bucket_name):
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created successfully")

def upload_file(bucket_name, file_name, object_name=None):
    if object_name is None:
        object_name = file_name
    
    s3.upload_file(file_name, bucket_name, object_name)
    print(f"File '{file_name}' uploaded to '{bucket_name}' as '{object_name}'")

def list_buckets():
    response = s3.list_buckets()
    print("Existing buckets:")
    for bucket in response['Buckets']:
        print(f"- {bucket['Name']}")

def list_objects(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    print(f"Objects in bucket '{bucket_name}':")
    for obj in response.get('Contents', []):
        print(f"- {obj['Key']}")

def download_file(bucket_name, object_name, file_name):
    s3.download_file(bucket_name, object_name, file_name)
    print(f"File '{object_name}' downloaded from '{bucket_name}' as '{file_name}'")

def delete_bucket(bucket_name):
    # First, delete all objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    
    # Then delete the bucket
    s3.delete_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' and all its contents deleted successfully")

# Usage example
if __name__ == "__main__":
    bucket_name = "my-test-bucket"
    file_name = "test.txt"
    
    # Create a test file
    with open(file_name, "w") as f:
        f.write("This is a test file for S3 operations with LocalStack")

    create_bucket(bucket_name)
    list_buckets()
    upload_file(bucket_name, file_name)
    list_objects(bucket_name)
    download_file(bucket_name, file_name, "downloaded_" + file_name)
    delete_bucket(bucket_name)

    # Clean up the test files
    os.remove(file_name)
    os.remove("downloaded_" + file_name)