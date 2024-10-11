import boto3
import json
import time
from botocore.exceptions import ClientError

# LocalStack endpoint URL
LOCALSTACK_ENDPOINT = 'http://localhost:4566'

# Initialize boto3 clients for S3 and SQS
s3 = boto3.client('s3', endpoint_url=LOCALSTACK_ENDPOINT, 
                  aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
sqs = boto3.client('sqs', endpoint_url=LOCALSTACK_ENDPOINT, 
                   aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

BUCKET_NAME = 'my-test-bucket'
QUEUE_NAME = 'my-test-queue'

def create_bucket(bucket_name):
    """
    Create a new S3 bucket.

    Parameters:
        bucket_name (str): The name of the bucket to be created.

    Raises:
        ClientError: If there is an error creating the bucket.
    """
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully")
    except ClientError as e:
        print(f"Error creating bucket: {e}")

def create_queue(queue_name):
    """
    Create a new SQS queue.

    Parameters:
        queue_name (str): The name of the queue to be created.

    Returns:
        str: The URL of the newly created queue.

    Raises:
        ClientError: If there is an error creating the queue.
    """
    try:
        response = sqs.create_queue(QueueName=queue_name)
        print(f"Queue '{queue_name}' created successfully")
        return response['QueueUrl']
    except ClientError as e:
        print(f"Error creating queue: {e}")

def upload_file(bucket_name, file_name, object_name):
    """
    Uploads a file to the specified S3 bucket.

    Parameters:
        bucket_name (str): The name of the S3 bucket to upload the file to.
        file_name (str): The local path to the file to be uploaded.
        object_name (str): The name of the object in S3 to be created.

    Returns:
        bool: True if the file was uploaded successfully, False if there was an error.

    Raises:
        ClientError: If there is an error uploading the file.
    """
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"File '{file_name}' uploaded to '{bucket_name}' as '{object_name}'")
        return True
    except ClientError as e:
        print(f"Error uploading file: {e}")
        return False

def send_message(queue_url, message_body):
    """
    Sends a message to the specified SQS queue.

    Parameters:
        queue_url (str): The URL of the SQS queue to send the message to.
        message_body (str): The body of the message to be sent.

    Returns:
        None

    Raises:
        ClientError: If there is an error sending the message.
    """
    try:
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
        print(f"Message sent. MessageId: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending message: {e}")

def receive_messages(queue_url):
    """
    Continuously receive messages from the specified SQS queue and print them to the console.
    
    This function will run indefinitely until it is manually stopped with a KeyboardInterrupt (e.g. Ctrl+C).
    It will poll the queue every 1 second for new messages, and if there are any messages, it will delete them
    from the queue after processing them.
    
    Parameters:
        queue_url (str): The URL of the SQS queue to receive messages from.
    
    Returns:
        None
    """
    try:
        while True:
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
            
            if 'Messages' in response:
                message = response['Messages'][0]
                print(f"Received message: {message['Body']}")
                
                # Delete the message from the queue
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                print("Message processed and deleted from the queue")
            else:
                print("No messages in the queue. Waiting...")
            
            time.sleep(1)  # Wait for 1 second before polling again
    except KeyboardInterrupt:
        print("Message receiving stopped.")

def main():
    # Create S3 bucket
    """
    Main entry point for the application.

    This function will create an S3 bucket and an SQS queue, upload a test file to the bucket,
    send a message to the queue about the upload, and then start receiving messages from the queue.
    """
    create_bucket(BUCKET_NAME)

    # Create SQS queue
    queue_url = create_queue(QUEUE_NAME)

    # Upload a file to S3
    file_name = 'test_file.txt'
    object_name = 'uploaded_test_file.txt'
    
    # Create a test file
    with open(file_name, 'w') as f:
        f.write("This is a test file for S3 upload")

    if upload_file(BUCKET_NAME, file_name, object_name):
        # Send a message to SQS about the upload
        message = json.dumps({
            'bucket': BUCKET_NAME,
            'object': object_name,
            'timestamp': time.time()
        })
        send_message(queue_url, message)

    # Start receiving messages
    receive_messages(queue_url)

if __name__ == "__main__":
    main()