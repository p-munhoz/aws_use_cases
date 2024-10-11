import boto3
import json
import time
import uuid
from botocore.exceptions import ClientError

# LocalStack endpoint URL
LOCALSTACK_ENDPOINT = 'http://localhost:4566'

# Initialize boto3 clients
s3 = boto3.client('s3', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
sqs = boto3.client('sqs', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
dynamodb = boto3.client('dynamodb', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
sns = boto3.client('sns', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

BUCKET_NAME = 'advanced-test-bucket'
QUEUE_NAME = 'file-processing-queue'
TABLE_NAME = 'file-metadata'
TOPIC_NAME = 'file-processed-topic'

def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully")
    except ClientError as e:
        print(f"Error creating bucket: {e}")

def create_queue(queue_name):
    try:
        response = sqs.create_queue(QueueName=queue_name)
        print(f"Queue '{queue_name}' created successfully")
        return response['QueueUrl']
    except ClientError as e:
        print(f"Error creating queue: {e}")

def create_dynamodb_table(table_name):
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'file_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'file_id', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"DynamoDB table '{table_name}' created successfully")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"DynamoDB table '{table_name}' already exists")
        else:
            print(f"Error creating DynamoDB table: {e}")

def create_sns_topic(topic_name):
    try:
        response = sns.create_topic(Name=topic_name)
        print(f"SNS topic '{topic_name}' created successfully")
        return response['TopicArn']
    except ClientError as e:
        print(f"Error creating SNS topic: {e}")

def upload_file(bucket_name, file_name, object_name):
    try:
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"File '{file_name}' uploaded to '{bucket_name}' as '{object_name}'")
        return True
    except ClientError as e:
        print(f"Error uploading file: {e}")
        return False

def send_message(queue_url, message_body):
    try:
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
        print(f"Message sent. MessageId: {response['MessageId']}")
    except ClientError as e:
        print(f"Error sending message: {e}")

def store_file_metadata(file_id, metadata):
    try:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'file_id': {'S': file_id},
                'status': {'S': 'uploaded'},
                'metadata': {'S': json.dumps(metadata)}
            }
        )
        print(f"Metadata stored for file_id: {file_id}")
    except ClientError as e:
        print(f"Error storing metadata: {e}")

def process_file(file_id, bucket, object_name):
    # Simulate file processing
    print(f"Processing file: {object_name}")
    time.sleep(2)  # Simulate some processing time
    
    # Update file status in DynamoDB
    try:
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={'file_id': {'S': file_id}},
            UpdateExpression="SET #status = :status",
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': {'S': 'processed'}}
        )
        print(f"File status updated for file_id: {file_id}")
    except ClientError as e:
        print(f"Error updating file status: {e}")

def notify_completion(topic_arn, message):
    try:
        sns.publish(TopicArn=topic_arn, Message=json.dumps(message))
        print(f"Notification sent to SNS topic")
    except ClientError as e:
        print(f"Error sending notification: {e}")

def receive_and_process_messages(queue_url, topic_arn):
    print("Starting to receive and process messages...")
    try:
        while True:
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=5)
            
            if 'Messages' in response:
                for message in response['Messages']:
                    print(f"Received message: {message['Body']}")
                    message_body = json.loads(message['Body'])
                    
                    # Process the file
                    process_file(message_body['file_id'], message_body['bucket'], message_body['object'])
                    
                    # Notify about completion
                    notify_completion(topic_arn, {
                        'file_id': message_body['file_id'],
                        'status': 'processed',
                        'timestamp': time.time()
                    })
                    
                    # Delete the message from the queue
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                    print("Message processed and deleted from the queue")
            else:
                print("No messages in the queue. Waiting...")
            
            time.sleep(1)  # Wait for 1 second before polling again
    except KeyboardInterrupt:
        print("Message processing stopped.")

def main():
    # Create resources
    create_bucket(BUCKET_NAME)
    queue_url = create_queue(QUEUE_NAME)
    create_dynamodb_table(TABLE_NAME)
    topic_arn = create_sns_topic(TOPIC_NAME)

    # Upload a file to S3
    file_name = 'test_file.txt'
    object_name = f'uploaded_{uuid.uuid4()}.txt'
    
    # Create a test file
    with open(file_name, 'w') as f:
        f.write("This is a test file for advanced AWS services integration")

    if upload_file(BUCKET_NAME, file_name, object_name):
        file_id = str(uuid.uuid4())
        metadata = {
            'original_name': file_name,
            'size': 54,  # Size of our test file content
            'upload_time': time.time()
        }
        store_file_metadata(file_id, metadata)
        
        # Send a message to SQS about the upload
        message = {
            'file_id': file_id,
            'bucket': BUCKET_NAME,
            'object': object_name,
            'timestamp': time.time()
        }
        send_message(queue_url, message)

    # Start receiving and processing messages
    receive_and_process_messages(queue_url, topic_arn)

if __name__ == "__main__":
    main()