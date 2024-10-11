import boto3
import json
import time
import uuid
from botocore.exceptions import ClientError
from PIL import Image
import io

# LocalStack endpoint URL
LOCALSTACK_ENDPOINT = 'http://localhost:4566'

# Initialize boto3 clients
s3 = boto3.client('s3', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
sqs = boto3.client('sqs', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
dynamodb = boto3.client('dynamodb', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')
sns = boto3.client('sns', endpoint_url=LOCALSTACK_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

BUCKET_NAME = 'advanced-test-bucket'
QUEUE_NAME = 'file-processing-queue'
DLQ_NAME = 'dead-letter-queue'
TABLE_NAME = 'file-metadata'
TOPIC_NAME = 'file-processed-topic'

def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully")
    except ClientError as e:
        print(f"Error creating bucket: {e}")

def create_queue(queue_name, is_dlq=False):
    try:
        if is_dlq:
            response = sqs.create_queue(QueueName=queue_name)
        else:
            dlq_url = create_queue(DLQ_NAME, is_dlq=True)
            dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
            redrive_policy = {
                'deadLetterTargetArn': dlq_arn,
                'maxReceiveCount': '3'
            }
            response = sqs.create_queue(
                QueueName=queue_name,
                Attributes={
                    'RedrivePolicy': json.dumps(redrive_policy)
                }
            )
        print(f"Queue '{queue_name}' created successfully")
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'QueueAlreadyExists':
            print(f"Queue '{queue_name}' already exists. Fetching URL.")
            return sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        else:
            print(f"Error creating queue: {e}")
            return None

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

def process_image(bucket, object_name):
    try:
        # Download the image from S3
        response = s3.get_object(Bucket=bucket, Key=object_name)
        image_data = response['Body'].read()
        
        # Open the image using Pillow
        image = Image.open(io.BytesIO(image_data))
        
        # Resize the image
        resized_image = image.resize((100, 100))
        
        # Save the resized image to a buffer
        buffer = io.BytesIO()
        resized_image.save(buffer, format="JPEG")
        buffer.seek(0)
        
        # Upload the resized image back to S3
        resized_object_name = f"resized_{object_name}"
        s3.put_object(Bucket=bucket, Key=resized_object_name, Body=buffer)
        
        print(f"Image processed and resized version uploaded as {resized_object_name}")
        return resized_object_name
    except Exception as e:
        print(f"Error processing image: {e}")
        raise

def update_file_status(file_id, status, additional_info=None):
    try:
        update_expression = "SET #status = :status"
        expression_attribute_values = {':status': {'S': status}}
        expression_attribute_names = {'#status': 'status'}

        if additional_info:
            update_expression += ", additional_info = :info"
            expression_attribute_values[':info'] = {'S': json.dumps(additional_info)}

        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={'file_id': {'S': file_id}},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names
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
                    
                    try:
                        # Process the image
                        resized_object = process_image(message_body['bucket'], message_body['object'])
                        
                        # Update file status in DynamoDB
                        update_file_status(message_body['file_id'], 'processed', {'resized_object': resized_object})
                        
                        # Notify about completion
                        notify_completion(topic_arn, {
                            'file_id': message_body['file_id'],
                            'status': 'processed',
                            'resized_object': resized_object,
                            'timestamp': time.time()
                        })
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        update_file_status(message_body['file_id'], 'error', {'error_message': str(e)})
                    finally:
                        # Delete the message from the queue
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
                        print("Message processed and deleted from the queue")
            else:
                print("No messages in the queue. Waiting...")
            
            time.sleep(1)  # Wait for 1 second before polling again
    except KeyboardInterrupt:
        print("Message processing stopped.")

def query_dynamodb():
    try:
        response = dynamodb.scan(TableName=TABLE_NAME)
        items = response.get('Items', [])
        print("\nCurrent DynamoDB contents:")
        for item in items:
            print(f"File ID: {item['file_id']['S']}")
            print(f"Status: {item['status']['S']}")
            print(f"Metadata: {item['metadata']['S']}")
            if 'additional_info' in item:
                print(f"Additional Info: {item['additional_info']['S']}")
            print("---")
    except ClientError as e:
        print(f"Error querying DynamoDB: {e}")

def main():
    # Create resources
    create_bucket(BUCKET_NAME)
    queue_url = create_queue(QUEUE_NAME)
    if queue_url is None:
        print("Failed to create or get queue URL. Exiting.")
        return
    create_dynamodb_table(TABLE_NAME)
    topic_arn = create_sns_topic(TOPIC_NAME)

    # Upload a file to S3
    file_name = 'test_image.jpg'  # Make sure you have this image in your directory
    object_name = f'uploaded_{uuid.uuid4()}.jpg'
    
    if upload_file(BUCKET_NAME, file_name, object_name):
        file_id = str(uuid.uuid4())
        metadata = {
            'original_name': file_name,
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

    # Query DynamoDB to see the results
    query_dynamodb()
    
    # Start receiving and processing messages
    receive_and_process_messages(queue_url, topic_arn)

if __name__ == "__main__":
    main()