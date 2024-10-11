import boto3
from datetime import datetime, timedelta
import random
import io
import csv
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
import uvicorn
from typing import List

# Initialize S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET_NAME = 'order-processing-bucket'
COLD_STORAGE_BUCKET = 'order-archive-bucket'

# Initialize FastAPI app
app = FastAPI()

def ensure_buckets_exist():
    """
    Ensure that the necessary buckets exist in S3.

    This function will check if the given buckets exist in S3, and if not, it will create them.
    The buckets to be checked are defined in the BUCKET_NAME and COLD_STORAGE_BUCKET variables.
    """
    for bucket in [BUCKET_NAME, COLD_STORAGE_BUCKET]:
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' created.")

def generate_order():
    """
    Generate a random order.

    This function will generate a dictionary with the following structure:

    {
        'order_id': str,
        'customer_id': str,
        'product': str,
        'quantity': int,
        'price': float,
        'timestamp': str
    }

    The values for the dictionary are randomly generated.
    """
    products = ['Widget A', 'Widget B', 'Widget C', 'Widget D']
    return {
        'order_id': f'ORD-{random.randint(1000, 9999)}',
        'customer_id': f'CUST-{random.randint(100, 999)}',
        'product': random.choice(products),
        'quantity': random.randint(1, 10),
        'price': round(random.uniform(10, 100), 2),
        'timestamp': datetime.now().isoformat()
    }

def process_daily_orders(date):
    """
    Process a day's worth of orders and upload partitioned daily reports.

    This function takes a date as an argument and will generate a number of orders for that date.
    It will then partition the orders by product and generate a report for each product.
    The reports will be uploaded to S3 in the form of partitioned CSV files.
    """
    orders = [generate_order() for _ in range(random.randint(50, 200))]
    
    # Implement data partitioning
    partitions = {}
    for order in orders:
        product = order['product']
        if product not in partitions:
            partitions[product] = []
        partitions[product].append(order)
    
    # Generate daily report with partitions
    for product, product_orders in partitions.items():
        report = io.StringIO()
        writer = csv.writer(report)
        writer.writerow(['Order ID', 'Customer ID', 'Product', 'Quantity', 'Price', 'Timestamp'])
        
        for order in product_orders:
            writer.writerow([order['order_id'], order['customer_id'], order['product'], 
                             order['quantity'], order['price'], order['timestamp']])
        
        # Upload partitioned daily report
        report_key = f'daily_reports/{date.strftime("%Y-%m-%d")}/{product}_report.csv'
        upload_with_retry(BUCKET_NAME, report_key, report.getvalue())
        print(f"Daily report uploaded: {report_key}")

def upload_with_retry(bucket, key, body, max_retries=3):
    """
    Uploads an object to S3 with a specified number of retries.

    If the upload fails, this function will retry up to max_retries times.
    If all retries fail, it will raise a ClientError.

    Parameters:
        bucket (str): The name of the S3 bucket to upload to.
        key (str): The S3 key for the object to be uploaded.
        body (str or bytes): The object to be uploaded.
        max_retries (int): The maximum number of times to retry the upload if it fails.
            Defaults to 3.
    """
    for attempt in range(max_retries):
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            return
        except ClientError as e:
            if attempt == max_retries - 1:
                raise
            print(f"Upload failed. Retrying... (Attempt {attempt + 1}/{max_retries})")

def move_to_cold_storage(days_old=30):
    """
    Move objects older than the given number of days from the main bucket to cold storage.

    This function will list all objects in the main bucket with the prefix 'daily_reports/'
    and check if their dates are older than the given threshold. If so, it will copy the
    object to the cold storage bucket and then delete it from the main bucket.

    Parameters:
        days_old (int): The number of days old a file must be to be moved to cold storage.
            Defaults to 30.
    """
    threshold_date = datetime.now() - timedelta(days=days_old)
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix='daily_reports/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Extract date from key
            date_str = key.split('/')[1]
            try:
                file_date = datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                print(f"Skipping file with invalid date format: {key}")
                continue
            
            if file_date < threshold_date:
                # Move to cold storage
                copy_source = {'Bucket': BUCKET_NAME, 'Key': key}
                s3.copy(copy_source, COLD_STORAGE_BUCKET, key)
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                print(f"Moved {key} to cold storage")

@app.get("/reports/")
async def get_reports(start_date: str, end_date: str, product: str = None):
    """
    Returns a list of daily reports for the given date range.

    The list will contain dictionaries with the following keys:

        - date: The date of the report in YYYY-MM-DD format.
        - product: The product name for the report.
        - content: The content of the report as a string.

    If a product is specified, the list will contain a single dictionary with the report
    for that product. If no product is specified, the list will contain a dictionary for
    each product report found in the given date range.

    Parameters:
        start_date (str): The start date of the range in YYYY-MM-DD format.
        end_date (str): The end date of the range in YYYY-MM-DD format.
        product (str): The product name to filter the reports by. Defaults to None.

    Returns:
        List[Dict[str, Union[str, datetime.date]]]

    Raises:
        HTTPException: If the date format is invalid. Use YYYY-MM-DD.
    """
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    reports = []
    current_date = start
    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        if product:
            report_key = f'daily_reports/{date_str}/{product}_report.csv'
        else:
            report_key = f'daily_reports/{date_str}/'
        
        try:
            if product:
                response = s3.get_object(Bucket=BUCKET_NAME, Key=report_key)
                reports.append({
                    "date": date_str,
                    "product": product,
                    "content": response['Body'].read().decode('utf-8')
                })
            else:
                # List all product reports for the day
                response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=report_key)
                for obj in response.get('Contents', []):
                    product_key = obj['Key']
                    product_name = product_key.split('/')[-1].replace('_report.csv', '')
                    product_response = s3.get_object(Bucket=BUCKET_NAME, Key=product_key)
                    reports.append({
                        "date": date_str,
                        "product": product_name,
                        "content": product_response['Body'].read().decode('utf-8')
                    })
        except ClientError:
            # Report not found, continue to next date
            pass
        
        current_date += timedelta(days=1)
    
    return reports

def main():
    """
    Main entry point for the application.

    This function will simulate processing orders for the past 45 days, move old reports to cold storage,
    and then start the API server.
    """
    ensure_buckets_exist()
    
    # Simulate processing orders for the past week
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=45)
    current_date = start_date
    while current_date <= end_date:
        process_daily_orders(current_date)
        current_date += timedelta(days=1)
    
    # Move old reports to cold storage
    move_to_cold_storage()

    print("Order processing completed. Starting API server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()