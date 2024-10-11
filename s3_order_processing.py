import boto3
from datetime import datetime, timedelta
import random
import io
import csv

# Initialize S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET_NAME = 'order-processing-bucket'

def ensure_bucket_exists():
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")

def generate_order():
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
    orders = [generate_order() for _ in range(random.randint(50, 200))]
    
    # Generate daily report
    report = io.StringIO()
    writer = csv.writer(report)
    writer.writerow(['Order ID', 'Customer ID', 'Product', 'Quantity', 'Price', 'Timestamp'])
    
    total_revenue = 0
    for order in orders:
        writer.writerow([order['order_id'], order['customer_id'], order['product'], 
                         order['quantity'], order['price'], order['timestamp']])
        total_revenue += order['quantity'] * order['price']
    
    # Upload daily report
    report_key = f'daily_reports/{date.strftime("%Y-%m-%d")}_report.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=report_key, Body=report.getvalue())
    print(f"Daily report uploaded: {report_key}")
    
    return total_revenue

def generate_monthly_report(year, month):
    start_date = datetime(year, month, 1)
    end_date = start_date + timedelta(days=32)
    end_date = end_date.replace(day=1) - timedelta(days=1)
    
    monthly_data = []
    current_date = start_date
    while current_date <= end_date:
        daily_report_key = f'daily_reports/{current_date.strftime("%Y-%m-%d")}_report.csv'
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=daily_report_key)
            daily_report = response['Body'].read().decode('utf-8')
            csv_reader = csv.reader(io.StringIO(daily_report))
            next(csv_reader)  # Skip header
            daily_total = sum(float(row[4]) * int(row[3]) for row in csv_reader)
            monthly_data.append((current_date.strftime("%Y-%m-%d"), daily_total))
        except s3.exceptions.NoSuchKey:
            print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        current_date += timedelta(days=1)
    
    # Generate and upload monthly report
    monthly_report = io.StringIO()
    writer = csv.writer(monthly_report)
    writer.writerow(['Date', 'Daily Revenue'])
    writer.writerows(monthly_data)
    
    monthly_report_key = f'monthly_reports/{year}-{month:02d}_report.csv'
    s3.put_object(Bucket=BUCKET_NAME, Key=monthly_report_key, Body=monthly_report.getvalue())
    print(f"Monthly report uploaded: {monthly_report_key}")

def retrieve_report(report_key):
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=report_key)
        return response['Body'].read().decode('utf-8')
    except s3.exceptions.NoSuchKey:
        return f"Report not found: {report_key}"

def main():
    ensure_bucket_exists()
    
    # Simulate processing orders for the past week
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    current_date = start_date
    while current_date <= end_date:
        process_daily_orders(current_date)
        current_date += timedelta(days=1)
    
    # Generate monthly report for the current month
    today = datetime.now()
    generate_monthly_report(today.year, today.month)
    
    # Retrieve and print a specific daily report
    sample_date = end_date - timedelta(days=3)
    sample_report_key = f'daily_reports/{sample_date.strftime("%Y-%m-%d")}_report.csv'
    print(f"\nSample Daily Report ({sample_date.strftime('%Y-%m-%d')}):")
    print(retrieve_report(sample_report_key))
    
    # Retrieve and print the monthly report
    monthly_report_key = f'monthly_reports/{today.year}-{today.month:02d}_report.csv'
    print(f"\nMonthly Report ({today.year}-{today.month:02d}):")
    print(retrieve_report(monthly_report_key))

if __name__ == "__main__":
    main()