from db_utils import get_connection
from dotenv import load_dotenv
import boto3
import os

from payments import export_payments
from offices import export_offices
from customers import export_customers
from employees import export_employees
from productlines import export_productlines
from products import export_products
from orders import export_orders
from orderdetails import export_orderdetails

load_dotenv()

bucket = os.getenv("BUCKET")

table_exports = [
    ("Payments", export_payments),
    ("Offices", export_offices),
    ("Customers", export_customers),
    ("Employees", export_employees),
    ("ProductLines", export_productlines),
    ("Products", export_products),
    ("Orders", export_orders),
    ("OrderDetails", export_orderdetails),
]

def upload_to_s3(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)

def run_etl():
    update_timestamps_str = os.getenv("UPDATE_TIMESTAMPS")
    if not update_timestamps_str:
        raise ValueError("UPDATE_TIMESTAMPS not set in .env")
    
    update_timestamps = [d.strip() for d in update_timestamps_str.split(",") if d.strip()]
    
    for update_timestamp in update_timestamps:
        conn, cursor = get_connection(update_timestamp)
        for table, export_func in table_exports:
            filename = export_func(cursor, update_timestamp)
            s3_key = f"{table}/{update_timestamp}/{filename}"
            upload_to_s3(filename, bucket, s3_key)
            os.remove(filename)
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_etl()