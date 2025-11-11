import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch_date
from s3_to_stg.customers import load_customers
from s3_to_stg.products import load_products
from s3_to_stg.payments import load_payments    
from s3_to_stg.offices import load_offices
from s3_to_stg.employees import load_employees
from s3_to_stg.orderDetails import load_orderdetails
from s3_to_stg.productlines import load_productlines
from s3_to_stg.orders import load_orders

table_exports = [
    load_customers,
    load_payments,
    load_products,
    load_offices,
    load_employees,
    load_orderdetails,
    load_productlines,
    load_orders
]

def run_s3_to_stg(latest_date=None):
    # if cur is None or conn is None:
    #     conn, cur = get_redshift_connection()
        
    if latest_date is None:
        latest_date = get_latest_etl_batch_date()
    
    # bucket = os.getenv("BUCKET")

    # if not bucket:
    #     raise ValueError("BUCKET not set in .env")
    
    for load_func in table_exports:
        print(f"Loading data for function: {load_func}")
        load_func(latest_date)
        print(f"Completed loading data for function: {load_func}")
    return

if __name__ == "__main__":
    run_s3_to_stg()