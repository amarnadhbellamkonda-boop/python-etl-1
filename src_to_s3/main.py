from .customers import export_customers
from .payments import export_payments
from .offices import export_offices
from .employees import export_employees
from .productlines import export_productlines
from .products import export_products
from .orders import export_orders
from .orderdetails import export_orderdetails
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import get_latest_etl_batch_date

table_exports = [
    ("Customers", export_customers),
    ("Products", export_products),
    ("Payments", export_payments),
    ("Offices", export_offices),
    ("Employees", export_employees),
    ("ProductLines", export_productlines),
    ("Orders", export_orders),
    ("OrderDetails", export_orderdetails),
]

def run_src_to_s3(latest_date=None):
    try:
        print("ETL process started from src to s3.")
        
        if latest_date is None:
            latest_date = get_latest_etl_batch_date()
        
        for table, export_func in table_exports:
            try:
                print(f"Fetching data for table: {table}")
                export_func(latest_date)
                print(f"Data fetched successfully for table: {table}\n")
            except Exception as e:
                print(f"Error processing table {table}: {e}\n")
        print("ETL process completed for all tables.")
    except Exception as e:
        print("Error in running src to s3",e)

if __name__ == "__main__":
    run_src_to_s3()
