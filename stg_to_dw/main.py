import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from utils import get_latest_etl_batch

from stg_to_dw.customers import load_customers_into_dw
from stg_to_dw.products import load_products_into_dw
from stg_to_dw.payments import load_payments_into_dw
from stg_to_dw.offices import load_offices_into_dw
from stg_to_dw.employees import load_employees_into_dw
from stg_to_dw.orderdetails import load_orderdetails_into_dw
from stg_to_dw.productlines import load_productlines_into_dw
from stg_to_dw.orders import load_orders_into_dw
from stg_to_dw.customer_history import load_customer_history_into_dw
from stg_to_dw.product_history import load_product_history_into_dw
from stg_to_dw.daily_customer_summary import load_daily_customer_summary_into_dw
from stg_to_dw.daily_product_summary import load_daily_product_summary_into_dw
from stg_to_dw.monthly_customer_summary import load_monthly_customer_summary_into_dw
from stg_to_dw.monthly_product_summary import load_monthly_product_summary_into_dw


ETL_EXECUTION_ORDER = [
    load_offices_into_dw,
    load_employees_into_dw,
    load_productlines_into_dw,
    load_products_into_dw,
    load_customers_into_dw,
    load_orders_into_dw,
    load_orderdetails_into_dw,
    load_payments_into_dw,
    load_product_history_into_dw,
    load_customer_history_into_dw,
    load_daily_customer_summary_into_dw,
    load_daily_product_summary_into_dw,
    load_monthly_customer_summary_into_dw,
    load_monthly_product_summary_into_dw
]


def run_dw_etl(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):
    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    print("Starting DW ETL Process")
    print(f"Batch No: {ETL_BATCH_NO}")
    print(f"Batch Date: {ETL_BATCH_DATE}")

    for func in ETL_EXECUTION_ORDER:
        name = func.__name__
        print(f"\nRunning: {name}")
        func(ETL_BATCH_NO, ETL_BATCH_DATE)
        print(f"Completed: {name}")

    print("\nDW ETL Completed Successfully")


if __name__ == "__main__":
    load_dotenv()
    run_dw_etl()
