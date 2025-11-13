import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_orderdetails_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")    
    dev_dw = os.getenv("DEV_DW_SCHEMA")          
    table = "orderdetails"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")
    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    insert_query = f"""
INSERT INTO {dev_dw}.orderdetails
(
   dw_order_id,
   dw_product_id,
   src_ordernumber,
   src_productcode,
   quantityordered,
   priceeach,
   orderlinenumber,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
SELECT 
   ord.dw_order_id,
   prod.dw_product_id,
   s.ordernumber,
   s.productcode,
   s.quantityordered,
   s.priceeach,
   s.orderlinenumber,
   s.create_timestamp,
   s.update_timestamp,
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   {ETL_BATCH_NO},
   '{ETL_BATCH_DATE}'
FROM {dev_stage}.orderdetails s
LEFT JOIN {dev_dw}.orders ord
        ON s.ordernumber = ord.src_ordernumber
LEFT JOIN {dev_dw}.products prod
        ON s.productcode = prod.src_productcode
WHERE NOT EXISTS (
    SELECT 1
    FROM {dev_dw}.orderdetails dw
    WHERE dw.src_ordernumber = s.ordernumber
      AND dw.src_productcode = s.productcode
);
"""

    update_query = f"""
UPDATE {dev_dw}.orderdetails dw
SET 
    quantityordered = s.quantityordered,
    priceeach = s.priceeach,
    orderlinenumber = s.orderlinenumber,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.orderdetails s
WHERE dw.src_ordernumber = s.ordernumber
  AND dw.src_productcode = s.productcode
  AND dw.src_update_timestamp < s.update_timestamp;
"""

    try:
        print(f"Loading table {table}...")

        cur.execute(insert_query)
        conn.commit()

        cur.execute(update_query)
        conn.commit()

        print(f"Table {table} loaded successfully.")

    except Exception as e:
        print(f"Error loading table {table}: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_orderdetails_into_dw()
