import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_orders_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")     # j25Amarnadh_prodstage
    dev_dw = os.getenv("DEV_DW_SCHEMA")           # j25Amarnadh_proddw
    table = "orders"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ INSERT NEW ORDERS
    # --------------------------------------------
    insert_query = f"""
INSERT INTO {dev_dw}.orders
(
   dw_customer_id,
   src_ordernumber,
   orderdate,
   requireddate,
   shippeddate,
   status,
   comments,
   cancelleddate,
   src_customernumber,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
SELECT 
   cust.dw_customer_id,
   s.ordernumber,
   s.orderdate,
   s.requireddate,
   s.shippeddate,
   s.status,
   s.comments,
   s.cancelleddate,
   s.customernumber,
   s.create_timestamp,
   s.update_timestamp,
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   {ETL_BATCH_NO},
   '{ETL_BATCH_DATE}'
FROM {dev_stage}.orders s
LEFT JOIN {dev_dw}.customers cust
       ON s.customernumber = cust.src_customernumber
WHERE NOT EXISTS (
    SELECT 1 
    FROM {dev_dw}.orders dw
    WHERE dw.src_ordernumber = s.ordernumber
);
"""

    # --------------------------------------------
    # 2️⃣ UPDATE EXISTING ORDERS (ONLY NEWER DATA)
    # --------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.orders dw
SET 
    dw_customer_id = cust.dw_customer_id,
    orderdate = s.orderdate,
    requireddate = s.requireddate,
    shippeddate = s.shippeddate,
    status = s.status,
    comments = s.comments,
    cancelleddate = s.cancelleddate,
    src_customernumber = s.customernumber,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.orders s
LEFT JOIN {dev_dw}.customers cust
       ON s.customernumber = cust.src_customernumber
WHERE dw.src_ordernumber = s.ordernumber
  AND dw.src_update_timestamp < s.update_timestamp;
"""

    try:
        print(f"Loading table {table}...")

        # Execute INSERT
        cur.execute(insert_query)
        conn.commit()

        # Execute UPDATE
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
    load_orders_into_dw()
