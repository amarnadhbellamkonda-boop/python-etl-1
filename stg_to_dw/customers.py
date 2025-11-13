import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_customers_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")     # e.g., j25Amarnadh_prodstage
    dev_dw = os.getenv("DEV_DW_SCHEMA")           # e.g., j25Amarnadh_proddw
    table = "customers"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ INSERT NEW CUSTOMERS
    # --------------------------------------------
    insert_query = f"""
INSERT INTO {dev_dw}.customers
(
   src_customernumber,
   customername,
   contactlastname,
   contactfirstname,
   phone,
   addressline1,
   addressline2,
   city,
   state,
   postalcode,
   country,
   salesrepemployeenumber,
   creditlimit,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
SELECT
   s.customernumber,
   s.customername,
   s.contactlastname,
   s.contactfirstname,
   s.phone,
   s.addressline1,
   s.addressline2,
   s.city,
   s.state,
   s.postalcode,
   s.country,
   s.salesrepemployeenumber,
   s.creditlimit,
   s.create_timestamp,
   s.update_timestamp,
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   {ETL_BATCH_NO},
   '{ETL_BATCH_DATE}'
FROM {dev_stage}.customers s
WHERE NOT EXISTS (
    SELECT 1
    FROM {dev_dw}.customers d
    WHERE d.src_customernumber = s.customernumber
);
"""

    # --------------------------------------------
    # 2️⃣ UPDATE EXISTING CUSTOMERS (newer data only)
    # --------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.customers d
SET 
    customername = s.customername,
    contactlastname = s.contactlastname,
    contactfirstname = s.contactfirstname,
    phone = s.phone,
    addressline1 = s.addressline1,
    addressline2 = s.addressline2,
    city = s.city,
    state = s.state,
    postalcode = s.postalcode,
    country = s.country,
    salesrepemployeenumber = s.salesrepemployeenumber,
    creditlimit = s.creditlimit,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.customers s
WHERE d.src_customernumber = s.customernumber
  AND d.src_update_timestamp < s.update_timestamp;
"""

    try:
        print(f"Loading table {table}...")

        # INSERT new records
        cur.execute(insert_query)
        conn.commit()

        # UPDATE existing records
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
    load_customers_into_dw()
