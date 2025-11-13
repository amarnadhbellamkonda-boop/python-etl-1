import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_offices_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")    
    dev_dw = os.getenv("DEV_DW_SCHEMA")          
    table = "offices"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")
    insert_query = f"""
INSERT INTO {dev_dw}.offices
(
   officecode,
   city,
   phone,
   addressline1,
   addressline2,
   state,
   country,
   postalcode,
   territory,
   src_create_timestamp,
   src_update_timestamp,
   dw_create_timestamp,
   dw_update_timestamp,
   etl_batch_no,
   etl_batch_date
)
SELECT
   s.officecode,
   s.city,
   s.phone,
   s.addressline1,
   s.addressline2,
   s.state,
   s.country,
   s.postalcode,
   s.territory,
   s.create_timestamp,      -- stage column
   s.update_timestamp,      -- stage column
   CURRENT_TIMESTAMP,
   CURRENT_TIMESTAMP,
   {ETL_BATCH_NO},
   '{ETL_BATCH_DATE}'
FROM {dev_stage}.offices s
WHERE NOT EXISTS (
    SELECT 1
    FROM {dev_dw}.offices d
    WHERE d.officecode = s.officecode
);
"""

    update_query = f"""
UPDATE {dev_dw}.offices d
SET 
    city = s.city,
    phone = s.phone,
    addressline1 = s.addressline1,
    addressline2 = s.addressline2,
    state = s.state,
    country = s.country,
    postalcode = s.postalcode,
    territory = s.territory,
    src_update_timestamp = s.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.offices s
WHERE d.officecode = s.officecode
  AND d.src_update_timestamp < s.update_timestamp;
"""

    try:
        print(f"Loading table {table}...")

        # INSERT new rows
        cur.execute(insert_query)
        conn.commit()

        # UPDATE existing rows
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
    load_offices_into_dw()
