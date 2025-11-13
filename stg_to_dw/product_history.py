import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_product_history_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")        # e.g., j25Amarnadh_proddw
    table = "product_history"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ CLOSE OUT EXISTING ACTIVE RECORDS (SCD2)
    # --------------------------------------------
    update_close_query = f"""
UPDATE {dev_dw}.product_history h
SET 
    effective_to_date = DATEADD(day, -1, DATE '{ETL_BATCH_DATE}'),
    dw_active_record_ind = 0,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    update_etl_batch_no = {ETL_BATCH_NO},
    update_etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_dw}.products p
WHERE h.dw_product_id = p.dw_product_id
  AND h.dw_active_record_ind = 1
  AND h.msrp <> p.msrp;
"""

    # --------------------------------------------
    # 2️⃣ INSERT NEW ACTIVE RECORDS (SCD2)
    # --------------------------------------------
    insert_new_query = f"""
INSERT INTO {dev_dw}.product_history
(
    dw_product_id,
    msrp,
    effective_from_date,
    dw_active_record_ind,
    dw_create_timestamp,
    dw_update_timestamp,
    create_etl_batch_no,
    create_etl_batch_date
)
SELECT
    p.dw_product_id,
    p.msrp,
    '{ETL_BATCH_DATE}',
    1,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM {dev_dw}.products p
LEFT JOIN (
    SELECT dw_product_id, msrp
    FROM {dev_dw}.product_history
    WHERE dw_active_record_ind = 1
) h ON p.dw_product_id = h.dw_product_id
WHERE h.dw_product_id IS NULL
   OR h.msrp <> p.msrp;
"""

    try:
        print(f"Running SCD2 for {table}...")

        # Close out old records
        cur.execute(update_close_query)
        conn.commit()

        # Insert new active records
        cur.execute(insert_new_query)
        conn.commit()

        print(f"Product history SCD2 completed successfully.")

    except Exception as e:
        print(f"Error loading {table}: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_product_history_into_dw()
