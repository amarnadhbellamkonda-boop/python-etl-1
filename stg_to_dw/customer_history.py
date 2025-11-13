import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_customer_history_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")   # e.g., j25Amarnadh_proddw
    table = "customer_history"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ CLOSE OUT EXISTING ACTIVE SCD2 RECORDS
    # --------------------------------------------
    update_close_query = f"""
UPDATE {dev_dw}.customer_history h
SET 
    effective_to_date = DATEADD(day, -1, DATE '{ETL_BATCH_DATE}'),
    dw_active_record_ind = 0,
    dw_update_timestamp = CURRENT_TIMESTAMP,cur
    update_etl_batch_no = {ETL_BATCH_NO},
    update_etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_dw}.customers c
WHERE h.dw_customer_id = c.dw_customer_id
  AND h.dw_active_record_ind = 1
  AND h.creditlimit <> c.creditlimit;
"""

    # --------------------------------------------
    # 2️⃣ INSERT NEW ACTIVE SCD2 RECORDS
    # --------------------------------------------
    insert_new_query = f"""
INSERT INTO {dev_dw}.customer_history 
(
    dw_customer_id,
    creditlimit,
    effective_from_date,
    dw_active_record_ind,
    dw_create_timestamp,
    dw_update_timestamp,
    create_etl_batch_no,
    create_etl_batch_date
)
SELECT
    cust.dw_customer_id,
    cust.creditlimit,
    '{ETL_BATCH_DATE}',
    1,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM {dev_dw}.customers cust
LEFT JOIN (
    SELECT dw_customer_id, creditlimit
    FROM {dev_dw}.customer_history
    WHERE dw_active_record_ind = 1
) hist
    ON cust.dw_customer_id = hist.dw_customer_id
WHERE hist.dw_customer_id IS NULL
   OR hist.creditlimit <> cust.creditlimit;
"""

    try:
        print(f"Running SCD2 for {table}...")

        # Close old active records
        cur.execute(update_close_query)
        conn.commit()

        # Insert new SCD2 records
        cur.execute(insert_new_query)
        conn.commit()

        print(f"Customer history SCD2 completed successfully.")

    except Exception as e:
        print(f"Error loading table {table}: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_customer_history_into_dw()
