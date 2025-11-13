import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_productlines_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")
    dev_dw = os.getenv("DEV_DW_SCHEMA")
    table = "productlines"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ INSERT NEW ROWS
    # --------------------------------------------
    insert_query = f"""
INSERT INTO {dev_dw}.productlines
(
    productline,
    textdescription,
    htmldescription,
    image,
    src_create_timestamp,
    src_update_timestamp,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
)
SELECT 
    src.productline,
    src.textdescription,
    src.htmldescription,
    src.image,
    src.create_timestamp,     -- correct source column
    src.update_timestamp,     -- correct source column
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM {dev_stage}.productlines src
WHERE NOT EXISTS (
    SELECT 1 
    FROM {dev_dw}.productlines dw
    WHERE dw.productline = src.productline
);
"""

    # --------------------------------------------
    # 2️⃣ UPDATE EXISTING ROWS WITH NEWER DATA
    # --------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.productlines dw
SET
    textdescription = src.textdescription,
    htmldescription = src.htmldescription,
    image = src.image,
    src_update_timestamp = src.update_timestamp,  -- correct source column
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.productlines src
WHERE dw.productline = src.productline
  AND dw.src_update_timestamp < src.update_timestamp;
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
        print(f"Error loading {table}: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    load_productlines_into_dw()