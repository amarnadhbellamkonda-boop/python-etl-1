import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_products_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_stage = os.getenv("DEV_STAGE_SCHEMA")     # e.g., j25Amarnadh_prodstage
    dev_dw = os.getenv("DEV_DW_SCHEMA")           # e.g., j25Amarnadh_proddw
    table = "products"

    if not dev_stage:
        raise ValueError("DEV_STAGE_SCHEMA not set in .env")

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    # --------------------------------------------
    # 1️⃣ INSERT NEW PRODUCTS
    # --------------------------------------------
    insert_query = f"""
INSERT INTO {dev_dw}.products
(
    src_productcode,
    productname,
    productline,
    productscale,
    productvendor,
    productdescription,
    quantityinstock,
    buyprice,
    msrp,
    src_create_timestamp,
    src_update_timestamp,
    dw_product_line_id,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
)
SELECT
    src.productcode,
    src.productname,
    src.productline,
    src.productscale,
    src.productvendor,
    src.productdescription,
    src.quantityinstock,
    src.buyprice,
    src.msrp,
    src.create_timestamp,       -- stage
    src.update_timestamp,       -- stage
    pl.dw_product_line_id,      -- FK lookup
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM {dev_stage}.products src
LEFT JOIN {dev_dw}.productlines pl 
       ON src.productline = pl.productline
WHERE NOT EXISTS (
    SELECT 1 
    FROM {dev_dw}.products dw
    WHERE dw.src_productcode = src.productcode
);
"""

    # --------------------------------------------
    # 2️⃣ UPDATE EXISTING PRODUCTS (newer updates only)
    # --------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.products dw
SET
    productname = src.productname,
    productline = src.productline,
    productscale = src.productscale,
    productvendor = src.productvendor,
    productdescription = src.productdescription,
    quantityinstock = src.quantityinstock,
    buyprice = src.buyprice,
    msrp = src.msrp,
    dw_product_line_id = pl.dw_product_line_id,
    src_update_timestamp = src.update_timestamp,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM {dev_stage}.products src
LEFT JOIN {dev_dw}.productlines pl
       ON src.productline = pl.productline
WHERE dw.src_productcode = src.productcode
  AND dw.src_update_timestamp < src.update_timestamp;
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
    load_products_into_dw()
