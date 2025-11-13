import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_daily_product_summary_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")   # j25Amarnadh_proddw
    table = "daily_product_summary"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    print(f"Loading table {table}...")

    # ----------------------------------------------------------------------
    # 1️⃣ DROP TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS temp_daily_product_summary;")
    conn.commit()

    # ----------------------------------------------------------------------
    # 2️⃣ CREATE TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("""
CREATE TEMP TABLE temp_daily_product_summary (
    summary_date DATE,
    dw_product_id INT,
    customer_apd INT,
    product_cost_amount DECIMAL(18,2),
    product_mrp_amount DECIMAL(18,2),
    cancelled_product_qty INT,
    cancelled_cost_amount DECIMAL(18,2),
    cancelled_mrp_amount DECIMAL(18,2),
    cancelled_order_apd INT
);
""")
    conn.commit()

    # ----------------------------------------------------------------------
    # 3️⃣ BLOCK 1 — PRODUCT METRICS FROM ORDERS
    # ----------------------------------------------------------------------
    insert_orders = f"""
INSERT INTO temp_daily_product_summary
SELECT 
    DATE(o.orderdate),
    od.dw_product_id,
    COUNT(DISTINCT o.dw_customer_id),
    SUM(od.priceeach * od.quantityordered),
    SUM(p.msrp * od.quantityordered),
    0,
    0,
    0,
    0
FROM {dev_dw}.orders o
JOIN {dev_dw}.orderdetails od ON o.dw_order_id = od.dw_order_id
JOIN {dev_dw}.products p ON od.dw_product_id = p.dw_product_id
WHERE DATE(o.orderdate) >= '{ETL_BATCH_DATE}'
GROUP BY 1,2;
"""
    cur.execute(insert_orders)
    conn.commit()

    # ----------------------------------------------------------------------
    # 4️⃣ BLOCK 2 — CANCELLED PRODUCT METRICS
    # ----------------------------------------------------------------------
    insert_cancelled = f"""
INSERT INTO temp_daily_product_summary
SELECT 
    DATE(o.cancelleddate),
    od.dw_product_id,
    0,
    0,
    0,
    SUM(od.quantityordered),
    SUM(od.priceeach * od.quantityordered),
    SUM(p.msrp * od.quantityordered),
    COUNT(DISTINCT o.dw_order_id)
FROM {dev_dw}.orders o
JOIN {dev_dw}.orderdetails od ON o.dw_order_id = od.dw_order_id
JOIN {dev_dw}.products p ON od.dw_product_id = p.dw_product_id
WHERE LOWER(TRIM(o.status)) = 'cancelled'
  AND DATE(o.cancelleddate) >= '{ETL_BATCH_DATE}'
GROUP BY 1,2;
"""
    cur.execute(insert_cancelled)
    conn.commit()

    # ----------------------------------------------------------------------
    # 5️⃣ FINAL INSERT INTO DW TABLE
    # ----------------------------------------------------------------------
    final_insert = f"""
INSERT INTO {dev_dw}.daily_product_summary (
    summary_date,
    dw_product_id,
    customer_apd,
    product_cost_amount,
    product_mrp_amount,
    cancelled_product_qty,
    cancelled_cost_amount,
    cancelled_mrp_amount,
    cancelled_order_apd,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
)
SELECT
    summary_date,
    dw_product_id,
    SUM(customer_apd),
    SUM(product_cost_amount),
    SUM(product_mrp_amount),
    SUM(cancelled_product_qty),
    SUM(cancelled_cost_amount),
    SUM(cancelled_mrp_amount),
    SUM(cancelled_order_apd),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM temp_daily_product_summary
GROUP BY 1,2;
"""
    cur.execute(final_insert)
    conn.commit()

    print(f"Table {table} loaded successfully!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_daily_product_summary_into_dw()
