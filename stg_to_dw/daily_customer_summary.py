import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_daily_customer_summary_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")   # j25Amarnadh_proddw
    table = "daily_customer_summary"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    print(f"Loading table {table}...")

    # ----------------------------------------------------------------------
    # 1️⃣ DROP TEMP TABLE (single statement)
    # ----------------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS temp_daily_customer_summary;")
    conn.commit()

    # ----------------------------------------------------------------------
    # 2️⃣ CREATE TEMP TABLE (single statement)
    # ----------------------------------------------------------------------
    cur.execute("""
CREATE TEMP TABLE temp_daily_customer_summary (
    summary_date DATE,
    dw_customer_id INT,
    order_count INT,
    order_apd INT,
    order_cost_amount DECIMAL(18,2),
    cancelled_order_count INT,
    cancelled_order_amount DECIMAL(18,2),
    cancelled_order_apd INT,
    shipped_order_count INT,
    shipped_order_amount DECIMAL(18,2),
    shipped_order_apd INT,
    payment_apd INT,
    payment_amount DECIMAL(18,2),
    products_ordered_qty INT,
    products_items_qty INT,
    order_mrp_amount DECIMAL(18,2),
    new_customer_apd INT,
    new_customer_paid_apd INT
);
""")
    conn.commit()

    # ----------------------------------------------------------------------
    # 3️⃣ BLOCK 1 — Orders summary
    # ----------------------------------------------------------------------
    insert_orders = f"""
INSERT INTO temp_daily_customer_summary
SELECT 
    DATE(o.orderdate),
    o.dw_customer_id,
    COUNT(DISTINCT o.dw_order_id),
    1,
    SUM(od.priceeach * od.quantityordered),
    0,0,0,
    0,0,0,
    0,0,
    COUNT(DISTINCT od.dw_product_id),
    COUNT(od.quantityordered),
    SUM(p.msrp * od.quantityordered),
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
    # 4️⃣ BLOCK 2 — New customers summary
    # ----------------------------------------------------------------------
    insert_new_customers = f"""
INSERT INTO temp_daily_customer_summary
SELECT 
    DATE(c.src_create_timestamp),
    c.dw_customer_id,
    0,0,0,
    0,0,0,
    0,0,0,
    0,0,
    0,0,0,
    1,
    0
FROM {dev_dw}.customers c
WHERE DATE(c.src_create_timestamp) >= '{ETL_BATCH_DATE}';
"""
    cur.execute(insert_new_customers)
    conn.commit()

    # ----------------------------------------------------------------------
    # 5️⃣ BLOCK 3 — Cancelled orders summary
    # ----------------------------------------------------------------------
    insert_cancelled = f"""
INSERT INTO temp_daily_customer_summary
SELECT 
    DATE(o.cancelleddate),
    o.dw_customer_id,
    0,0,0,
    COUNT(o.dw_order_id),
    SUM(od.priceeach * od.quantityordered),
    1,
    0,0,0,
    0,0,
    0,0,0,
    0,
    0
FROM {dev_dw}.orders o
JOIN {dev_dw}.orderdetails od ON o.dw_order_id = od.dw_order_id
WHERE DATE(o.cancelleddate) >= '{ETL_BATCH_DATE}'
  AND o.status = 'Cancelled'
GROUP BY 1,2;
"""
    cur.execute(insert_cancelled)
    conn.commit()

    # ----------------------------------------------------------------------
    # 6️⃣ BLOCK 4 — Payments summary
    # ----------------------------------------------------------------------
    insert_payments = f"""
INSERT INTO temp_daily_customer_summary
SELECT 
    DATE(p.paymentdate),
    p.dw_customer_id,
    0,0,0,
    0,0,0,
    0,0,0,
    1,
    SUM(p.amount),
    0,0,0,
    0,
    1
FROM {dev_dw}.payments p
WHERE DATE(p.paymentdate) >= '{ETL_BATCH_DATE}'
GROUP BY 1,2;
"""
    cur.execute(insert_payments)
    conn.commit()

    # ----------------------------------------------------------------------
    # 7️⃣ BLOCK 5 — Shipped orders summary
    # ----------------------------------------------------------------------
    insert_shipped = f"""
INSERT INTO temp_daily_customer_summary
SELECT 
    DATE(o.shippeddate),
    o.dw_customer_id,
    0,0,0,
    0,0,0,
    COUNT(o.dw_order_id),
    SUM(od.priceeach * od.quantityordered),
    1,
    0,0,
    0,0,0,
    0,
    0
FROM {dev_dw}.orders o
JOIN {dev_dw}.orderdetails od ON o.dw_order_id = od.dw_order_id
WHERE DATE(o.shippeddate) >= '{ETL_BATCH_DATE}'
  AND o.status = 'Shipped'
GROUP BY 1,2;
"""
    cur.execute(insert_shipped)
    conn.commit()

    # ----------------------------------------------------------------------
    # 8️⃣ FINAL INSERT INTO DW TABLE
    # ----------------------------------------------------------------------
    final_insert = f"""
INSERT INTO {dev_dw}.daily_customer_summary (
    summary_date,
    dw_customer_id,
    order_count,
    order_apd,
    order_cost_amount,
    cancelled_order_count,
    cancelled_order_amount,
    cancelled_order_apd,
    shipped_order_count,
    shipped_order_amount,
    shipped_order_apd,
    payment_apd,
    payment_amount,
    products_ordered_qty,
    products_items_qty,
    order_mrp_amount,
    new_customer_apd,
    new_customer_paid_apd,
    dw_create_timestamp,
    dw_update_timestamp,
    etl_batch_no,
    etl_batch_date
)
SELECT
    summary_date,
    dw_customer_id,
    SUM(order_count),
    MAX(order_apd),
    SUM(order_cost_amount),
    SUM(cancelled_order_count),
    SUM(cancelled_order_amount),
    MAX(cancelled_order_apd),
    SUM(shipped_order_count),
    SUM(shipped_order_amount),
    MAX(shipped_order_apd),
    MAX(payment_apd),
    SUM(payment_amount),
    SUM(products_ordered_qty),
    SUM(products_items_qty),
    SUM(order_mrp_amount),
    MAX(new_customer_apd),
    MAX(new_customer_paid_apd),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM temp_daily_customer_summary
GROUP BY 1,2;
"""
    cur.execute(final_insert)
    conn.commit()

    print(f"Table {table} loaded successfully!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_daily_customer_summary_into_dw()
