import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_monthly_customer_summary_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")
    table = "monthly_customer_summary"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    print(f"Loading table {table}...")

    # ----------------------------------------------------------------------
    # 1️⃣ DROP TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS temp_monthly_customer_summary;")
    conn.commit()

    # ----------------------------------------------------------------------
    # 2️⃣ CREATE TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("""
CREATE TEMP TABLE temp_monthly_customer_summary (
    start_of_the_month_date DATE,
    dw_customer_id INT,
    order_count INT,
    order_apd INT,
    order_apm INT,
    order_cost_amount DECIMAL(18,2),
    cancelled_order_count INT,
    cancelled_order_amount DECIMAL(18,2),
    cancelled_order_apd INT,
    cancelled_order_apm INT,
    shipped_order_count INT,
    shipped_order_amount DECIMAL(18,2),
    shipped_order_apd INT,
    shipped_order_apm INT,
    payment_apd INT,
    payment_apm INT,
    payment_amount DECIMAL(18,2),
    products_ordered_qty INT,
    products_items_qty INT,
    order_mrp_amount DECIMAL(18,2),
    new_customer_apd INT,
    new_customer_apm INT,
    new_customer_paid_apd INT,
    new_customer_paid_apm INT
);
""")
    conn.commit()

    # ----------------------------------------------------------------------
    # 3️⃣ INSERT MONTH AGGREGATES (FROM DAILY TABLE)
    # ----------------------------------------------------------------------
    monthly_insert = f"""
INSERT INTO temp_monthly_customer_summary
SELECT  
    DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
    dw_customer_id,
    SUM(order_count),
    SUM(order_apd),
    COUNT(DISTINCT summary_date) AS order_apm,
    SUM(order_cost_amount),
    SUM(cancelled_order_count),
    SUM(cancelled_order_amount),
    SUM(cancelled_order_apd),
    COUNT(DISTINCT CASE WHEN cancelled_order_count > 0 THEN summary_date END),
    SUM(shipped_order_count),
    SUM(shipped_order_amount),
    SUM(shipped_order_apd),
    COUNT(DISTINCT CASE WHEN shipped_order_count > 0 THEN summary_date END),
    SUM(payment_apd),
    COUNT(DISTINCT CASE WHEN payment_amount > 0 THEN summary_date END),
    SUM(payment_amount),
    SUM(products_ordered_qty),
    SUM(products_items_qty),
    SUM(order_mrp_amount),
    SUM(new_customer_apd),
    COUNT(DISTINCT CASE WHEN new_customer_apd > 0 THEN summary_date END),
    SUM(new_customer_paid_apd),
    COUNT(DISTINCT CASE WHEN new_customer_paid_apd > 0 THEN summary_date END)
FROM {dev_dw}.daily_customer_summary
WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
GROUP BY 1,2;
"""
    cur.execute(monthly_insert)
    conn.commit()

    # ----------------------------------------------------------------------
    # 4️⃣ UPDATE EXISTING MONTHLY RECORDS
    # ----------------------------------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.monthly_customer_summary m
SET 
    order_count = m.order_count + t.order_count,
    order_apd = m.order_apd + t.order_apd,
    order_apm = m.order_apm + t.order_apm,
    order_cost_amount = m.order_cost_amount + t.order_cost_amount,
    cancelled_order_count = m.cancelled_order_count + t.cancelled_order_count,
    cancelled_order_amount = m.cancelled_order_amount + t.cancelled_order_amount,
    cancelled_order_apd = m.cancelled_order_apd + t.cancelled_order_apd,
    cancelled_order_apm = m.cancelled_order_apm + t.cancelled_order_apm,
    shipped_order_count = m.shipped_order_count + t.shipped_order_count,
    shipped_order_amount = m.shipped_order_amount + t.shipped_order_amount,
    shipped_order_apd = m.shipped_order_apd + t.shipped_order_apd,
    shipped_order_apm = m.shipped_order_apm + t.shipped_order_apm,
    payment_apd = m.payment_apd + t.payment_apd,
    payment_apm = m.payment_apm + t.payment_apm,
    payment_amount = m.payment_amount + t.payment_amount,
    products_ordered_qty = m.products_ordered_qty + t.products_ordered_qty,
    products_items_qty = m.products_items_qty + t.products_items_qty,
    order_mrp_amount = m.order_mrp_amount + t.order_mrp_amount,
    new_customer_apd = m.new_customer_apd + t.new_customer_apd,
    new_customer_apm = m.new_customer_apm + t.new_customer_apm,
    new_customer_paid_apd = m.new_customer_paid_apd + t.new_customer_paid_apd,
    new_customer_paid_apm = m.new_customer_paid_apm + t.new_customer_paid_apm,
    dw_update_timestamp = CURRENT_TIMESTAMP,
    etl_batch_no = {ETL_BATCH_NO},
    etl_batch_date = '{ETL_BATCH_DATE}'
FROM temp_monthly_customer_summary t
WHERE m.start_of_the_month_date = t.start_of_the_month_date
  AND m.dw_customer_id = t.dw_customer_id;
"""
    cur.execute(update_query)
    conn.commit()

    # ----------------------------------------------------------------------
    # 5️⃣ INSERT NEW MONTHLY RECORDS
    # ----------------------------------------------------------------------
    insert_new = f"""
INSERT INTO {dev_dw}.monthly_customer_summary
(
  start_of_the_month_date,
  dw_customer_id,
  order_count,
  order_apd,
  order_apm,
  order_cost_amount,
  cancelled_order_count,
  cancelled_order_amount,
  cancelled_order_apd,
  cancelled_order_apm,
  shipped_order_count,
  shipped_order_amount,
  shipped_order_apd,
  shipped_order_apm,
  payment_apd,
  payment_apm,
  payment_amount,
  products_ordered_qty,
  products_items_qty,
  order_mrp_amount,
  new_customer_apd,
  new_customer_apm,
  new_customer_paid_apd,
  new_customer_paid_apm,
  dw_create_timestamp,
  dw_update_timestamp,
  etl_batch_no,
  etl_batch_date
)
SELECT 
    t.*,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM temp_monthly_customer_summary t
LEFT JOIN {dev_dw}.monthly_customer_summary m
  ON m.start_of_the_month_date = t.start_of_the_month_date
 AND m.dw_customer_id = t.dw_customer_id
WHERE m.dw_customer_id IS NULL;
"""
    cur.execute(insert_new)
    conn.commit()

    print(f"Table {table} loaded successfully!")

    cur.close()
    conn.close()



if __name__ == "__main__":
    load_monthly_customer_summary_into_dw()
