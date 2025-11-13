import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
from dotenv import load_dotenv
from utils import get_redshift_connection, get_latest_etl_batch

load_dotenv()

def load_monthly_product_summary_into_dw(ETL_BATCH_NO=None, ETL_BATCH_DATE=None):

    conn, cur = get_redshift_connection()

    if ETL_BATCH_NO is None or ETL_BATCH_DATE is None:
        ETL_BATCH_NO, ETL_BATCH_DATE = get_latest_etl_batch()

    dev_dw = os.getenv("DEV_DW_SCHEMA")   # e.g., j25Amarnadh_proddw
    table = "monthly_product_summary"

    if not dev_dw:
        raise ValueError("DEV_DW_SCHEMA not set in .env")

    print(f"Loading table {table}...")

    # ----------------------------------------------------------------------
    # 1️⃣ DROP TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("DROP TABLE IF EXISTS temp_monthly_product_summary;")
    conn.commit()

    # ----------------------------------------------------------------------
    # 2️⃣ CREATE TEMP TABLE
    # ----------------------------------------------------------------------
    cur.execute("""
CREATE TEMP TABLE temp_monthly_product_summary (
    start_of_the_month_date DATE,
    dw_product_id INT,
    customer_apd INT,
    customer_apm INT,
    product_cost_amount DECIMAL(18,2),
    product_mrp_amount DECIMAL(18,2),
    cancelled_product_qty INT,
    cancelled_cost_amount DECIMAL(18,2),
    cancelled_mrp_amount DECIMAL(18,2),
    cancelled_order_apd INT,
    cancelled_order_apm INT
);
""")
    conn.commit()

    # ----------------------------------------------------------------------
    # 3️⃣ INSERT MONTH AGGREGATES FROM DAILY_PRODUCT_SUMMARY
    #     (MySQL DATE_FORMAT → Redshift DATE_TRUNC)
    # ----------------------------------------------------------------------
    monthly_insert = f"""
INSERT INTO temp_monthly_product_summary
SELECT
    DATE_TRUNC('month', summary_date) AS start_of_the_month_date,
    dw_product_id,
    MAX(customer_apd) AS customer_apd,
    1 AS customer_apm,
    SUM(product_cost_amount) AS product_cost_amount,
    SUM(product_mrp_amount) AS product_mrp_amount,
    SUM(cancelled_product_qty) AS cancelled_product_qty,
    SUM(cancelled_cost_amount) AS cancelled_cost_amount,
    SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
    MAX(cancelled_order_apd) AS cancelled_order_apd,
    SUM(cancelled_order_apd) AS cancelled_order_apm
FROM {dev_dw}.daily_product_summary
WHERE etl_batch_date >= '{ETL_BATCH_DATE}'
GROUP BY 1, 2;
"""
    cur.execute(monthly_insert)
    conn.commit()

    # ----------------------------------------------------------------------
    # 4️⃣ UPDATE EXISTING MONTHLY_PRODUCT_SUMMARY ROWS
    #     (MySQL UPDATE ... JOIN → Redshift UPDATE ... FROM)
    # ----------------------------------------------------------------------
    update_query = f"""
UPDATE {dev_dw}.monthly_product_summary m
SET
    customer_apd         = m.customer_apd         + t.customer_apd,
    customer_apm         = m.customer_apm         + t.customer_apm,
    product_cost_amount  = m.product_cost_amount  + t.product_cost_amount,
    product_mrp_amount   = m.product_mrp_amount   + t.product_mrp_amount,
    cancelled_product_qty= m.cancelled_product_qty+ t.cancelled_product_qty,
    cancelled_cost_amount= m.cancelled_cost_amount+ t.cancelled_cost_amount,
    cancelled_mrp_amount = m.cancelled_mrp_amount + t.cancelled_mrp_amount,
    cancelled_order_apd  = m.cancelled_order_apd  + t.cancelled_order_apd,
    cancelled_order_apm  = m.cancelled_order_apm  + t.cancelled_order_apm,
    dw_update_timestamp  = CURRENT_TIMESTAMP,
    etl_batch_no         = {ETL_BATCH_NO},
    etl_batch_date       = '{ETL_BATCH_DATE}'
FROM temp_monthly_product_summary t
WHERE m.start_of_the_month_date = t.start_of_the_month_date
  AND m.dw_product_id          = t.dw_product_id;
"""
    cur.execute(update_query)
    conn.commit()

    # ----------------------------------------------------------------------
    # 5️⃣ INSERT NEW MONTHLY_PRODUCT_SUMMARY ROWS
    # ----------------------------------------------------------------------
    insert_new = f"""
INSERT INTO {dev_dw}.monthly_product_summary
(
  start_of_the_month_date,
  dw_product_id,
  customer_apd,
  customer_apm,
  product_cost_amount,
  product_mrp_amount,
  cancelled_product_qty,
  cancelled_cost_amount,
  cancelled_mrp_amount,
  cancelled_order_apd,
  cancelled_order_apm,
  dw_create_timestamp,
  dw_update_timestamp,
  etl_batch_no,
  etl_batch_date
)
SELECT
    t.start_of_the_month_date,
    t.dw_product_id,
    t.customer_apd,
    t.customer_apm,
    t.product_cost_amount,
    t.product_mrp_amount,
    t.cancelled_product_qty,
    t.cancelled_cost_amount,
    t.cancelled_mrp_amount,
    t.cancelled_order_apd,
    t.cancelled_order_apm,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    {ETL_BATCH_NO},
    '{ETL_BATCH_DATE}'
FROM temp_monthly_product_summary t
LEFT JOIN {dev_dw}.monthly_product_summary m
  ON m.start_of_the_month_date = t.start_of_the_month_date
 AND m.dw_product_id          = t.dw_product_id
WHERE m.dw_product_id IS NULL;
"""
    cur.execute(insert_new)
    conn.commit()

    print(f"Table {table} loaded successfully!")

    cur.close()
    conn.close()


if __name__ == "__main__":
    load_monthly_product_summary_into_dw()
